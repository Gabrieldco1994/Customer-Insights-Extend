# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "e0542029-2e22-491e-9fdf-b8d7d889679a",
# META       "default_lakehouse_name": "dataverse_fsiprincipal_cds2_workspace_unqcc8d8adf2841f011be52000d3a106",
# META       "default_lakehouse_workspace_id": "b84200d4-c6e9-4925-a23a-77f16f9701eb",
# META       "known_lakehouses": [
# META         {
# META           "id": "e0542029-2e22-491e-9fdf-b8d7d889679a"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, LongType, TimestampType, StringType

# ============================================================
# 1) READ
# ============================================================
df_conta = spark.read.table("crfc2_contaapagar")
df_parc  = spark.read.table("crfc2_parceiro")
df_proc  = spark.read.table("crfc2_processamentodocalculo")

# ============================================================
# 2) CAST FORMATS (Dataverse-friendly)
# ============================================================
df_conta = (
    df_conta
      .withColumn("crfc2_valor", F.col("crfc2_valor").cast(DecimalType(18, 2)))
      .withColumn("crfc2_descricao", F.col("crfc2_descricao").cast(StringType()))
)

df_proc = (
    df_proc
      .withColumn("crfc2_status", F.col("crfc2_status").cast(LongType()))
      .withColumn("crfc2_dataprocessamento", F.col("crfc2_dataprocessamento").cast(TimestampType()))
)

df_parc = (
    df_parc
      .withColumn("crfc2_nome", F.col("crfc2_nome").cast(StringType()))
      .withColumn("crfc2_email", F.col("crfc2_email").cast(StringType()))
)

# ============================================================
# 3) JOIN contaapagar x parceiro
# ============================================================
df_join1 = (
    df_conta.alias("c")
      .join(
          df_parc.alias("p"),
          F.col("c.crfc2_parceiroid") == F.col("p.crfc2_parceiroid"),
          "left"
      )
      .select(
          F.col("c.Id").cast(StringType()).alias("contaapagar_id"),
          F.col("c.crfc2_parceiroid").cast(StringType()).alias("parceiro_ref"),
          F.col("c.crfc2_valor"),
          F.col("c.crfc2_descricao"),
          F.col("p.Id").cast(StringType()).alias("parceiro_id"),
          F.col("p.crfc2_nome"),
          F.col("p.crfc2_email")
      )
)

# ============================================================
# 4) JOIN com processamentodocalculo
# ============================================================
df_join2 = (
    df_join1.alias("j")
      .join(
          df_proc.alias("pr"),
          F.col("j.contaapagar_id") == F.col("pr.crfc2_contaid").cast(StringType()),
          "left"
      )
)

# ============================================================
# 5) SELECT FINAL - Apenas tipos Dataverse e campos essenciais
# ============================================================
df_out = (
    df_join2.select(
        F.col("contaapagar_id"),
        F.col("parceiro_ref"),
        F.col("crfc2_valor").alias("valor"),
        F.col("crfc2_descricao").alias("descricao"),
        F.col("parceiro_id"),
        F.col("crfc2_nome").alias("parceiro_nome"),
        F.col("crfc2_email").alias("parceiro_email"),
        F.col("pr.Id").cast(StringType()).alias("processamento_id"),
        F.col("pr.crfc2_status").cast(LongType()).alias("processamento_status"),
        F.current_timestamp().alias("data_processamento")
    )
)

# ============================================================
# 5.1) "PRIMARY KEY" via Spark
# ============================================================
df_out = (
    df_out
      .withColumn(
          "dv_pk",
          F.coalesce(
              F.col("contaapagar_id"),
              F.sha2(
                  F.concat_ws(
                      "||",
                      F.col("parceiro_ref"),
                      F.col("processamento_id"),
                      F.col("descricao"),
                      F.col("valor").cast(StringType()),
                      F.col("data_processamento").cast(StringType())
                  ),
                  256
              )
          ).cast(StringType())
      )
      .filter(F.col("dv_pk").isNotNull())
)

# ============================================================
# 5.2) Deduplicação e validação
# ============================================================
df_out_dedup = df_out.dropDuplicates(["dv_pk"])

# ============================================================
# Apenas adiciona novos registros por contaapagar_id (NÃO recria tabela)
# ============================================================
from delta.tables import DeltaTable

target_table_name = "contaapagar_enriched"

# Carrega os Ids já existentes na tabela de destino
df_existing = spark.read.table(target_table_name).select(F.col("contaapagar_id"))

# Adiciona apenas novos por contaapagar_id
df_novos = df_out_dedup.join(df_existing, on="contaapagar_id", how="left_anti")
if df_novos.count() > 0:
    df_novos.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(target_table_name)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
