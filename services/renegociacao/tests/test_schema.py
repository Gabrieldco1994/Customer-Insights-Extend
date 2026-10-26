import unittest
from pathlib import Path
from xml.etree import ElementTree

from renegociacao.odata import metadata


class SqlSchema(unittest.TestCase):
    def test_snapshot_columns_use_sql_supported_size_with_length_constraint(self):
        source = (Path(__file__).resolve().parents[1] / "schema.sql").read_text()
        self.assertNotIn("nvarchar(10000)", source.lower())
        self.assertEqual(source.count("Snapshot nvarchar(max)"), 2)
        self.assertEqual(source.count("LEN(Snapshot) <= 10000"), 2)

    def test_odata_nullability_matches_optional_dataverse_attributes(self):
        ns = {"e": "http://docs.oasis-open.org/odata/ns/edm"}
        document = ElementTree.fromstring(metadata())
        for field in document.findall(".//e:Property", ns):
            self.assertEqual(
                field.attrib["Nullable"],
                "false" if field.attrib["Name"] == "Id" else "true",
            )


if __name__ == "__main__":
    unittest.main()
