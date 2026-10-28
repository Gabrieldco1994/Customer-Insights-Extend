-- DEMO source only. Run bootstrap as temporary Entra SQL administrator.
-- No destructive statements. Existing fixture rows are never reset.
IF SCHEMA_ID(N'demo') IS NULL EXEC(N'CREATE SCHEMA demo AUTHORIZATION dbo');
GO
IF OBJECT_ID(N'demo.Contracts', N'U') IS NULL
BEGIN
    CREATE TABLE demo.Contracts (
        Id uniqueidentifier NOT NULL CONSTRAINT PK_DemoContracts PRIMARY KEY,
        ContactId uniqueidentifier NOT NULL,
        AnchorId uniqueidentifier NOT NULL,
        Number nvarchar(80) NOT NULL,
        Vehicle nvarchar(160) NOT NULL,
        DebtCents int NOT NULL CHECK (DebtCents > 0),
        Eligible bit NOT NULL,
        PositionDate datetimeoffset(0) NOT NULL DEFAULT SYSDATETIMEOFFSET()
    );
    CREATE INDEX IX_DemoContracts_ContactRef ON demo.Contracts(ContactId, Id);
END;
GO
IF OBJECT_ID(N'demo.Installments', N'U') IS NULL
BEGIN
    CREATE TABLE demo.Installments (
        Id uniqueidentifier NOT NULL CONSTRAINT PK_DemoInstallments PRIMARY KEY,
        ContractId uniqueidentifier NOT NULL REFERENCES demo.Contracts(Id),
        ContactId uniqueidentifier NOT NULL,
        Number int NOT NULL CHECK (Number > 0),
        AmountCents int NOT NULL CHECK (AmountCents >= 0),
        DueDate datetimeoffset(0) NOT NULL,
        Status nvarchar(32) NOT NULL,
        CONSTRAINT UQ_DemoInstallment UNIQUE(ContractId, Number)
    );
    CREATE INDEX IX_DemoInstallments_ClientRef ON demo.Installments(ContactId, Id);
    CREATE INDEX IX_DemoInstallments_ContractRef ON demo.Installments(ContractId, Id);
END;
GO
IF OBJECT_ID(N'demo.Offers', N'U') IS NULL
BEGIN
    CREATE TABLE demo.Offers (
        Id uniqueidentifier NOT NULL CONSTRAINT PK_DemoOffers PRIMARY KEY,
        ContractId uniqueidentifier NOT NULL REFERENCES demo.Contracts(Id),
        Version int NOT NULL CHECK (Version > 0),
        DownPaymentCents int NOT NULL CHECK (DownPaymentCents >= 0),
        InstallmentCents int NOT NULL CHECK (InstallmentCents > 0),
        FinalInstallmentCents int NOT NULL CHECK (FinalInstallmentCents > 0),
        InstallmentCount int NOT NULL CHECK (InstallmentCount > 0),
        TotalCents int NOT NULL CHECK (TotalCents > 0),
        ValidUntil datetimeoffset(0) NOT NULL,
        CONSTRAINT CK_DemoOfferMoney CHECK (
            CONVERT(bigint, DownPaymentCents)
            + CONVERT(bigint, InstallmentCents) * (InstallmentCount - 1)
            + FinalInstallmentCents = TotalCents),
        CONSTRAINT CK_DemoUniformPayments CHECK (FinalInstallmentCents = InstallmentCents)
    );
    CREATE INDEX IX_DemoOffers_Contract ON demo.Offers(ContractId, Id);
END;
GO
IF OBJECT_ID(N'demo.Requests', N'U') IS NULL
BEGIN
    CREATE TABLE demo.Requests (
        Id uniqueidentifier NOT NULL CONSTRAINT PK_DemoRequests PRIMARY KEY,
        ContractId uniqueidentifier NOT NULL REFERENCES demo.Contracts(Id),
        ContactId uniqueidentifier NOT NULL,
        Channel varchar(4) NOT NULL CHECK (Channel IN ('page','chat')),
        SourceState varchar(12) NOT NULL DEFAULT 'draft'
            CHECK (SourceState IN ('draft','review','finalized')),
        OfferId uniqueidentifier NULL REFERENCES demo.Offers(Id),
        Snapshot nvarchar(max) NULL
            CHECK (Snapshot IS NULL OR LEN(Snapshot) <= 10000),
        ReviewHash char(64) NULL,
        Protocol varchar(60) NULL,
        AcceptedAt datetimeoffset(0) NULL,
        ProjectionState varchar(12) NOT NULL DEFAULT 'pending'
            CHECK (ProjectionState IN ('pending','synced')),
        BpfState varchar(12) NOT NULL DEFAULT 'pending',
        IntegrationError varchar(80) NULL,
        CONSTRAINT UQ_DemoRequestContract UNIQUE(ContractId)
    );
    CREATE INDEX IX_DemoRequests_Contact ON demo.Requests(ContactId, Id);
END;
GO
IF OBJECT_ID(N'demo.Agreements', N'U') IS NULL
BEGIN
    CREATE TABLE demo.Agreements (
        RequestId uniqueidentifier NOT NULL CONSTRAINT PK_DemoAgreements PRIMARY KEY
            REFERENCES demo.Requests(Id),
        ContractId uniqueidentifier NOT NULL CONSTRAINT UQ_DemoAgreementContract UNIQUE
            REFERENCES demo.Contracts(Id),
        Protocol varchar(60) NOT NULL UNIQUE,
        Snapshot nvarchar(max) NOT NULL CHECK (LEN(Snapshot) <= 10000),
        AcceptedAt datetimeoffset(0) NOT NULL
    );
END;
GO
-- Formalized source evidence is append-only, including for the runtime principal.
IF OBJECT_ID(N'demo.AssistedRequests', N'U') IS NULL
BEGIN
    CREATE TABLE demo.AssistedRequests (
        RequestId uniqueidentifier NOT NULL CONSTRAINT PK_DemoAssistedRequests PRIMARY KEY
            REFERENCES demo.Requests(Id),
        OperatorContactId uniqueidentifier NOT NULL,
        Proposal nvarchar(max) NOT NULL CHECK (LEN(Proposal) <= 10000),
        Nonce varchar(36) NOT NULL,
        ApprovedAt datetimeoffset(0) NULL,
        ApproverId uniqueidentifier NULL
    );
END;
GO
CREATE OR ALTER TRIGGER demo.Agreements_Immutable ON demo.Agreements
INSTEAD OF UPDATE, DELETE AS
BEGIN
    THROW 51001, 'DEMO agreements are immutable', 1;
END;
GO
IF DATABASE_PRINCIPAL_ID(N'demo_service') IS NULL CREATE ROLE demo_service;
GRANT SELECT, INSERT, UPDATE, DELETE, EXECUTE ON SCHEMA::demo TO demo_service;
DENY UPDATE, DELETE ON OBJECT::demo.Agreements TO demo_service;
