create MASTER KEY ENCRYPTION
BY PASSWORD='Talk_To_Strangers90876'
CREATE DATABASE mbmeet

CREATE DATABASE SCOPED CREDENTIAL MySasCredential
WITH IDENTITY = 'SHARED ACCESS SIGNATURE',
SECRET = 'sv=2026-02-06&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2026-11-02T14:59:43Z&st=2026-10-31T06:44:43Z&spr=https&sig=a4Eq1LfphkgmJB%2FrlC%2FKvgFqf%2BHLcATmnWmFmNX2psI%3D
';
  

CREATE EXTERNAL DATA SOURCE sriDataSource
WITH (
    LOCATION = 'abfss://gold@staz104shrishti.dfs.core.windows.net',
    CREDENTIAL = MySasCredential
);
--DROP EXTERNAL DATA SOURCE sriDataSource


CREATE EXTERNAL FILE FORMAT format_delta
WITH(
    FORMAT_TYPE= DELTA
)

CREATE EXTERNAL FILE FORMAT format_parquet
WITH(
    FORMAT_TYPE= PARQUET
)


CREATE EXTERNAL TABLE gold.customers1
WITH(
    LOCATION='customerSri',
    DATA_SOURCE= sriDataSource,
    FILE_FORMAT=format_parquet
)
as 
SELECT * FROM gold.customers


SELECT name, location, credential_id FROM sys.external_data_sources;
SELECT * FROM sys.database_scoped_credentials;

ALTER DATABASE SCOPED CREDENTIAL MySasCredential
   WITH IDENTITY = 'SHARED ACCESS SIGNATURE',
   SECRET = 'sv=2026-02-06&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2026-11-02T14:59:43Z&st=2026-10-31T06:44:43Z&spr=https&sig=a4Eq1LfphkgmJB%2FrlC%2FKvgFqf%2BHLcATmnWmFmNX2psI%3D';

SELECT *
FROM OPENROWSET(
    BULK 'https://staz104shrishti.dfs.core.windows.net/silver/AdventureWorks_Customers/',
    --DATA_SOURCE='MySasDataSource',
    FORMAT='PARAQUET'
)

SELECT *
FROM OPENROWSET(
    BULK 'https://staz104shrishti.dfs.core.windows.net/silver/AdventureWorks_Calendar/',
    --DATA_SOURCE='MySasDataSource',
    FORMAT='DELTA'
)AS data