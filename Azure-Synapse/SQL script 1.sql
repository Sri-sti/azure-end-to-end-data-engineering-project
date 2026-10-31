create MASTER KEY ENCRYPTION
BY PASSWORD='Talk_To_Strangers90876'

CREATE DATABASE SCOPED CREDENTIAL MySasCredential
WITH IDENTITY = 'SHARED ACCESS SIGNATURE',
SECRET = 'sv=2026-02-06&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2026-11-02T14:59:43Z&st=2026-10-31T06:44:43Z&spr=https&sig=a4Eq1LfphkgmJB%2FrlC%2FKvgFqf%2BHLcATmnWmFmNX2psI%3D
';

CREATE EXTERNAL DATA SOURCE sriDataSource
WITH(
    LOCATION='abfss://silver@staz104shrishti.dfs.core.windows.net/synapsedevsri/',
    CREDENTIAL= MySasCredential
)

SELECT *
FROM OPENROWSET(
    BULK 'https://staz104shrishti.dfs.core.windows.net/silver/AdventureWorks_Customers/',
    DATA_SOURCE='MySasDataSource',
    FORMAT='PARAQUET'
)AS data;