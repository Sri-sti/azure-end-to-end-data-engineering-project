create SCHEMA Gold

CREATE VIEW Gold.calander
AS
SELECT *
FROM OPENROWSET(
    BULK 'https://staz104shrishti.dfs.core.windows.net/silver/AdventureWorks_Calendar/',
    FORMAT='DELTA'
)AS data

SELECT * FROM Gold.calander


--customer
CREATE VIEW Gold.customers
AS
SELECT *
FROM OPENROWSET(
    BULK 'https://staz104shrishti.dfs.core.windows.net/silver/AdventureWorks_Customers/',
    FORMAT='PARQUET'
)AS data

--products
CREATE VIEW Gold.products
AS
SELECT *
FROM OPENROWSET(
    BULK 'https://staz104shrishti.dfs.core.windows.net/silver/AdventureWorks_Products/',
    FORMAT='PARQUET'
)AS data

--product subcategories
CREATE VIEW Gold.subcategories
AS
SELECT *
FROM OPENROWSET(
    BULK 'https://staz104shrishti.dfs.core.windows.net/silver/AdventureWorks_Product_Subcategories/',
    FORMAT='PARQUET'
)AS data

--sales
CREATE VIEW Gold.sales
AS
SELECT *
FROM OPENROWSET(
    BULK 'https://staz104shrishti.dfs.core.windows.net/silver/AdventureWorks_Sales/',
    FORMAT='DELTA'
)AS data

--territories
CREATE VIEW Gold.territories
AS
SELECT *
FROM OPENROWSET(
    BULK 'https://staz104shrishti.dfs.core.windows.net/silver/AdventureWorks_Territories/',
    FORMAT='PARQUET'
)AS data

--returns
CREATE VIEW Gold.returns
AS
SELECT *
FROM OPENROWSET(
    BULK 'https://staz104shrishti.dfs.core.windows.net/silver/AdventureWorks_Returns/',
    FORMAT='PARQUET'
)AS data





