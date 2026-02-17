USE financial_aid_db;
GO

IF OBJECT_ID('tempdb..#AidDistributionCube') IS NOT NULL
    DROP TABLE #AidDistributionCube;


-- Query 1: Aid Distribution Cube
-- Analyze total aid distribution by country across all sectors and years since 2020.

SELECT
    COALESCE(c.Country_Name, 'ALL COUNTRIES') AS Country,
    COALESCE(s.Sub_Sector_Name, 'ALL SECTORS') AS Sector,
    COALESCE(CAST(CAST(f.Time_Key / 10 AS INT) AS VARCHAR), 'ALL YEARS') AS Year,

    COUNT(DISTINCT f.Aid_Fact_Key) AS Number_of_Transactions,
    SUM(f.Value_USD) AS Total_Aid_USD,
    AVG(f.Value_USD) AS Avg_Transaction_Size_USD,

    GROUPING(c.Country_Name) AS Country_Subtotal,
    GROUPING(s.Sub_Sector_Name) AS Sector_Subtotal,
    GROUPING(CAST(f.Time_Key / 10 AS INT)) AS Year_Subtotal
INTO #AidDistributionCube
FROM fact_aid_transactions f
    INNER JOIN dim_recipient_org ro 
        ON f.Recipient_Org_Key = ro.Recipient_Org_Key
    INNER JOIN dim_recipient_country c 
        ON ro.Recipient_Country_Key = c.Recipient_Country_Key
    INNER JOIN dim_sub_sector s 
        ON f.Sub_Sector_Key = s.Sub_Sector_Key
WHERE
    f.Value_USD IS NOT NULL
    AND CAST(f.Time_Key / 10 AS INT) >= 2020
GROUP BY
    CUBE(c.Country_Name, s.Sub_Sector_Name, CAST(f.Time_Key / 10 AS INT));

-- Step 2: Query cube table
SELECT TOP 10
    Country,
    FORMAT(Total_Aid_USD, 'C0') AS Total_Aid_Formatted,
    FORMAT(Number_of_Transactions, 'N0') AS Transactions,
    FORMAT(Avg_Transaction_Size_USD, 'C0') AS Avg_Transaction_Size
FROM #AidDistributionCube
WHERE
    Sector_Subtotal = 1
    AND Year_Subtotal = 1
    AND Country_Subtotal = 0
ORDER BY Total_Aid_USD DESC;






-- Query 2: Aid Effectiveness Ranking
-- Rank sectors within each year based on total aid received.
-- Identify high-impact sectors (top 15%) and measure funding concentration.

WITH SectorAidSummary AS (
    SELECT
        sec.Sector_Category,
        sub.Sub_Sector_Name,
        CAST(LEFT(CAST(f.Time_Key AS VARCHAR), 4) AS INT) AS Calendar_Year,
        COUNT(DISTINCT f.Aid_Fact_Key) AS Transaction_Count,
        SUM(f.Value_USD) AS Total_Aid_USD,
        AVG(f.Value_USD) AS Avg_Transaction_USD
    FROM fact_aid_transactions f
        INNER JOIN dim_sub_sector sub 
            ON f.Sub_Sector_Key = sub.Sub_Sector_Key
        INNER JOIN dim_sector sec 
            ON sub.Sector_Key = sec.Sector_Key
    WHERE
        f.Value_USD > 0
        AND CAST(LEFT(CAST(f.Time_Key AS VARCHAR), 4) AS INT)
            IN (2023, 2024, 2025)
    GROUP BY
        sec.Sector_Category,
        sub.Sub_Sector_Name,
        LEFT(CAST(f.Time_Key AS VARCHAR), 4)
),

RankedSectors AS (
    SELECT
        *,
        PERCENTILE_CONT(0.85) 
            WITHIN GROUP (ORDER BY Total_Aid_USD)
            OVER (PARTITION BY Calendar_Year) AS Percentile_85_Threshold
    FROM SectorAidSummary
)

SELECT
    Sector_Category,
    Sub_Sector_Name,
    Calendar_Year AS Year,

    FORMAT(Total_Aid_USD, 'C0') AS Total_Aid,
    FORMAT(Transaction_Count, 'N0') AS Transactions,

    DENSE_RANK() OVER (
        PARTITION BY Calendar_Year
        ORDER BY Total_Aid_USD DESC
    ) AS Sector_Rank,

    FORMAT(
        Total_Aid_USD * 100.0 
        / SUM(Total_Aid_USD) OVER (PARTITION BY Calendar_Year),
        'N2'
    ) + '%' AS Percent_of_Total_Aid,

    FORMAT(
        PERCENT_RANK() OVER (
            PARTITION BY Calendar_Year
            ORDER BY Total_Aid_USD
        ),
        'P2'
    ) AS Percentile_Rank,

    FORMAT(Avg_Transaction_USD, 'C0') AS Avg_Transaction_Size

FROM RankedSectors
WHERE Total_Aid_USD >= Percentile_85_Threshold
ORDER BY Calendar_Year DESC, Total_Aid_USD DESC;






-- Query 3: Donor Organization Performance
-- This query evaluates the performance of major donor organizations between 2022 and 2025. 
WITH DonorAnnualContributions AS (
    SELECT
        prov.Provider_Org,
        LEFT(prov.Provider_Org_Type, 30) AS Org_Type,

        -- Extract Year from Time_Key
        CAST(LEFT(CAST(f.Time_Key AS VARCHAR),4) AS INT) AS Calendar_Year,

        COUNT(DISTINCT f.Aid_Fact_Key) AS Projects_Funded,
        SUM(f.Value_USD) AS Total_Contribution_USD,
        AVG(f.Value_USD) AS Avg_Project_Size_USD,
        COUNT(DISTINCT ro.Recipient_Country_Key) AS Countries_Served

    FROM fact_aid_transactions f
        INNER JOIN dim_provider_org prov 
            ON f.Provider_Org_Key = prov.Provider_Org_Key
        INNER JOIN dim_recipient_org ro 
            ON f.Recipient_Org_Key = ro.Recipient_Org_Key

    WHERE
        f.Value_USD > 0
        AND CAST(LEFT(CAST(f.Time_Key AS VARCHAR),4) AS INT)
            BETWEEN 2022 AND 2025

    GROUP BY
        prov.Provider_Org,
        prov.Provider_Org_Type,
        LEFT(CAST(f.Time_Key AS VARCHAR),4)
),

DonorWithComparisons AS (
    SELECT
        Provider_Org,
        Org_Type,
        Calendar_Year,
        Projects_Funded,
        Countries_Served,
        Total_Contribution_USD,
        Avg_Project_Size_USD,

        LAG(Total_Contribution_USD) OVER (
            PARTITION BY Provider_Org
            ORDER BY Calendar_Year
        ) AS Previous_Year_Contribution,

        LEAD(Total_Contribution_USD) OVER (
            PARTITION BY Provider_Org
            ORDER BY Calendar_Year
        ) AS Next_Year_Contribution,

        FIRST_VALUE(Total_Contribution_USD) OVER (
            PARTITION BY Provider_Org
            ORDER BY Total_Contribution_USD DESC
        ) AS Best_Year_Contribution,

        AVG(Total_Contribution_USD) OVER (
            PARTITION BY Provider_Org
        ) AS Donor_Avg_Contribution,

        DENSE_RANK() OVER (
            PARTITION BY Calendar_Year
            ORDER BY Total_Contribution_USD DESC
        ) AS Donor_Rank

    FROM DonorAnnualContributions
    WHERE Total_Contribution_USD > 1000000
)

SELECT TOP 30
    UPPER(LEFT(Provider_Org, 50)) AS Donor_Organization,
    Org_Type,
    Calendar_Year AS Year,

    FORMAT(Total_Contribution_USD, 'C0') AS Total_Contribution,
    FORMAT(Projects_Funded, 'N0') AS Projects,
    Countries_Served,
    Donor_Rank,

    CASE
        WHEN Previous_Year_Contribution IS NOT NULL THEN
            FORMAT(
                (Total_Contribution_USD - Previous_Year_Contribution)
                * 100.0 / Previous_Year_Contribution,
                'N1'
            ) + '%'
        ELSE 'N/A'
    END AS YoY_Growth,

    FORMAT(
        (Total_Contribution_USD - Donor_Avg_Contribution)
        * 100.0 / Donor_Avg_Contribution,
        'N1'
    ) + '%'
    AS Performance_vs_Avg,

    FORMAT(
        Total_Contribution_USD * 100.0 / Best_Year_Contribution,
        'N1'
    ) + '%'
    AS Percent_of_Best_Year

FROM DonorWithComparisons
WHERE Calendar_Year = 2025
ORDER BY Donor_Rank, Total_Contribution_USD DESC;




-- Query 4: Aid Flow Trends with Moving Averages
USE financial_aid_db;
GO

IF OBJECT_ID('tempdb..#QuarterlyAidFlows') IS NOT NULL
    DROP TABLE #QuarterlyAidFlows;


WITH QuarterlyBase AS (
    SELECT
        CAST(f.Time_Key / 10 AS INT) AS Calendar_Year,
        CAST(f.Time_Key % 10 AS INT) AS Quarter_Number,

        COUNT(DISTINCT f.Aid_Fact_Key) AS Transactions,
        SUM(f.Value_USD) AS Total_Aid_USD,
        AVG(f.Value_USD) AS Avg_Transaction_USD,
        COUNT(DISTINCT ro.Recipient_Country_Key) AS Countries_Receiving_Aid

    FROM fact_aid_transactions f
        INNER JOIN dim_recipient_org ro 
            ON f.Recipient_Org_Key = ro.Recipient_Org_Key

    WHERE
        f.Value_USD > 0
        AND CAST(f.Time_Key / 10 AS INT) >= 2020

    GROUP BY
        CAST(f.Time_Key / 10 AS INT),
        CAST(f.Time_Key % 10 AS INT)
)

SELECT
    Calendar_Year,
    CONCAT('Q', Quarter_Number) AS Calendar_Quarter,
    CONCAT(Calendar_Year, '-Q', Quarter_Number) AS Year_Quarter,
    (Calendar_Year * 4) + Quarter_Number AS Quarter_Index,

    Transactions,
    Total_Aid_USD,
    Avg_Transaction_USD,
    Countries_Receiving_Aid,

    -- 4-quarter moving average
    AVG(Total_Aid_USD) OVER (
        ORDER BY Calendar_Year, Quarter_Number
        ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
    ) AS Moving_Avg_4Q,

    -- 8-quarter moving average
    AVG(Total_Aid_USD) OVER (
        ORDER BY Calendar_Year, Quarter_Number
        ROWS BETWEEN 7 PRECEDING AND CURRENT ROW
    ) AS Moving_Avg_8Q,

    -- Previous quarter value
    LAG(Total_Aid_USD, 1) OVER (
        ORDER BY Calendar_Year, Quarter_Number
    ) AS Previous_Quarter_Aid

INTO #QuarterlyAidFlows
FROM QuarterlyBase;


SELECT
    Year_Quarter,
    FORMAT(Total_Aid_USD, 'C0') AS Total_Aid,
    FORMAT(Transactions, 'N0') AS Transaction_Count,
    FORMAT(Avg_Transaction_USD, 'C0') AS Avg_Transaction,
    Countries_Receiving_Aid,

    FORMAT(Moving_Avg_4Q, 'C0') AS [4Q_Moving_Avg],
    FORMAT(Moving_Avg_8Q, 'C0') AS [8Q_Moving_Avg],

    CASE
        WHEN Previous_Quarter_Aid IS NOT NULL 
             AND Previous_Quarter_Aid > 0
        THEN FORMAT(
                (Total_Aid_USD - Previous_Quarter_Aid)
                * 100.0 / Previous_Quarter_Aid,
                'N1'
             ) + '%'
        ELSE 'N/A'
    END AS QoQ_Growth,

    CASE
        WHEN Total_Aid_USD > Moving_Avg_4Q * 1.1 THEN 'Above Trend'
        WHEN Total_Aid_USD < Moving_Avg_4Q * 0.9 THEN 'Below Trend'
        ELSE 'On Trend'
    END AS Trend_Status

FROM #QuarterlyAidFlows
WHERE Quarter_Index >= (
    SELECT MIN(Quarter_Index) + 3 
    FROM #QuarterlyAidFlows
)
ORDER BY Quarter_Index DESC;


SELECT DISTINCT TOP 50 Time_Key
FROM fact_aid_transactions
ORDER BY Time_Key DESC;