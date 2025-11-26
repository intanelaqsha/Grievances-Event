-- =====================================================
-- STEP 1: LOAD & NORMALIZE DATA
-- =====================================================

-- Load raw data
CREATE OR REPLACE TABLE raw_data AS
SELECT 
    ROW_NUMBER() OVER () - 1 AS Raw_ID,
    *
FROM read_csv_auto('Grievances-Grid view 3.csv', header=true, all_varchar=true);

-- Normalize multi-value columns
CREATE OR REPLACE TABLE normalized_data AS
SELECT
    Raw_ID,
    ID,
    "Date Filed" AS Date_Filed,

    -- Suppliers
    CASE 
        WHEN Suppliers IS NULL OR TRIM(Suppliers) = '' THEN []
        ELSE LIST_DISTINCT(LIST_FILTER(
            LIST_TRANSFORM(
                STRING_SPLIT(REGEXP_REPLACE(Suppliers, '\[|\]', '', 'g'), ','), 
                x -> TRIM(x)
            ), x -> x != ''
        ))
    END AS Suppliers,

    -- Mills
    CASE 
        WHEN Mills IS NULL OR TRIM(Mills) = '' THEN []
        ELSE LIST_DISTINCT(LIST_FILTER(
            LIST_TRANSFORM(
                STRING_SPLIT(REGEXP_REPLACE(Mills, '\[|\]', '', 'g'), ','), 
                x -> TRIM(x)
            ), x -> x != ''
        ))
    END AS Mills,

    -- PIOConcessions
    CASE 
        WHEN "PIOConcessions-v2" IS NULL OR TRIM("PIOConcessions-v2") = '' THEN []
        ELSE LIST_DISTINCT(LIST_FILTER(
            LIST_TRANSFORM(
                STRING_SPLIT(REGEXP_REPLACE("PIOConcessions-v2", '\[|\]', '', 'g'), ','), 
                x -> TRIM(x)
            ), x -> x != ''
        ))
    END AS PIOConcessions,

    -- Issues
    CASE 
        WHEN Issues IS NULL OR TRIM(Issues) = '' THEN []
        ELSE LIST_DISTINCT(LIST_FILTER(
            LIST_TRANSFORM(
                STRING_SPLIT(REGEXP_REPLACE(Issues, '\[|\]', '', 'g'), ','), 
                x -> TRIM(x)
            ), x -> x != ''
        ))
    END AS Issues,

    -- Source
    CASE 
        WHEN Source IS NULL OR TRIM(Source) = '' THEN []
        ELSE LIST_FILTER(
            LIST_TRANSFORM(
                STRING_SPLIT(REPLACE(Source, ', ', '<<COMMA_SPACE>>'), ','), 
                x -> TRIM(REPLACE(x, '<<COMMA_SPACE>>', ', '))
            ), x -> x != ''
        )
    END AS Source_List

FROM raw_data;

-- Expand rows by Source
CREATE OR REPLACE TABLE expanded_data AS
SELECT 
    ROW_NUMBER() OVER () - 1 AS Row_ID,
    Raw_ID,
    ID,
    Date_Filed,
    Suppliers,
    Mills,
    PIOConcessions,
    Issues,
    UNNEST(CASE WHEN LEN(Source_List) = 0 THEN [NULL] ELSE Source_List END) AS Source
FROM normalized_data;


-- =====================================================
-- STEP 2: MERGE PER SOURCE USING CONNECTED COMPONENTS
-- =====================================================

-- Create pairwise edges: two rows are connected if they share a source AND overlap in Suppliers/Mills/PIOConcessions
CREATE OR REPLACE TABLE overlap_edges AS
SELECT 
    a.Row_ID AS Row_A,
    b.Row_ID AS Row_B
FROM expanded_data a
JOIN expanded_data b
  ON a.Source = b.Source
 AND a.Row_ID < b.Row_ID  -- avoid duplicate pairs
WHERE
    ARRAY_LENGTH(ARRAY_INTERSECT(a.Suppliers, b.Suppliers)) > 0
 OR ARRAY_LENGTH(ARRAY_INTERSECT(a.Mills, b.Mills)) > 0
 OR ARRAY_LENGTH(ARRAY_INTERSECT(a.PIOConcessions, b.PIOConcessions)) > 0;

-- Use recursive CTE to find connected components (each component = event)
WITH RECURSIVE components AS (
    -- Anchor: each node starts as its own component
    SELECT Row_ID AS node, Row_ID AS component
    FROM expanded_data

    UNION ALL

    -- Recursive step: assign smallest component ID among connected nodes
    SELECT e.Row_B AS node,
           c.component AS component
    FROM components c
    JOIN overlap_edges e
      ON e.Row_A = c.node
     AND e.Row_B <> c.component
)
-- Get final component ID per row
, final_components AS (
    SELECT node AS Row_ID,
           MIN(component) AS Event_Component
    FROM components
    GROUP BY node
)

-- Aggregate per Event_Component
CREATE OR REPLACE TABLE step2_events AS
SELECT
    CONCAT('EVT_', DENSE_RANK() OVER (ORDER BY fc.Event_Component)) AS Event_ID,
    e.Source,
    LIST_DISTINCT(ARRAY_CONCAT_AGG(e.Suppliers)) AS Suppliers,
    LIST_DISTINCT(ARRAY_CONCAT_AGG(e.Mills)) AS Mills,
    LIST_DISTINCT(ARRAY_CONCAT_AGG(e.PIOConcessions)) AS PIOConcessions,
    LIST_DISTINCT(ARRAY_CONCAT_AGG(e.Issues)) AS Issues,
    LIST_DISTINCT(ARRAY_AGG(e.ID)) AS Grievance_List,
    COUNT(DISTINCT e.ID) AS Grievance_Count,
    MIN(STRPTIME(e.Date_Filed, '%m/%d/%Y')) AS Earliest_Date,
    MAX(STRPTIME(e.Date_Filed, '%m/%d/%Y')) AS Latest_Date
FROM expanded_data e
JOIN final_components fc
  ON e.Row_ID = fc.Row_ID
GROUP BY fc.Event_Component, e.Source
ORDER BY Event_ID;

-- Check results
SELECT COUNT(*) AS total_events_after_step2 FROM step2_events;
SELECT * FROM step2_events;
