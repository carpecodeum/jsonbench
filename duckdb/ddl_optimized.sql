-- Optimized DuckDB schema with extracted typed columns using views
-- DuckDB doesn't support STORED generated columns, so we'll use views

CREATE TABLE bluesky_optimized (
    j JSON
);

-- Create a view with extracted typed columns for better performance
CREATE VIEW bluesky_typed AS
SELECT 
    j,
    j->>'$.kind' AS kind,
    j->>'$.did' AS did,
    CAST(j->>'$.time_us' AS BIGINT) AS time_us,
    TO_TIMESTAMP(CAST(j->>'$.time_us' AS BIGINT) / 1000000) AS timestamp_col,
    j->>'$.commit.operation' AS commit_operation,
    j->>'$.commit.collection' AS commit_collection,
    j->>'$.commit.rev' AS commit_rev
FROM bluesky_optimized; 