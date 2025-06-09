-- Optimized ClickHouse queries using preprocessed variant columns
-- These queries should show significant performance improvements over JSON path access

-- Q1: Event distribution using direct column access (fastest possible)
SELECT commit_collection AS event, count() AS count 
FROM bluesky_variants_test.bluesky_preprocessed 
GROUP BY commit_collection 
ORDER BY count DESC;

-- Q2: Event + user stats using typed columns with efficient filtering
SELECT commit_collection AS event, count() AS count, count(DISTINCT did) AS users 
FROM bluesky_variants_test.bluesky_preprocessed 
WHERE kind = 'commit' AND commit_operation = 'create' 
GROUP BY commit_collection 
ORDER BY count DESC;

-- Q3: Hourly patterns using native DateTime column (no conversion needed)
SELECT commit_collection AS event, toHour(timestamp_col) as hour_of_day, count() AS count 
FROM bluesky_variants_test.bluesky_preprocessed 
WHERE kind = 'commit' 
  AND commit_operation = 'create' 
  AND commit_collection IN ('app.bsky.feed.post', 'app.bsky.feed.repost', 'app.bsky.feed.like') 
GROUP BY commit_collection, toHour(timestamp_col) 
ORDER BY hour_of_day, commit_collection;

-- Q4: Earliest posters using optimized timestamp column
SELECT did as user_id, MIN(timestamp_col) AS first_post_date 
FROM bluesky_variants_test.bluesky_preprocessed 
WHERE kind = 'commit' 
  AND commit_operation = 'create' 
  AND commit_collection = 'app.bsky.feed.post' 
GROUP BY did 
ORDER BY first_post_date ASC 
LIMIT 3;

-- Q5: Longest activity spans using native DateTime operations
SELECT did as user_id, 
       dateDiff('millisecond', MIN(timestamp_col), MAX(timestamp_col)) AS activity_span 
FROM bluesky_variants_test.bluesky_preprocessed 
WHERE kind = 'commit' 
  AND commit_operation = 'create' 
  AND commit_collection = 'app.bsky.feed.post' 
GROUP BY did 
ORDER BY activity_span DESC 
LIMIT 3; 