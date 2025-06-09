-- Optimized queries using the typed view instead of JSON paths
-- These should be significantly faster than the original JSON path queries

-- Q1: Event distribution using typed column
SELECT commit_collection AS event, count() AS count 
FROM bluesky_typed 
GROUP BY commit_collection 
ORDER BY count DESC;

-- Q2: Event + user stats using typed columns
SELECT commit_collection AS event, count() AS count, count(DISTINCT did) AS users 
FROM bluesky_typed 
WHERE kind = 'commit' AND commit_operation = 'create' 
GROUP BY commit_collection 
ORDER BY count DESC;

-- Q3: Hourly patterns using typed columns
SELECT commit_collection AS event, hour(timestamp_col) as hour_of_day, count() AS count 
FROM bluesky_typed 
WHERE kind = 'commit' 
  AND commit_operation = 'create' 
  AND commit_collection IN ('app.bsky.feed.post', 'app.bsky.feed.repost', 'app.bsky.feed.like') 
GROUP BY commit_collection, hour(timestamp_col) 
ORDER BY hour_of_day, commit_collection;

-- Q4: Earliest posters using typed columns
SELECT did as user_id, MIN(timestamp_col) AS first_post_date 
FROM bluesky_typed 
WHERE kind = 'commit' 
  AND commit_operation = 'create' 
  AND commit_collection = 'app.bsky.feed.post' 
GROUP BY did 
ORDER BY first_post_date ASC 
LIMIT 3;

-- Q5: Longest activity spans using typed columns
SELECT did as user_id, 
       date_diff('milliseconds', MIN(timestamp_col), MAX(timestamp_col)) AS activity_span 
FROM bluesky_typed 
WHERE kind = 'commit' 
  AND commit_operation = 'create' 
  AND commit_collection = 'app.bsky.feed.post' 
GROUP BY did 
ORDER BY activity_span DESC 
LIMIT 3; 