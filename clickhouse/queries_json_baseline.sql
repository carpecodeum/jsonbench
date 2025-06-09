-- JSON Object baseline queries for performance comparison
-- These use the original JSON Object approach with path expressions

-- Q1: Event distribution using JSON path access
SELECT data.commit.collection::String AS event, count() AS count 
FROM bluesky_variants_test.bluesky_json_baseline 
GROUP BY event 
ORDER BY count DESC;

-- Q2: Event + user stats using JSON path access with filtering
SELECT data.commit.collection::String AS event, count() AS count, uniqExact(data.did::String) AS users 
FROM bluesky_variants_test.bluesky_json_baseline 
WHERE data.kind::String = 'commit' AND data.commit.operation::String = 'create' 
GROUP BY event 
ORDER BY count DESC;

-- Q3: Hourly patterns using JSON timestamp conversion
SELECT data.commit.collection::String AS event, toHour(fromUnixTimestamp64Micro(data.time_us::UInt64)) as hour_of_day, count() AS count 
FROM bluesky_variants_test.bluesky_json_baseline 
WHERE data.kind::String = 'commit' 
  AND data.commit.operation::String = 'create' 
  AND data.commit.collection::String in ('app.bsky.feed.post', 'app.bsky.feed.repost', 'app.bsky.feed.like') 
GROUP BY event, hour_of_day 
ORDER BY hour_of_day, event;

-- Q4: Earliest posters using JSON timestamp conversion
SELECT data.did::String as user_id, min(fromUnixTimestamp64Micro(data.time_us::UInt64)) as first_post_ts 
FROM bluesky_variants_test.bluesky_json_baseline 
WHERE data.kind::String = 'commit' 
  AND data.commit.operation::String = 'create' 
  AND data.commit.collection::String = 'app.bsky.feed.post' 
GROUP BY user_id 
ORDER BY first_post_ts ASC 
LIMIT 3;

-- Q5: Longest activity spans using JSON timestamp conversion
SELECT data.did::String as user_id, 
       date_diff('milliseconds', min(fromUnixTimestamp64Micro(data.time_us::UInt64)), max(fromUnixTimestamp64Micro(data.time_us::UInt64))) AS activity_span 
FROM bluesky_variants_test.bluesky_json_baseline 
WHERE data.kind::String = 'commit' 
  AND data.commit.operation::String = 'create' 
  AND data.commit.collection::String = 'app.bsky.feed.post' 
GROUP BY user_id 
ORDER BY activity_span DESC 
LIMIT 3; 