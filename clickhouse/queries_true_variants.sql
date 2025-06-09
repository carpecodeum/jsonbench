-- Query 1: Event distribution (grouping by commit.collection using variant elements)
SELECT 
    variantElement(commit_collection, 'String') AS event, 
    count() AS count 
FROM bluesky_true_variants.bluesky_data 
WHERE commit_collection IS NOT NULL
GROUP BY event 
ORDER BY count DESC;

-- Query 2: Event + user statistics with filtering (using variant type checking)
SELECT 
    variantElement(commit_collection, 'String') AS event, 
    count() AS count, 
    uniqExact(did) AS users 
FROM bluesky_true_variants.bluesky_data 
WHERE kind = 'commit' 
  AND variantElement(commit_operation, 'String') = 'create'
  AND commit_collection IS NOT NULL
GROUP BY event 
ORDER BY count DESC;

-- Query 3: Hourly activity patterns with timestamp analysis
SELECT 
    variantElement(commit_collection, 'String') AS event,
    toHour(timestamp_col) as hour_of_day, 
    count() AS count 
FROM bluesky_true_variants.bluesky_data 
WHERE kind = 'commit' 
  AND variantElement(commit_operation, 'String') = 'create'
  AND variantElement(commit_collection, 'String') IN ['app.bsky.feed.post', 'app.bsky.feed.repost', 'app.bsky.feed.like']
GROUP BY event, hour_of_day 
ORDER BY hour_of_day, event;

-- Query 4: Earliest posters identification
SELECT 
    did as user_id, 
    min(timestamp_col) as first_post_ts 
FROM bluesky_true_variants.bluesky_data 
WHERE kind = 'commit' 
  AND variantElement(commit_operation, 'String') = 'create'
  AND variantElement(commit_collection, 'String') = 'app.bsky.feed.post'
GROUP BY user_id 
ORDER BY first_post_ts ASC 
LIMIT 3;

-- Query 5: Longest user activity spans calculation
SELECT 
    did as user_id, 
    date_diff('milliseconds', min(timestamp_col), max(timestamp_col)) AS activity_span 
FROM bluesky_true_variants.bluesky_data 
WHERE kind = 'commit' 
  AND variantElement(commit_operation, 'String') = 'create'
  AND variantElement(commit_collection, 'String') = 'app.bsky.feed.post'
GROUP BY user_id 
ORDER BY activity_span DESC 
LIMIT 3; 