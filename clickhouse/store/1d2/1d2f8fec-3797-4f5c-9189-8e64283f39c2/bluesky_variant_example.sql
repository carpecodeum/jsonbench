ATTACH TABLE _ UUID '6645b0aa-c6d4-404a-94c6-fa3998af6ce6'
(
    `id` UInt64,
    `metadata` Variant(Array(String), String, UInt64),
    `content` Variant(Array(UInt64), JSON, String),
    `original_json` JSON
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192
