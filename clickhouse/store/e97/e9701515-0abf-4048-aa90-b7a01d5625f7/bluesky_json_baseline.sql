ATTACH TABLE _ UUID 'ef73fad6-dbe6-485f-b61e-02875766f0c3'
(
    `data` JSON
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 8192
