SELECT toString(data.kind) as kind, count() FROM bluesky_1m.bluesky GROUP BY toString(data.kind) ORDER BY count() DESC;
SELECT toString(data.commit.collection) as collection, count() FROM bluesky_1m.bluesky WHERE toString(data.commit.collection) != '' GROUP BY toString(data.commit.collection) ORDER BY count() DESC LIMIT 10;
SELECT count() FROM bluesky_1m.bluesky WHERE toString(data.kind) = 'commit';
SELECT count() FROM bluesky_1m.bluesky WHERE toUInt64(data.time_us) > 1700000000000000;
SELECT toString(data.commit.operation) as op, toString(data.commit.collection) as coll, count() FROM bluesky_1m.bluesky WHERE toString(data.commit.operation) != '' AND toString(data.commit.collection) != '' GROUP BY toString(data.commit.operation), toString(data.commit.collection) ORDER BY count() DESC LIMIT 5;
