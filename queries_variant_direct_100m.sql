SELECT toString(data.JSON.kind) as kind, count() FROM bluesky_100m_variant.bluesky_data GROUP BY kind ORDER BY count() DESC;
SELECT toString(data.JSON.commit.collection) as collection, count() FROM bluesky_100m_variant.bluesky_data WHERE collection != '' GROUP BY collection ORDER BY count() DESC LIMIT 10;
SELECT count() FROM bluesky_100m_variant.bluesky_data WHERE toString(data.JSON.kind) = 'commit';
SELECT count() FROM bluesky_100m_variant.bluesky_data WHERE toUInt64(data.JSON.time_us) > 1700000000000000;
SELECT toString(data.JSON.commit.operation) as op, toString(data.JSON.commit.collection) as coll, count() FROM bluesky_100m_variant.bluesky_data WHERE op != '' AND coll != '' GROUP BY op, coll ORDER BY count() DESC LIMIT 5;
