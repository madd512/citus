SELECT citus.mitmproxy('conn.allow()');

SET citus.shard_count = 2; -- one per worker
SET citus.shard_replication_factor = 2; -- one shard per worker

CREATE TABLE agg_test (id int, val int, flag bool, kind int);
SELECT create_distributed_table('agg_test', 'id');
INSERT INTO agg_test VALUES (1, 1, true, 99), (3, 2, false, 99), (3, 3, true, 88);

-- the correct answer
SELECT bool_or(flag) FROM agg_test WHERE id = 3;

-- block queries. the other worker still exists though, so this should still work
SELECT citus.mitmproxy('conn.contains(b"bool_or").kill()');
SELECT bool_or(flag) FROM agg_test WHERE id = 3;

SELECT citus.mitmproxy('conn.allow()');
DROP TABLE agg_test;

SET citus.shard_replication_factor = 1;  -- workers contain disjoint subsets of the data

CREATE TABLE agg_test (id int, val int, flag bool, kind int);
SELECT create_distributed_table('agg_test', 'id');
INSERT INTO agg_test VALUES (1, 1, true, 99), (2, 2, false, 99), (2, 3, true, 88);

  -- it should work
SELECT citus.mitmproxy('recorder.reset()');
SELECT bool_or(flag) FROM agg_test WHERE id = 2;
SELECT count(1) FROM citus.dump_network_traffic(); -- a query was sent to the worker

  -- the query should fail, since we can't reach all the data it wants to hit
SELECT citus.mitmproxy('conn.contains(b"bool_or").kill()');
SELECT bool_or(flag) FROM agg_test WHERE id = 2;
SELECT count(1) FROM citus.dump_network_traffic(); -- a response was blocked

SELECT citus.mitmproxy('conn.allow()');
DROP TABLE agg_test;
