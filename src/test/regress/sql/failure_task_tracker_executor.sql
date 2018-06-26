SELECT citus.mitmproxy('conn.allow()');

SET citus.task_executor_type TO 'task-tracker';
SET citus.enable_unique_job_ids TO false;

SET citus.shard_count = 2; -- one per worker
SET citus.shard_replication_factor = 2; -- one shard per worker

CREATE TABLE agg_test (id int, val int, flag bool, kind int);
SELECT create_distributed_table('agg_test', 'id');
INSERT INTO agg_test VALUES (1, 1, true, 99), (2, 2, false, 99), (2, 3, true, 88);

-- the correct answer
SELECT bool_or(flag) FROM agg_test;

-- prevent coord from giving any tasks out
SELECT citus.mitmproxy('conn.contains(b"task_tracker_assign_task").killall()');

-- the other worker is still here so we should get the right results
SELECT bool_or(flag) FROM agg_test;

SELECT citus.mitmproxy('conn.contains(b"task_tracker_task_status").killall()');
SELECT bool_or(flag) FROM agg_test;

  -- kill it when the coord tries to pull results from the worker
SELECT citus.mitmproxy('conn.contains(b"TO STDOUT WITH").killall()');
SELECT bool_or(flag) FROM agg_test;

-- TODO: try failing the COPY after some data has been sent

SELECT citus.mitmproxy('conn.contains(b"task_tracker_cleanup_job").killall()');
SELECT bool_or(flag) FROM agg_test;

-- TODO: what if you fail the first task_tracker_task_status, but allow the second one?
SELECT citus.mitmproxy('conn.allow()');
DROP TABLE agg_test;

SET citus.shard_replication_factor = 1;  -- workers contain disjoint subsets of the data

CREATE TABLE agg_test (id int, val int, flag bool, kind int);
SELECT create_distributed_table('agg_test', 'id');
INSERT INTO agg_test VALUES (1, 1, true, 99), (2, 2, false, 99), (2, 3, true, 88);

-- for all of these the query should fail since we can't reach all of the shards it hits

SELECT citus.mitmproxy('conn.contains(b"task_tracker_assign_task").killall()');
SELECT bool_or(flag) FROM agg_test;

SELECT citus.mitmproxy('conn.contains(b"task_tracker_task_status").killall()');
SELECT bool_or(flag) FROM agg_test;

SELECT citus.mitmproxy('conn.contains(b"TO STDOUT WITH").killall()');
SELECT bool_or(flag) FROM agg_test;

SELECT citus.mitmproxy('conn.contains(b"task_tracker_cleanup_job").killall()');
SELECT bool_or(flag) FROM agg_test;

SELECT citus.mitmproxy('conn.allow()');
DROP TABLE agg_test;

SELECT citus.mitmproxy('conn.allow()');

-- copied over from multi_outer_join.sql

CREATE FUNCTION create_tables() RETURNS void
AS $$
  CREATE TABLE multi_outer_join_left ( l_custkey, l_name)
    AS SELECT s.a, to_char(s.a, '999') FROM generate_series(1, 20) AS s(a);
  
  CREATE TABLE multi_outer_join_right ( r_custkey, r_name)
    AS SELECT s.a, to_char(s.a, '999') FROM generate_series(1, 15) AS s(a);
$$ LANGUAGE SQL;

SELECT create_tables();
SELECT create_distributed_table('multi_outer_join_left', 'l_custkey');
SELECT create_distributed_table('multi_outer_join_right', 'r_custkey');

-- run a simple outer join
SELECT
	min(l_custkey), max(l_custkey)
FROM
	multi_outer_join_left a LEFT JOIN multi_outer_join_right b ON (l_custkey = r_custkey);

SELECT citus.mitmproxy('conn.contains(b"task_tracker_assign_task").killall()');
SELECT
	min(l_custkey), max(l_custkey)
FROM
	multi_outer_join_left a LEFT JOIN multi_outer_join_right b ON (l_custkey = r_custkey);

SELECT citus.mitmproxy('conn.contains(b"task_tracker_task_status").killall()');
SELECT
	min(l_custkey), max(l_custkey)
FROM
	multi_outer_join_left a LEFT JOIN multi_outer_join_right b ON (l_custkey = r_custkey);

SELECT citus.mitmproxy('conn.contains(b"TO STDOUT WITH").killall()');
SELECT
	min(l_custkey), max(l_custkey)
FROM
	multi_outer_join_left a LEFT JOIN multi_outer_join_right b ON (l_custkey = r_custkey);

SELECT citus.mitmproxy('conn.contains(b"task_tracker_cleanup_job").killall()');
SELECT
	min(l_custkey), max(l_custkey)
FROM
	multi_outer_join_left a LEFT JOIN multi_outer_join_right b ON (l_custkey = r_custkey);

SELECT citus.mitmproxy('conn.allow()');
DROP TABLE multi_outer_join_left;
DROP TABLE multi_outer_join_right;

-- okay, try again with a higher shard replication factor

SET citus.shard_replication_factor = 2; -- one shard per worker
SELECT create_tables();
SELECT create_distributed_table('multi_outer_join_left', 'l_custkey');
SELECT create_distributed_table('multi_outer_join_right', 'r_custkey');

-- the other worker is reachable, so these should work
SELECT citus.mitmproxy('conn.contains(b"task_tracker_assign_task").killall()');
SELECT
	min(l_custkey), max(l_custkey)
FROM
	multi_outer_join_left a LEFT JOIN multi_outer_join_right b ON (l_custkey = r_custkey);

SELECT citus.mitmproxy('conn.contains(b"task_tracker_task_status").killall()');
SELECT
	min(l_custkey), max(l_custkey)
FROM
	multi_outer_join_left a LEFT JOIN multi_outer_join_right b ON (l_custkey = r_custkey);

SELECT citus.mitmproxy('conn.contains(b"TO STDOUT WITH").killall()');
SELECT
	min(l_custkey), max(l_custkey)
FROM
	multi_outer_join_left a LEFT JOIN multi_outer_join_right b ON (l_custkey = r_custkey);

SELECT citus.mitmproxy('conn.contains(b"task_tracker_cleanup_job").killall()');
SELECT
	min(l_custkey), max(l_custkey)
FROM
	multi_outer_join_left a LEFT JOIN multi_outer_join_right b ON (l_custkey = r_custkey);

SELECT citus.mitmproxy('conn.allow()');
DROP TABLE multi_outer_join_left;
DROP TABLE multi_outer_join_right;
DROP FUNCTION create_tables();
