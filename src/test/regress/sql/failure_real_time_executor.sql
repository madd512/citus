SELECT citus.mitmproxy('flow.allow()');

SET citus.task_executor_type TO 'real-time';

SET citus.shard_count = 2; -- one per worker
SET citus.shard_replication_factor = 2; -- one shard per worker

CREATE TABLE agg_test (id int, val int, flag bool, kind int);
SELECT create_distributed_table('agg_test', 'id');
INSERT INTO agg_test VALUES (1, 1, true, 99), (2, 2, false, 99), (2, 3, true, 88);

-- block queries, but the other worker is still here, so we should get the right results
SELECT citus.mitmproxy('flow.contains(b"bool_or").kill()');
SELECT bool_or(flag) FROM agg_test;

DROP TABLE agg_test;

SET citus.shard_replication_factor = 1;  -- workers contain disjoint subsets of the data

CREATE TABLE agg_test (id int, val int, flag bool, kind int);
SELECT create_distributed_table('agg_test', 'id');
INSERT INTO agg_test VALUES (1, 1, true, 99), (2, 2, false, 99), (2, 3, true, 88);

-- the query should fail, since we can't reach all the data it wants to hit
SELECT bool_or(flag) FROM agg_test;

DROP TABLE agg_test;

-- copied over from multi_outer_join.sql

CREATE FUNCTION create_tables() RETURNS void
AS $$
  CREATE TABLE multi_outer_join_left
  (
  	l_custkey integer not null,
  	l_name varchar(25) not null,
  	l_address varchar(40) not null,
  	l_nationkey integer not null,
  	l_phone char(15) not null,
  	l_acctbal decimal(15,2) not null,
  	l_mktsegment char(10) not null,
  	l_comment varchar(117) not null
  );
  
  CREATE TABLE multi_outer_join_right
  (
  	r_custkey integer not null,
  	r_name varchar(25) not null,
  	r_address varchar(40) not null,
  	r_nationkey integer not null,
  	r_phone char(15) not null,
  	r_acctbal decimal(15,2) not null,
  	r_mktsegment char(10) not null,
  	r_comment varchar(117) not null
  );
  COPY multi_outer_join_left FROM '/home/brian/Work/citus/src/test/regress/data/customer-1-10.data' with delimiter '|';
  COPY multi_outer_join_left FROM '/home/brian/Work/citus/src/test/regress/data/customer-11-20.data' with delimiter '|';
  COPY multi_outer_join_right FROM '/home/brian/Work/citus/src/test/regress/data/customer-1-15.data' with delimiter '|';
$$ LANGUAGE SQL;

SELECT create_tables();
SELECT create_distributed_table('multi_outer_join_left', 'l_custkey');
SELECT create_distributed_table('multi_outer_join_right', 'r_custkey');

-- run a simple outer join
SELECT
	min(l_custkey), max(l_custkey)
FROM
	multi_outer_join_left a LEFT JOIN multi_outer_join_right b ON (l_custkey = r_custkey);

-- block the COPY
SELECT citus.mitmproxy('flow.contains(b"multi_outer_join_left").kill()');
SELECT
	min(l_custkey), max(l_custkey)
FROM
	multi_outer_join_left a LEFT JOIN multi_outer_join_right b ON (l_custkey = r_custkey);

SELECT citus.mitmproxy('flow.allow()');
DROP TABLE multi_outer_join_left;
DROP TABLE multi_outer_join_right;

-- okay, try again with a higher shard replication factor

SET citus.shard_replication_factor = 2; -- one shard per worker
SELECT create_tables();
SELECT create_distributed_table('multi_outer_join_left', 'l_custkey');
SELECT create_distributed_table('multi_outer_join_right', 'r_custkey');

-- the other worker is reachable, so this should work
SELECT citus.mitmproxy('flow.contains(b"multi_outer_join_left").kill()');
SELECT
	min(l_custkey), max(l_custkey)
FROM
	multi_outer_join_left a LEFT JOIN multi_outer_join_right b ON (l_custkey = r_custkey);

SELECT citus.mitmproxy('flow.allow()');
DROP TABLE multi_outer_join_left;
DROP TABLE multi_outer_join_right;
