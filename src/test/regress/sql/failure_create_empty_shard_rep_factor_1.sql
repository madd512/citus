SET citus.next_shard_id TO 100100;
ALTER SEQUENCE pg_catalog.pg_dist_placement_placementid_seq RESTART 20;

CREATE TABLE append_tt1(id int);
SELECT create_distributed_table('append_tt1', 'id', 'append');

SELECT * FROM pg_dist_shard;
SELECT * FROM pg_dist_shard_placement;

SET citus.shard_replication_factor TO 1;

-- reject all connections immediately
SELECT citus.mitmproxy('conn.kill()');

-- the first one goes to the other worker, so this succeeds
SELECT master_create_empty_shard('append_tt1');

-- create a shard, try with our worker first
SELECT master_create_empty_shard('append_tt1');

-- this one goes to the other worker
SELECT master_create_empty_shard('append_tt1');

-- if we fail the connection after it's been established Citus doesn't recover
-- kill the connection when we send master_apply_shard_ddl_command
SELECT citus.mitmproxy('conn.contains(b"CREATE TABLE").kill()');
SELECT master_create_empty_shard('append_tt1');

SELECT * FROM pg_dist_shard;
SELECT * FROM pg_dist_shard_placement;

SELECT citus.mitmproxy('conn.allow()');
DROP TABLE append_tt1;
