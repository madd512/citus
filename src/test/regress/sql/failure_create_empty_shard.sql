SET citus.next_shard_id TO 100200;
ALTER SEQUENCE pg_catalog.pg_dist_placement_placementid_seq RESTART 30;
SET citus.shard_replication_factor TO 2;

CREATE TABLE append_tt2(id int);
SELECT create_distributed_table('append_tt2', 'id', 'append');

-- prove that master_create_empty_shard is non-transactional, BEGIN is never sent
-- (if it was then we would see an error here)
SELECT citus.mitmproxy('conn.contains(b"BEGIN").kill()');
BEGIN;
SELECT master_create_empty_shard('append_tt2');
SELECT master_create_empty_shard('append_tt2');
ROLLBACK;
  -- even though the shard was actually created the ROLLBACK drops it from our metadata
SELECT * FROM pg_dist_shard_placement;

-- kill the connection on the first server response
SELECT citus.mitmproxy('conn.matches(b"^R").kill()');
SELECT master_create_empty_shard('append_tt2');
  -- again, nothing was added to the metadata, this create failed!
SELECT * FROM pg_dist_shard_placement;

-- kill the connection when we send master_apply_shard_ddl_command
SELECT citus.mitmproxy('conn.contains(b"CREATE TABLE").kill()');
SELECT master_create_empty_shard('append_tt2');
  -- again, nothing was added to the metadata, this create failed!
SELECT * FROM pg_dist_shard_placement;

-- kill the connection when we try to close it:
SELECT citus.mitmproxy('conn.matches(b"^X").kill()');
SELECT master_create_empty_shard('append_tt2');
SELECT * FROM pg_dist_shard_placement;

DROP TABLE append_tt2;
SELECT citus.mitmproxy('conn.allow()');
