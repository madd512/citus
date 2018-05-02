CREATE TABLE append_tt1(id int);
SELECT create_distributed_table('append_tt1', 'id', 'append');

-- prove that master_create_empty_shard is non-transactional, BEGIN is never sent
SELECT citus.mitmproxy('flow.contains(b"BEGIN").kill()');

BEGIN;
SELECT master_create_empty_shard('append_tt1');
ROLLBACK;

--   even though the shard was actually created the ROLLBACK drops it from our metadata
SELECT * FROM pg_dist_shard_placement;

-- kill the connection when we send master_apply_shard_ddl_command
SELECT citus.mitmproxy('flow.contains(b"CREATE TABLE").kill()');
SELECT master_create_empty_shard('append_tt1');
  -- again, nothing was added to the metadata, this create failed!
SELECT * FROM pg_dist_shard_placement;

-- kill the connection when we try to close it:
SELECT citus.mitmproxy('flow.matches(b"^X").kill()');
SELECT master_create_empty_shard('append_tt1');
SELECT * FROM pg_dist_shard_placement;

-- this time it should work, the other worker gets the shard
SELECT citus.mitmproxy('flow.contains(b"CREATE TABLE").kill()');
SET citus.shard_replication_factor TO 1;
SELECT master_create_empty_shard('append_tt1');

--  finally we have a shard to add
SELECT * FROM pg_dist_shard_placement;

DROP TABLE append_tt1;
SELECT citus.mitmproxy('flow.allow()');
