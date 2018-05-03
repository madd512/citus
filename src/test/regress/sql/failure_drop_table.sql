SET citus.next_shard_id TO 100300;
ALTER SEQUENCE pg_catalog.pg_dist_placement_placementid_seq RESTART 40;

SELECT citus.mitmproxy('flow.kill()');

-- none of these commands generate network traffic
SET citus.shard_replication_factor TO 2;
CREATE TABLE append (id int);
SELECT create_distributed_table('append', 'id', 'append');
DROP TABLE append;

SELECT citus.mitmproxy('flow.allow()');
CREATE TABLE append (id int);
SELECT create_distributed_table('append', 'id', 'append');
SELECT master_create_empty_shard('append');

SELECT * FROM pg_dist_shard;
SELECT * FROM pg_dist_shard_placement;
-- drop the connection right at the beginning
SELECT citus.mitmproxy('flow.contains(b"assign_distributed_transaction_id").kill()');
DROP TABLE append;
SELECT * FROM pg_dist_shard;
SELECT * FROM pg_dist_shard_placement;

SELECT citus.mitmproxy('flow.allow()');
CREATE TABLE append (id int);
SELECT create_distributed_table('append', 'id', 'append');
SELECT master_create_empty_shard('append');

SELECT * FROM pg_dist_shard;
SELECT * FROM pg_dist_shard_placement;
SELECT citus.mitmproxy('flow.contains(b"DROP TABLE IF EXISTS").kill()');
DROP TABLE append;
SELECT * FROM pg_dist_shard;
SELECT * FROM pg_dist_shard_placement;

-- next steps:
-- PREPARE TRANSACTION
-- COMMIT PREPARED
