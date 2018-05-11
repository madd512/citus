SET citus.next_shard_id TO 100300;
ALTER SEQUENCE pg_catalog.pg_dist_placement_placementid_seq RESTART 40;

SELECT citus.mitmproxy('flow.kill()');
SELECT citus.mitmproxy('recorder.reset()');

-- none of these commands generate network traffic
SET citus.shard_replication_factor TO 2;
CREATE TABLE append (id int);
SELECT create_distributed_table('append', 'id', 'append');
DROP TABLE append;

-- prove no network traffic happend
SELECT count(1) FROM citus.mitmproxy('recorder.dump()');

SELECT citus.mitmproxy('flow.allow()');
CREATE TABLE append (id int);
SELECT create_distributed_table('append', 'id', 'append');
SELECT master_create_empty_shard('append');

-- this time we intercepted some packets
SELECT count(1) FROM citus.mitmproxy('recorder.dump()');

SELECT * FROM pg_dist_shard;
SELECT * FROM pg_dist_shard_placement;
-- drop the connection right at the beginning
-- we fail before the transaction can be started, so Citus has no choice but to continue
-- with dropping the table, shards on the other worker have already been dropped
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
-- we're inside a transaction so we rollback when the connection failure happens, the
-- table is still there
SELECT * FROM pg_dist_partition;
SELECT * FROM pg_dist_shard;
SELECT * FROM pg_dist_shard_placement;

SELECT citus.mitmproxy('flow.contains(b"PREPARE TRANSACTION").kill()');
-- everything is rolled back again, we're still in a transaction
DROP TABLE append;

SELECT citus.mitmproxy('flow.contains(b"COMMIT PREPARED").kill()');
SELECT count(1) FROM pg_dist_transaction;
-- we've already sent COMMIT PREPARED to the other worker, so there's no turning back now!
-- we have, however, gained an entry in pg_dist_transaction. A later call to
-- recover_prepared_transactions() will fix that.
DROP TABLE append;
SELECT count(1) FROM pg_dist_transaction;

-- okay, clean up after ourselves
SELECT citus.mitmproxy('flow.allow()');
SELECT recover_prepared_transactions();

-- next steps:
-- PREPARE TRANSACTION
-- COMMIT PREPARED

-- variables:
-- 2PC
-- replication factor
-- append / hash distribution
