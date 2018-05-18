SELECT citus.mitmproxy('flow.allow()');

SET citus.shard_count = 1;
SET citus.shard_replication_factor = 2; -- one shard per worker
SET citus.multi_shard_commit_protocol TO '1pc';
SET citus.next_shard_id TO 100400;
ALTER SEQUENCE pg_catalog.pg_dist_placement_placementid_seq RESTART 100;

CREATE TABLE copy_test (key int, value int);
SELECT create_distributed_table('copy_test', 'key');

COPY copy_test FROM PROGRAM 'echo 0, 0 && echo 1, 1 && echo 2, 4 && echo 3, 9' WITH CSV;

SELECT citus.mitmproxy('flow.contains(b"assign_distributed_transaction").kill()');
COPY copy_test FROM PROGRAM 'echo 0, 0 && echo 1, 1 && echo 2, 4 && echo 3, 9' WITH CSV;

SELECT citus.mitmproxy('flow.contains(b"FROM STDIN WITH").kill()');
COPY copy_test FROM PROGRAM 'echo 0, 0 && echo 1, 1 && echo 2, 4 && echo 3, 9' WITH CSV;

SELECT citus.mitmproxy('flow.matches(b"^d").kill()'); -- raw rows from the client
COPY copy_test FROM PROGRAM 'echo 0, 0 && echo 1, 1 && echo 2, 4 && echo 3, 9' WITH CSV;

SELECT count(1) FROM pg_dist_shard_placement WHERE shardid IN (
  SELECT shardid FROM pg_dist_shard WHERE logicalrelid = 'copy_test'::regclass
) AND shardstate = 3;
SELECT count(1) FROM copy_test;

SELECT citus.mitmproxy('flow.matches(b"COMMIT[^T]").kill()'); -- don't match "COMMITTED"
COPY copy_test FROM PROGRAM 'echo 0, 0 && echo 1, 1 && echo 2, 4 && echo 3, 9' WITH CSV;

SELECT count(1) FROM pg_dist_shard_placement WHERE shardid IN (
  SELECT shardid FROM pg_dist_shard WHERE logicalrelid = 'copy_test'::regclass
) AND shardstate = 3;
SELECT count(1) FROM copy_test;

SELECT citus.mitmproxy('flow.allow()');

SELECT * FROM pg_dist_shard_placement WHERE shardid IN (
  SELECT shardid FROM pg_dist_shard WHERE logicalrelid = 'copy_test'::regclass
);

DROP TABLE copy_test;
