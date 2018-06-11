CREATE TABLE test (a int, b int);
SELECT create_distributed_table('test', 'a');

BEGIN;
UPDATE test SET b = 2 WHERE a = 2;
ROLLBACK;

SELECT citus.mitmproxy('conn.contains(b"UPDATE").kill()');

BEGIN;
UPDATE test SET b = 2 WHERE a = 2;
ROLLBACK;

SELECT citus.mitmproxy('conn.contains(b"BEGIN").kill()');

BEGIN;
UPDATE test SET b = 2 WHERE a = 2;
ROLLBACK;

SET citus.multi_shard_commit_protocol = '2pc';
SELECT citus.mitmproxy('conn.contains(b"COMMIT").kill()');

BEGIN;
UPDATE test SET b = 2 WHERE a = 2;
COMMIT;

SELECT citus.mitmproxy('conn.allow()');

SET citus.multi_shard_commit_protocol = '2pc';

BEGIN;
UPDATE test SET b = 2 WHERE a = 2;
COMMIT;

DROP TABLE test;
