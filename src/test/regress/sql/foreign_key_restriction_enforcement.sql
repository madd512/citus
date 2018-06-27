-- 
-- Tests multiple commands in transactions where
-- there is foreign key relation between reference
-- tables and distributed tables
--

CREATE SCHEMA test_fkey_to_ref_in_tx;
SET search_path TO 'test_fkey_to_ref_in_tx';

SET citus.shard_replication_factor TO 1;

CREATE TABLE referece_table(id int PRIMARY KEY);
SELECT create_reference_table('referece_table');

CREATE TABLE referece_table_second(id int PRIMARY KEY);
SELECT create_reference_table('referece_table_second');


CREATE TABLE on_update_fkey_table(id int PRIMARY KEY, value_1 int);
SELECT create_distributed_table('on_update_fkey_table', 'id');

CREATE TABLE on_delete_fkey_table(id int PRIMARY KEY, value_1 int);
SELECT create_distributed_table('on_delete_fkey_table', 'id');

CREATE TABLE unrelated_dist_table(id int PRIMARY KEY, value_1 int);
SELECT create_distributed_table('unrelated_dist_table', 'id');

-- we prefer to have two different tables because
-- ON UPDATE CASCADE and ON DELETE CASCADE could have 
-- different characteristics 
ALTER TABLE on_update_fkey_table ADD CONSTRAINT fkey FOREIGN KEY(value_1) REFERENCES referece_table(id) ON UPDATE CASCADE;
ALTER TABLE on_delete_fkey_table ADD CONSTRAINT fkey_2 FOREIGN KEY(value_1) REFERENCES referece_table_second(id) ON DELETE CASCADE;

INSERT INTO referece_table SELECT i FROM generate_series(0, 100) i;
INSERT INTO referece_table_second SELECT i FROM generate_series(0, 100) i;

INSERT INTO on_update_fkey_table SELECT i, i % 100  FROM generate_series(0, 1000) i;
INSERT INTO on_delete_fkey_table SELECT i, i % 100  FROM generate_series(0, 1000) i;
INSERT INTO unrelated_dist_table SELECT i, i % 100  FROM generate_series(0, 1000) i;


-- case 1.1: SELECT to a reference table is followed by a parallel SELECT to a distributed table
BEGIN;
	SELECT count(*) FROM referece_table;
	SELECT count(*) FROM on_update_fkey_table;
ROLLBACK;

-- case 1.2: SELECT to a reference table is followed by a multiple router SELECTs to a distributed table
BEGIN;	
	SELECT count(*) FROM referece_table;
	SELECT count(*) FROM on_update_fkey_table WHERE id = 15;
	SELECT count(*) FROM on_update_fkey_table WHERE id = 16;
	SELECT count(*) FROM on_update_fkey_table WHERE id = 17;
	SELECT count(*) FROM on_update_fkey_table WHERE id = 18;
	
ROLLBACK;

-- case 1.3: SELECT to a reference table is followed by a multi-shard UPDATE to a distributed table
BEGIN;	
	SELECT count(*) FROM referece_table;
	UPDATE on_update_fkey_table SET value_1 = 16 WHERE value_1 = 15;
ROLLBACK;

-- case 1.4: SELECT to a reference table is followed by a multiple sing-shard UPDATE to a distributed table
BEGIN;	
	SELECT count(*) FROM referece_table;
	UPDATE on_update_fkey_table SET value_1 = 16 WHERE id = 15;
	UPDATE on_update_fkey_table SET value_1 = 16 WHERE id = 16;
	UPDATE on_update_fkey_table SET value_1 = 16 WHERE id = 17;
	UPDATE on_update_fkey_table SET value_1 = 16 WHERE id = 18;
ROLLBACK;

-- case 1.5: SELECT to a reference table is followed by a DDL that touches fkey column
BEGIN;	
	SELECT count(*) FROM referece_table;
	ALTER TABLE on_update_fkey_table ALTER COLUMN value_1 SET DATA TYPE bigint;
ROLLBACK;

-- case 1.6: SELECT to a reference table is followed by an unrelated DDL
BEGIN;	
	SELECT count(*) FROM referece_table;
	ALTER TABLE on_update_fkey_table ADD COLUMN X INT;
ROLLBACK;

-- case 1.6: SELECT to a reference table is followed by a COPY
BEGIN;
	SELECT count(*) FROM referece_table;
	COPY on_update_fkey_table FROM STDIN WITH CSV;
1001,99
1002,99
1003,99
1004,99
1005,99
\.
ROLLBACK;

-- case 2.1: UPDATE to a reference table is followed by a multi-shard SELECT
BEGIN;
	UPDATE referece_table SET id = 101 WHERE id = 99;
	SELECT count(*) FROM on_update_fkey_table WHERE value_1 = 99;
	SELECT count(*) FROM on_update_fkey_table WHERE value_1 = 101;
ROLLBACK;

-- case 2.2: UPDATE to a reference table is followed by multiple router SELECT
BEGIN;
	UPDATE referece_table SET id = 101 WHERE id = 99;
	SELECT count(*) FROM on_update_fkey_table WHERE value_1 = 101 AND id = 99;
	SELECT count(*) FROM on_update_fkey_table WHERE value_1 = 101 AND id = 199;
	SELECT count(*) FROM on_update_fkey_table WHERE value_1 = 101 AND id = 299;
	SELECT count(*) FROM on_update_fkey_table WHERE value_1 = 101 AND id = 399;
ROLLBACK;


-- case 2.3: UPDATE to a reference table is followed by a multi-shard UPDATE
BEGIN;
	UPDATE referece_table SET id = 101 WHERE id = 99;
	UPDATE on_update_fkey_table SET value_1 = 15;
ROLLBACK;

-- case 2.4: UPDATE to a reference table is followed by multiple router UPDATEs
BEGIN;
	UPDATE referece_table SET id = 101 WHERE id = 99;
	UPDATE on_update_fkey_table SET value_1 = 101 WHERE id = 1;
	UPDATE on_update_fkey_table SET value_1 = 101 WHERE id = 2;
	UPDATE on_update_fkey_table SET value_1 = 101 WHERE id = 3;
	UPDATE on_update_fkey_table SET value_1 = 101 WHERE id = 4;
ROLLBACK;

-- case 2.5: UPDATE to a reference table is followed by a DDL that touches fkey column
BEGIN;
	UPDATE referece_table SET id = 101 WHERE id = 99;
	ALTER TABLE on_update_fkey_table ALTER COLUMN value_1 SET DATA TYPE bigint;
ROLLBACK;

-- case 2.6: UPDATE to a reference table is followed by an unrelated DDL
BEGIN;
	UPDATE referece_table SET id = 101 WHERE id = 99;
	ALTER TABLE on_update_fkey_table ADD COLUMN value_1_X INT;
ROLLBACK;

-- case 2.7: UPDATE to a reference table is followed by COPY
BEGIN;
	UPDATE referece_table SET id = 101 WHERE id = 99;
	COPY on_update_fkey_table FROM STDIN WITH CSV;
1001,99
1002,99
1003,99
1004,99
1005,99
\.
ROLLBACK;


-- case 2.8: UPDATE to a reference table is followed by TRUNCATE
BEGIN;
	UPDATE referece_table SET id = 101 WHERE id = 99;
	TRUNCATE on_update_fkey_table;
ROLLBACK;

-- case 3.1: an unrelated DDL to a reference table is followed by a real-time SELECT
BEGIN;
	ALTER TABLE referece_table ALTER COLUMN id SET DEFAULT 1001;
	SELECT count(*) FROM on_update_fkey_table;
ROLLBACK;

-- case 3.2: DDL that touches fkey column to a reference table is followed by a real-time SELECT
BEGIN;
	ALTER TABLE referece_table ALTER COLUMN id SET DATA TYPE int;
	SELECT count(*) FROM on_update_fkey_table;
ROLLBACK;


-- case 3.3: DDL to a reference table followed by a multi shard UPDATE
BEGIN;
	ALTER TABLE referece_table ALTER COLUMN id SET DEFAULT 1001;
	UPDATE on_update_fkey_table SET value_1 = 5 WHERE id != 11;
ROLLBACK;

-- case 3.4: DDL to a reference table followed by multiple router UPDATEs
BEGIN;
	ALTER TABLE referece_table ALTER COLUMN id SET DEFAULT 1001;
	UPDATE on_update_fkey_table SET value_1 = 98 WHERE id = 1;
	UPDATE on_update_fkey_table SET value_1 = 98 WHERE id = 2;
	UPDATE on_update_fkey_table SET value_1 = 98 WHERE id = 3;
	UPDATE on_update_fkey_table SET value_1 = 98 WHERE id = 4;
ROLLBACK;


-- case 3.5: DDL to reference table followed by a DDL to dist table
BEGIN;
	ALTER TABLE referece_table ALTER COLUMN id SET DATA TYPE smallint;
	CREATE INDEX fkey_test_index_1 ON on_update_fkey_table(value_1);
ROLLBACK;

-- case 4.6: DDL to reference table followed by a DDL to dist table, both touching fkey columns
BEGIN;
	ALTER TABLE referece_table ALTER COLUMN id SET DATA TYPE smallint;
	ALTER TABLE on_delete_fkey_table ALTER COLUMN value_1 SET DATA TYPE smallint;
ROLLBACK;

-- case 3.7: DDL to a reference table is followed by COPY
BEGIN;
	ALTER TABLE referece_table  ADD COLUMN X int;
	COPY on_update_fkey_table FROM STDIN WITH CSV;
1001,99
1002,99
1003,99
1004,99
1005,99
\.
ROLLBACK;

-- case 3.8: DDL to a reference table is followed by TRUNCATE
BEGIN;
	ALTER TABLE referece_table  ADD COLUMN X int;
	TRUNCATE on_update_fkey_table;
ROLLBACK;

-- case 3.9: DDL to a reference table is followed by TRUNCATE
BEGIN;
	ALTER TABLE referece_table ALTER COLUMN id SET DATA TYPE smallint;
	TRUNCATE on_update_fkey_table;
ROLLBACK;


-----
--- Now, start testing the other way araound
-----

-- case 4.1: SELECT to a dist table is follwed by a SELECT to a reference table
BEGIN;
	SELECT count(*) FROM on_update_fkey_table WHERE value_1 = 99;
	SELECT count(*) FROM referece_table;
ROLLBACK;

-- case 4.2: SELECT to a dist table is follwed by a DML to a reference table
BEGIN;
	SELECT count(*) FROM on_update_fkey_table WHERE value_1 = 99;
	UPDATE referece_table SET id = 101 WHERE id = 99;
ROLLBACK;

-- case 4.3: SELECT to a dist table is follwed by an unrelated DDL to a reference table
BEGIN;
	SELECT count(*) FROM on_update_fkey_table WHERE value_1 = 99;
	ALTER TABLE referece_table ADD COLUMN X INT;
ROLLBACK;

-- case 4.4: SELECT to a dist table is follwed by a DDL to a reference table
BEGIN;
	SELECT count(*) FROM on_update_fkey_table WHERE value_1 = 99;
	ALTER TABLE referece_table ALTER COLUMN id SET DATA TYPE smallint;
ROLLBACK;

-- case 4.5: SELECT to a dist table is follwed by a TRUNCATE
BEGIN;
	SELECT count(*) FROM on_update_fkey_table WHERE value_1 = 99;
	TRUNCATE referece_table CASCADE;
ROLLBACK;

-- case 4.6: Router SELECT to a dist table is followed by a TRUNCATE
BEGIN;
	SELECT count(*) FROM on_update_fkey_table WHERE id = 9;
	TRUNCATE referece_table CASCADE;
ROLLBACK;

-- case 5.1: Parallel UPDATE on distributed table follow by a SELECT
BEGIN;
	UPDATE on_update_fkey_table SET value_1 = 16 WHERE value_1 = 15;
	SELECT count(*) FROM referece_table;
ROLLBACK;

-- case 5.2: Parallel UPDATE on distributed table follow by a UPDATE
BEGIN;
	UPDATE on_update_fkey_table SET value_1 = 16 WHERE value_1 = 15;
	UPDATE referece_table SET id = 160 WHERE id = 15;
ROLLBACK;

-- case 5.3: Parallel UPDATE on distributed table follow by an unrelated DDL on reference table
BEGIN;
	UPDATE on_update_fkey_table SET value_1 = 16 WHERE value_1 = 15;
	ALTER TABLE referece_table ADD COLUMN X INT;
ROLLBACK;

-- case 5.4: Parallel UPDATE on distributed table follow by a related DDL on reference table
-- FIXME: Can we do better?
BEGIN;
	UPDATE on_update_fkey_table SET value_1 = 16 WHERE value_1 = 15;
	ALTER TABLE referece_table ALTER COLUMN id SET DATA TYPE smallint;
ROLLBACK;

-- case 6:1: Unrelated parallel DDL on distributed table followed by SELECT on ref. table
BEGIN;
	ALTER TABLE on_update_fkey_table ADD COLUMN X int;
	SELECT count(*) FROM referece_table;
ROLLBACK;

-- case 6:2: Related parallel DDL on distributed table followed by SELECT on ref. table
BEGIN;
	ALTER TABLE on_update_fkey_table ALTER COLUMN value_1 SET DATA TYPE smallint;
	UPDATE referece_table SET id = 160 WHERE id = 15;
ROLLBACK;

-- case 6:3: Unrelated parallel DDL on distributed table followed by UPDATE on ref. table
BEGIN;
	ALTER TABLE on_update_fkey_table ADD COLUMN X int;
	SELECT count(*) FROM referece_table;
ROLLBACK;

-- case 6:4: Related parallel DDL on distributed table followed by SELECT on ref. table
BEGIN;
	ALTER TABLE on_update_fkey_table ADD COLUMN X int;
	UPDATE referece_table SET id = 160 WHERE id = 15;
ROLLBACK;

-- case 6:5: Unrelated parallel DDL on distributed table followed by unrelated DDL on ref. table
BEGIN;
	ALTER TABLE on_update_fkey_table ADD COLUMN X int;
	ALTER TABLE referece_table ADD COLUMN X int;
ROLLBACK;

-- case 6:6: Unrelated parallel DDL on distributed table followed by related DDL on ref. table
BEGIN;
	ALTER TABLE on_update_fkey_table ADD COLUMN X int;
	ALTER TABLE on_update_fkey_table ALTER COLUMN value_1 SET DATA TYPE smallint;
ROLLBACK;


-- some more extensive tests

-- UPDATE on dist table is followed by DELETE to reference table
BEGIN;
	UPDATE on_update_fkey_table SET value_1 = 5 WHERE id != 11;
	DELETE FROM referece_table  WHERE id = 99;
ROLLBACK;

-- an unrelated update followed by update on dist table and update
-- on reference table
BEGIN;
	UPDATE unrelated_dist_table SET value_1 = 15;
	UPDATE on_update_fkey_table SET value_1 = 5 WHERE id != 11;
	UPDATE referece_table SET id = 101 WHERE id = 99;
ROLLBACK;

DROP SCHEMA test_fkey_to_ref_in_tx CASCADE;

SET search_path TO public;


