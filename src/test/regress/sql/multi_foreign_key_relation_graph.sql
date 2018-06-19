SET citus.next_shard_id TO 3000000;
SET client_min_messages TO LOG;
SET citus.shard_replication_factor TO 1;

CREATE FUNCTION get_foreign_key_relation(Oid, bool)
    RETURNS void
    LANGUAGE C STABLE STRICT
    AS 'citus', $$get_foreign_key_relation$$;

-- Complex case with non-distributed tables
CREATE TABLE tt1(id int PRIMARY KEY);

CREATE TABLE tt4(id int PRIMARY KEY, value_1 int REFERENCES tt4(id));

CREATE TABLE tt2(id int PRIMARY KEY, value_1 int REFERENCES tt1(id), value_2 int REFERENCES tt4(id));

CREATE TABLE tt3(id int PRIMARY KEY, value_1 int REFERENCES tt2(id), value_2 int REFERENCES tt3(id));

CREATE TABLE tt5(id int PRIMARY KEY);

CREATE TABLE tt6(id int PRIMARY KEY);

CREATE TABLE tt7(id int PRIMARY KEY, value_1 int REFERENCES tt6(id));

CREATE TABLE tt8(id int PRIMARY KEY, value_1 int REFERENCES tt6(id), value_2 int REFERENCES tt7(id));


SELECT get_foreign_key_relation('tt1'::regclass, TRUE);
SELECT get_foreign_key_relation('tt2'::regclass, TRUE);
SELECT get_foreign_key_relation('tt3'::regclass, TRUE);
SELECT get_foreign_key_relation('tt4'::regclass, TRUE);
SELECT get_foreign_key_relation('tt5'::regclass, TRUE);
SELECT get_foreign_key_relation('tt6'::regclass, TRUE);
SELECT get_foreign_key_relation('tt7'::regclass, TRUE);
SELECT get_foreign_key_relation('tt8'::regclass, TRUE);

-- Simple case with distributed tables
CREATE TABLE dtt1(id int PRIMARY KEY);
SELECT create_distributed_table('dtt1','id');

CREATE TABLE dtt2(id int PRIMARY KEY REFERENCES dtt1(id));
SELECT create_distributed_table('dtt2','id');

CREATE TABLE dtt3(id int PRIMARY KEY REFERENCES dtt2(id));
SELECT create_distributed_table('dtt3','id');

SELECT get_foreign_key_relation('dtt1'::regclass, TRUE);
SELECT get_foreign_key_relation('dtt2'::regclass, TRUE);
SELECT get_foreign_key_relation('dtt3'::regclass, TRUE);

