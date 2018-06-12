SET citus.next_shard_id TO 3000000;
SET client_min_messages TO LOG;
SET citus.shard_replication_factor TO 1;

CREATE FUNCTION get_referencing_relation_id_list(Oid)
    RETURNS SETOF Oid
    LANGUAGE C STABLE STRICT
    AS 'citus', $$get_referencing_relation_id_list$$;

CREATE FUNCTION get_referenced_relation_id_list(Oid)
    RETURNS SETOF Oid
    LANGUAGE C STABLE STRICT
    AS 'citus', $$get_referenced_relation_id_list$$;

-- Complex case with non-distributed tables
CREATE TABLE tt1(id int PRIMARY KEY);

CREATE TABLE tt4(id int PRIMARY KEY, value_1 int REFERENCES tt4(id));

CREATE TABLE tt2(id int PRIMARY KEY, value_1 int REFERENCES tt1(id), value_2 int REFERENCES tt4(id));

CREATE TABLE tt3(id int PRIMARY KEY, value_1 int REFERENCES tt2(id), value_2 int REFERENCES tt3(id));

CREATE TABLE tt5(id int PRIMARY KEY);

CREATE TABLE tt6(id int PRIMARY KEY);

CREATE TABLE tt7(id int PRIMARY KEY, value_1 int REFERENCES tt6(id));

CREATE TABLE tt8(id int PRIMARY KEY, value_1 int REFERENCES tt6(id), value_2 int REFERENCES tt7(id));

-- Simple case with distributed tables
CREATE TABLE dtt1(id int PRIMARY KEY);
SELECT create_distributed_table('dtt1','id');

CREATE TABLE dtt2(id int PRIMARY KEY REFERENCES dtt1(id));
SELECT create_distributed_table('dtt2','id');

CREATE TABLE dtt3(id int PRIMARY KEY REFERENCES dtt2(id));
SELECT create_distributed_table('dtt3','id');

SELECT get_referencing_relation_id_list::regclass FROM get_referencing_relation_id_list('tt1'::regclass) ORDER BY 1;
SELECT get_referencing_relation_id_list::regclass FROM get_referencing_relation_id_list('tt2'::regclass) ORDER BY 1;
SELECT get_referencing_relation_id_list::regclass FROM get_referencing_relation_id_list('tt3'::regclass) ORDER BY 1;
SELECT get_referencing_relation_id_list::regclass FROM get_referencing_relation_id_list('tt4'::regclass) ORDER BY 1;
SELECT get_referencing_relation_id_list::regclass FROM get_referencing_relation_id_list('tt5'::regclass) ORDER BY 1;
SELECT get_referencing_relation_id_list::regclass FROM get_referencing_relation_id_list('tt6'::regclass) ORDER BY 1;
SELECT get_referencing_relation_id_list::regclass FROM get_referencing_relation_id_list('tt7'::regclass) ORDER BY 1;
SELECT get_referencing_relation_id_list::regclass FROM get_referencing_relation_id_list('tt8'::regclass) ORDER BY 1;

SELECT get_referenced_relation_id_list::regclass FROM get_referenced_relation_id_list('tt1'::regclass) ORDER BY 1;
SELECT get_referenced_relation_id_list::regclass FROM get_referenced_relation_id_list('tt2'::regclass) ORDER BY 1;
SELECT get_referenced_relation_id_list::regclass FROM get_referenced_relation_id_list('tt3'::regclass) ORDER BY 1;
SELECT get_referenced_relation_id_list::regclass FROM get_referenced_relation_id_list('tt4'::regclass) ORDER BY 1;
SELECT get_referenced_relation_id_list::regclass FROM get_referenced_relation_id_list('tt5'::regclass) ORDER BY 1;
SELECT get_referenced_relation_id_list::regclass FROM get_referenced_relation_id_list('tt6'::regclass) ORDER BY 1;
SELECT get_referenced_relation_id_list::regclass FROM get_referenced_relation_id_list('tt7'::regclass) ORDER BY 1;
SELECT get_referenced_relation_id_list::regclass FROM get_referenced_relation_id_list('tt8'::regclass) ORDER BY 1;

SELECT get_referencing_relation_id_list::regclass FROM get_referencing_relation_id_list('dtt1'::regclass) ORDER BY 1;
SELECT get_referencing_relation_id_list::regclass FROM get_referencing_relation_id_list('dtt2'::regclass) ORDER BY 1;
SELECT get_referencing_relation_id_list::regclass FROM get_referencing_relation_id_list('dtt3'::regclass) ORDER BY 1;

/* Check to see whether cache invalidation works with create table */
CREATE TABLE tt9(id int PRIMARY KEY, value_1 int REFERENCES tt8(id));

SELECT get_referencing_relation_id_list::regclass FROM get_referencing_relation_id_list('tt6'::regclass) ORDER BY 1;
SELECT get_referencing_relation_id_list::regclass FROM get_referencing_relation_id_list('tt7'::regclass) ORDER BY 1;
SELECT get_referencing_relation_id_list::regclass FROM get_referencing_relation_id_list('tt8'::regclass) ORDER BY 1;
SELECT get_referencing_relation_id_list::regclass FROM get_referencing_relation_id_list('tt9'::regclass) ORDER BY 1;

CREATE TABLE dtt4(id int PRIMARY KEY);
SELECT create_distributed_table('dtt4', 'id');

SELECT get_referencing_relation_id_list::regclass FROM get_referencing_relation_id_list('dtt4'::regclass) ORDER BY 1;

ALTER TABLE dtt4 ADD CONSTRAINT dtt4_fkey FOREIGN KEY (id) REFERENCES dtt3(id);

SELECT get_referenced_relation_id_list::regclass FROM get_referenced_relation_id_list('dtt4'::regclass) ORDER BY 1;
SELECT get_referencing_relation_id_list::regclass FROM get_referencing_relation_id_list('dtt4'::regclass) ORDER BY 1;


