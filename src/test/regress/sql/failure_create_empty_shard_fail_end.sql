SET citus.next_shard_id TO 100200;
ALTER SEQUENCE pg_catalog.pg_dist_placement_placementid_seq RESTART 30;

CREATE TABLE append_tt2(id int);
SELECT create_distributed_table('append_tt2', 'id', 'append');

SELECT * FROM pg_dist_shard_placement;

-- kill the connection when we try to close it:
--SELECT citus.mitmproxy('flow.matches(b"^X").kill()');
--SELECT master_create_empty_shard('append_tt2');
--SELECT * FROM pg_dist_shard_placement;

SELECT citus.mitmproxy('flow.allow()');
INSERT INTO append_tt2 VALUES (1);
SELECT * FROM append_tt2;

SELECT master_create_empty_shard('append_tt2');

--  finally we have a shard to add
SELECT * FROM pg_dist_shard_placement;
INSERT INTO append_tt2 VALUES (2);
SELECT * FROM append_tt2;

DROP TABLE append_tt2;
SELECT citus.mitmproxy('flow.allow()');
