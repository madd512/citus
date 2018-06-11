CREATE FUNCTION citus.mitmproxy(text) RETURNS TABLE(result text) AS $$
DECLARE
  command ALIAS FOR $1;
  result text;
BEGIN
  CREATE TEMPORARY TABLE mitmproxy_command (command text) ON COMMIT DROP;
  CREATE TEMPORARY TABLE mitmproxy_result (res text) ON COMMIT DROP;

  INSERT INTO mitmproxy_command VALUES (command);

  EXECUTE format('COPY mitmproxy_command TO %L', current_setting('citus.mitmfifo'));
  EXECUTE format('COPY mitmproxy_result FROM %L', current_setting('citus.mitmfifo'));

  RETURN QUERY SELECT * FROM mitmproxy_result;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION citus.clear_network_traffic() RETURNS void AS $$
BEGIN
  PERFORM citus.mitmproxy('recorder.reset()');
  RETURN; -- return void
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION citus.dump_network_traffic(
  normalize_shards bool default true,
  dump_unknown_messages bool default false
) RETURNS TABLE(result text) AS $$
DECLARE
  normalize_param text := CASE WHEN normalize_shards THEN 'True' ELSE 'False' END;
  dump_param text := CASE WHEN dump_unknown_messages THEN 'True' ELSE 'False' END;
  query text := format(
	'recorder.dump(normalize_shards=%s,dump_unknown_messages=%s)',
	normalize_param, dump_param);
BEGIN
  RETURN QUERY SELECT citus.mitmproxy(query);
END;
$$ LANGUAGE plpgsql;
