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
