set sql_dialect='presto';
set enable_fallback_to_original_planner=false;
set debug_skip_fold_constant=false;
-- SELECT url_extract_parameter('http://example.com/path1/p.php?k1=v1&k2=v2&k3&k4#Ref1', 'k1'); # error: errCode = 2, detailMessage = Can not found function 'url_extract_parameter'
-- SELECT url_extract_parameter('http://example.com/path1/p.php?k1=v1&k2=v2&k3&k4#Ref1', 'k2'); # error: errCode = 2, detailMessage = Can not found function 'url_extract_parameter'
-- SELECT url_extract_parameter('http://example.com/path1/p.php?k1=v1&k2=v2&k3&k4#Ref1', 'k3'); # error: errCode = 2, detailMessage = Can not found function 'url_extract_parameter'
-- SELECT url_extract_parameter('http://example.com/path1/p.php?k1=v1&k2=v2&k3&k4#Ref1', 'k4'); # error: errCode = 2, detailMessage = Can not found function 'url_extract_parameter'
-- SELECT url_extract_parameter('http://example.com/path1/p.php?k1=v1&k2=v2&k3&k4#Ref1', 'k5'); # error: errCode = 2, detailMessage = Can not found function 'url_extract_parameter'
-- SELECT url_extract_parameter('http://example.com/path1/p.php?k1=v1&k1=v2&k1&k1#Ref1', 'k1'); # error: errCode = 2, detailMessage = Can not found function 'url_extract_parameter'
-- SELECT url_extract_parameter('http://example.com/path1/p.php?k1&k1=v1&k1&k1#Ref1', 'k1'); # error: errCode = 2, detailMessage = Can not found function 'url_extract_parameter'
-- SELECT url_extract_parameter('http://example.com/path1/p.php?k=a=b=c&x=y#Ref1', 'k'); # error: errCode = 2, detailMessage = Can not found function 'url_extract_parameter'
-- SELECT url_extract_parameter('http://example.com/path1/p.php?k1=a%26k2%3Db&k2=c#Ref1', 'k2'); # error: errCode = 2, detailMessage = Can not found function 'url_extract_parameter'
-- SELECT url_extract_parameter('foo', 'k1'); # error: errCode = 2, detailMessage = Can not found function 'url_extract_parameter'
-- SELECT url_encode(', '); # error: errCode = 2, detailMessage = Can not found function 'url_encode'
-- SELECT url_encode('\uD867\uDE3D'); # error: errCode = 2, detailMessage = Can not found function 'url_encode'
SELECT url_decode(', ');
SELECT url_extract_protocol(', ');
SELECT url_extract_host(', ');
SELECT url_extract_port(', ');
SELECT url_extract_path(', ');
SELECT url_extract_query(', ');
-- SELECT url_extract_fragment(', '); # error: errCode = 2, detailMessage = Can not found function 'url_extract_fragment'
set debug_skip_fold_constant=true;
-- SELECT url_extract_parameter('http://example.com/path1/p.php?k1=v1&k2=v2&k3&k4#Ref1', 'k1'); # error: errCode = 2, detailMessage = Can not found function 'url_extract_parameter'
-- SELECT url_extract_parameter('http://example.com/path1/p.php?k1=v1&k2=v2&k3&k4#Ref1', 'k2'); # error: errCode = 2, detailMessage = Can not found function 'url_extract_parameter'
-- SELECT url_extract_parameter('http://example.com/path1/p.php?k1=v1&k2=v2&k3&k4#Ref1', 'k3'); # error: errCode = 2, detailMessage = Can not found function 'url_extract_parameter'
-- SELECT url_extract_parameter('http://example.com/path1/p.php?k1=v1&k2=v2&k3&k4#Ref1', 'k4'); # error: errCode = 2, detailMessage = Can not found function 'url_extract_parameter'
-- SELECT url_extract_parameter('http://example.com/path1/p.php?k1=v1&k2=v2&k3&k4#Ref1', 'k5'); # error: errCode = 2, detailMessage = Can not found function 'url_extract_parameter'
-- SELECT url_extract_parameter('http://example.com/path1/p.php?k1=v1&k1=v2&k1&k1#Ref1', 'k1'); # error: errCode = 2, detailMessage = Can not found function 'url_extract_parameter'
-- SELECT url_extract_parameter('http://example.com/path1/p.php?k1&k1=v1&k1&k1#Ref1', 'k1'); # error: errCode = 2, detailMessage = Can not found function 'url_extract_parameter'
-- SELECT url_extract_parameter('http://example.com/path1/p.php?k=a=b=c&x=y#Ref1', 'k'); # error: errCode = 2, detailMessage = Can not found function 'url_extract_parameter'
-- SELECT url_extract_parameter('http://example.com/path1/p.php?k1=a%26k2%3Db&k2=c#Ref1', 'k2'); # error: errCode = 2, detailMessage = Can not found function 'url_extract_parameter'
-- SELECT url_extract_parameter('foo', 'k1'); # error: errCode = 2, detailMessage = Can not found function 'url_extract_parameter'
-- SELECT url_encode(', '); # error: errCode = 2, detailMessage = Can not found function 'url_encode'
-- SELECT url_encode('\uD867\uDE3D'); # error: errCode = 2, detailMessage = Can not found function 'url_encode'
SELECT url_decode(', ');
SELECT url_extract_protocol(', ');
SELECT url_extract_host(', ');
SELECT url_extract_port(', ');
SELECT url_extract_path(', ');
SELECT url_extract_query(', ');
-- SELECT url_extract_fragment(', ') # error: errCode = 2, detailMessage = Can not found function 'url_extract_fragment'
