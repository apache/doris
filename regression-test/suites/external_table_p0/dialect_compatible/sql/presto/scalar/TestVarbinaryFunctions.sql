set sql_dialect='presto';
set enable_fallback_to_original_planner=false;
set debug_skip_fold_constant=false;
SELECT length(CAST('' AS VARBINARY));
SELECT length(CAST('a' AS VARBINARY));
SELECT length(CAST('abc' AS VARBINARY));
-- SELECT CONCAT(CAST('foo' AS VARBINARY), CAST ('bar' AS VARBINARY)); # differ: doris : foobar, presto : Zm9vYmFy
-- SELECT CONCAT(CAST('foo' AS VARBINARY), CAST ('bar' AS VARBINARY), CAST ('baz' AS VARBINARY)); # differ: doris : foobarbaz, presto : Zm9vYmFyYmF6
-- SELECT CONCAT(CAST('foo' AS VARBINARY), CAST ('bar' AS VARBINARY), CAST ('bazbaz' AS VARBINARY)); # differ: doris : foobarbazbaz, presto : Zm9vYmFyYmF6YmF6
-- SELECT CONCAT(X'000102', X'AAABAC', X'FDFEFF'); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT CONCAT(X'000102', X'AAABAC', X'FDFEFF');	               ^	Encountered: STRING LITERAL	Expected: ||, COMMA, .	
-- SELECT CONCAT(X'CAFFEE', X'F7', X'DE58'); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT CONCAT(X'CAFFEE', X'F7', X'DE58');	               ^	Encountered: STRING LITERAL	Expected: ||, COMMA, .	
-- SELECT CONCAT(X'58', X'F7'); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT CONCAT(X'58', X'F7');	               ^	Encountered: STRING LITERAL	Expected: ||, COMMA, .	
-- SELECT CONCAT(X'', X'58', X'F7'); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT CONCAT(X'', X'58', X'F7');	               ^	Encountered: STRING LITERAL	Expected: ||, COMMA, .	
-- SELECT CONCAT(X'58', X'', X'F7'); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT CONCAT(X'58', X'', X'F7');	               ^	Encountered: STRING LITERAL	Expected: ||, COMMA, .	
-- SELECT CONCAT(X'58', X'F7', X''); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT CONCAT(X'58', X'F7', X'');	               ^	Encountered: STRING LITERAL	Expected: ||, COMMA, .	
-- SELECT concat(X'', X'58', X'', X'F7', X''); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT concat(X'', X'58', X'', X'F7', X'');	               ^	Encountered: STRING LITERAL	Expected: ||, COMMA, .	
-- SELECT concat(X'', X'', X'', X'', X'', X''); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT concat(X'', X'', X'', X'', X'', X'');	               ^	Encountered: STRING LITERAL	Expected: ||, COMMA, .	
SELECT to_base64(CAST('' AS VARBINARY));
SELECT to_base64(CAST('a' AS VARBINARY));
SELECT to_base64(CAST('abc' AS VARBINARY));
SELECT to_base64(CAST('hello world' AS VARBINARY));
SELECT from_base64(to_base64(CAST('' AS VARBINARY)));
-- SELECT from_base64(to_base64(CAST('a' AS VARBINARY))); # differ: doris : a, presto : YQ==
-- SELECT from_base64(to_base64(CAST('abc' AS VARBINARY))); # differ: doris : abc, presto : YWJj
SELECT from_base64(CAST(to_base64(CAST('' AS VARBINARY)) AS VARBINARY));
-- SELECT from_base64(CAST(to_base64(CAST('a' AS VARBINARY)) AS VARBINARY)); # differ: doris : a, presto : YQ==
-- SELECT from_base64(CAST(to_base64(CAST('abc' AS VARBINARY)) AS VARBINARY)); # differ: doris : abc, presto : YWJj
-- SELECT to_base64url(CAST('a' AS VARBINARY)); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT to_base64url(CAST('a' AS VARBINARY));	                                ^	Encountered: IDENTIFIER	Expected	
-- SELECT to_base64url(CAST('abc' AS VARBINARY)); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT to_base64url(CAST('abc' AS VARBINARY));	                                  ^	Encountered: IDENTIFIER	Expected	
-- SELECT to_base64url(CAST('hello world' AS VARBINARY)); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT to_base64url(CAST('hello world' AS VARBINARY));	                                          ^	Encountered: IDENTIFIER	Expected	
-- SELECT from_base64url(to_base64url(CAST('' AS VARBINARY))); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT from_base64url(to_base64url(CAST('' AS VARBINARY)));	                                              ^	Encountered: IDENTIFIER	Expected	
-- SELECT from_base64url(to_base64url(CAST('a' AS VARBINARY))); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT from_base64url(to_base64url(CAST('a' AS VARBINARY)));	                                               ^	Encountered: IDENTIFIER	Expected	
-- SELECT from_base64url(to_base64url(CAST('abc' AS VARBINARY))); # error: errCode = 2, detailMessage = Syntax error in line 1:	...o_base64url(CAST('abc' AS VARBINARY)));	                             ^	Encountered: IDENTIFIER	Expected	
-- SELECT from_base64url(CAST(to_base64url(CAST('' AS VARBINARY)) AS VARBINARY)); # error: errCode = 2, detailMessage = Syntax error in line 1:	...T(to_base64url(CAST('' AS VARBINARY)) AS VARBINARY));	                             ^	Encountered: IDENTIFIER	Expected	
-- SELECT from_base64url(CAST(to_base64url(CAST('a' AS VARBINARY)) AS VARBINARY)); # error: errCode = 2, detailMessage = Syntax error in line 1:	...(to_base64url(CAST('a' AS VARBINARY)) AS VARBINARY));	                             ^	Encountered: IDENTIFIER	Expected	
-- SELECT from_base64url(CAST(to_base64url(CAST('abc' AS VARBINARY)) AS VARBINARY)); # error: errCode = 2, detailMessage = Syntax error in line 1:	...o_base64url(CAST('abc' AS VARBINARY)) AS VARBINARY));	                             ^	Encountered: IDENTIFIER	Expected	
SELECT to_hex(CAST('' AS VARBINARY));
SELECT to_hex(CAST('a' AS VARBINARY));
SELECT to_hex(CAST('abc' AS VARBINARY));
-- SELECT to_hex(CAST('hello world' AS VARBINARY)); # differ: doris : 68656c6c6f20776f726c64, presto : 68656C6C6F20776F726C64
SELECT from_hex('');
-- SELECT from_hex('61'); # differ: doris : a, presto : YQ==
-- SELECT from_hex('617a6f'); # differ: doris : azo, presto : YXpv
-- SELECT from_hex('617A6F'); # differ: doris : azo, presto : YXpv
SELECT from_hex(CAST('' AS VARBINARY));
-- SELECT from_hex(CAST('61' AS VARBINARY)); # differ: doris : a, presto : YQ==
-- SELECT from_hex(CAST('617a6F' AS VARBINARY)); # differ: doris : azo, presto : YXpv
-- SELECT to_big_endian_64(0); # error: errCode = 2, detailMessage = Can not found function 'to_big_endian_64'
-- SELECT to_big_endian_64(1); # error: errCode = 2, detailMessage = Can not found function 'to_big_endian_64'
-- SELECT to_big_endian_64(9223372036854775807); # error: errCode = 2, detailMessage = Can not found function 'to_big_endian_64'
-- SELECT to_big_endian_64(-9223372036854775807); # error: errCode = 2, detailMessage = Can not found function 'to_big_endian_64'
-- SELECT from_big_endian_64(from_hex('0000000000000000')); # error: errCode = 2, detailMessage = Can not found function 'from_hex'
-- SELECT from_big_endian_64(from_hex('0000000000000001')); # error: errCode = 2, detailMessage = Can not found function 'from_hex'
-- SELECT from_big_endian_64(from_hex('7FFFFFFFFFFFFFFF')); # error: errCode = 2, detailMessage = Can not found function 'from_hex'
-- SELECT from_big_endian_64(from_hex('8000000000000001')); # error: errCode = 2, detailMessage = Can not found function 'from_hex'
-- SELECT to_big_endian_32(0); # error: errCode = 2, detailMessage = Can not found function 'to_big_endian_32'
-- SELECT to_big_endian_32(1); # error: errCode = 2, detailMessage = Can not found function 'to_big_endian_32'
-- SELECT to_big_endian_32(2147483647); # error: errCode = 2, detailMessage = Can not found function 'to_big_endian_32'
-- SELECT to_big_endian_32(-2147483647); # error: errCode = 2, detailMessage = Can not found function 'to_big_endian_32'
-- SELECT from_big_endian_32(from_hex('00000000')); # error: errCode = 2, detailMessage = Can not found function 'from_hex'
-- SELECT from_big_endian_32(from_hex('00000001')); # error: errCode = 2, detailMessage = Can not found function 'from_hex'
-- SELECT from_big_endian_32(from_hex('7FFFFFFF')); # error: errCode = 2, detailMessage = Can not found function 'from_hex'
-- SELECT from_big_endian_32(from_hex('80000001')); # error: errCode = 2, detailMessage = Can not found function 'from_hex'
-- SELECT to_ieee754_32(CAST(0.0 AS REAL)); # error: errCode = 2, detailMessage = 	no viable alternative at input 'REAL'(line 1, pos 33)	
-- SELECT to_ieee754_32(CAST(1.0 AS REAL)); # error: errCode = 2, detailMessage = 	no viable alternative at input 'REAL'(line 1, pos 33)	
-- SELECT to_ieee754_32(CAST(3.14 AS REAL)); # error: errCode = 2, detailMessage = 	no viable alternative at input 'REAL'(line 1, pos 34)	
-- SELECT to_ieee754_32(CAST(NAN() AS REAL)); # error: errCode = 2, detailMessage = 	no viable alternative at input 'REAL'(line 1, pos 35)	
-- SELECT to_ieee754_32(CAST(INFINITY() AS REAL)); # error: errCode = 2, detailMessage = 	no viable alternative at input 'REAL'(line 1, pos 40)	
-- SELECT to_ieee754_32(CAST(-INFINITY() AS REAL)); # error: errCode = 2, detailMessage = 	no viable alternative at input 'REAL'(line 1, pos 41)	
-- SELECT to_ieee754_32(CAST(3.4028235E38 AS REAL)); # error: errCode = 2, detailMessage = 	no viable alternative at input 'REAL'(line 1, pos 42)	
-- SELECT to_ieee754_32(CAST(-3.4028235E38 AS REAL)); # error: errCode = 2, detailMessage = 	no viable alternative at input 'REAL'(line 1, pos 43)	
-- SELECT to_ieee754_32(CAST(1.4E-45 AS REAL)); # error: errCode = 2, detailMessage = 	no viable alternative at input 'REAL'(line 1, pos 37)	
-- SELECT to_ieee754_32(CAST(-1.4E-45 AS REAL)); # error: errCode = 2, detailMessage = 	no viable alternative at input 'REAL'(line 1, pos 38)	
-- SELECT from_ieee754_32(from_hex('3F800000')); # error: errCode = 2, detailMessage = Can not found function 'from_hex'
-- SELECT from_ieee754_32(to_ieee754_32(CAST(1.0 AS REAL))); # error: errCode = 2, detailMessage = 	no viable alternative at input 'REAL'(line 1, pos 49)	
-- SELECT from_ieee754_32(from_hex('4048F5C3')); # error: errCode = 2, detailMessage = Can not found function 'from_hex'
-- SELECT from_ieee754_32(to_ieee754_32(CAST(3.14 AS REAL))); # error: errCode = 2, detailMessage = 	no viable alternative at input 'REAL'(line 1, pos 50)	
-- SELECT from_ieee754_32(to_ieee754_32(CAST(NAN() AS REAL))); # error: errCode = 2, detailMessage = 	no viable alternative at input 'REAL'(line 1, pos 51)	
-- SELECT from_ieee754_32(to_ieee754_32(CAST(INFINITY() AS REAL))); # error: errCode = 2, detailMessage = 	no viable alternative at input 'REAL'(line 1, pos 56)	
-- SELECT from_ieee754_32(to_ieee754_32(CAST(-INFINITY() AS REAL))); # error: errCode = 2, detailMessage = 	no viable alternative at input 'REAL'(line 1, pos 57)	
-- SELECT from_ieee754_32(to_ieee754_32(CAST(3.4028235E38 AS REAL))); # error: errCode = 2, detailMessage = 	no viable alternative at input 'REAL'(line 1, pos 58)	
-- SELECT from_ieee754_32(to_ieee754_32(CAST(-3.4028235E38 AS REAL))); # error: errCode = 2, detailMessage = 	no viable alternative at input 'REAL'(line 1, pos 59)	
-- SELECT from_ieee754_32(to_ieee754_32(CAST(1.4E-45 AS REAL))); # error: errCode = 2, detailMessage = 	no viable alternative at input 'REAL'(line 1, pos 53)	
-- SELECT from_ieee754_32(to_ieee754_32(CAST(-1.4E-45 AS REAL))); # error: errCode = 2, detailMessage = 	no viable alternative at input 'REAL'(line 1, pos 54)	
-- SELECT to_ieee754_64(0.0); # error: errCode = 2, detailMessage = Can not found function 'to_ieee754_64'
-- SELECT to_ieee754_64(1.0); # error: errCode = 2, detailMessage = Can not found function 'to_ieee754_64'
-- SELECT to_ieee754_64(3.1415926); # error: errCode = 2, detailMessage = Can not found function 'to_ieee754_64'
-- SELECT to_ieee754_64(NAN()); # error: errCode = 2, detailMessage = Can not found function 'NAN'
-- SELECT to_ieee754_64(INFINITY()); # error: errCode = 2, detailMessage = Can not found function 'INFINITY'
-- SELECT to_ieee754_64(-INFINITY()); # error: errCode = 2, detailMessage = Can not found function 'INFINITY'
-- SELECT to_ieee754_64(1.7976931348623157E308); # error: errCode = 2, detailMessage = Can not found function 'to_ieee754_64'
-- SELECT to_ieee754_64(-1.7976931348623157E308); # error: errCode = 2, detailMessage = Can not found function 'to_ieee754_64'
-- SELECT to_ieee754_64(4.9E-324); # error: errCode = 2, detailMessage = Can not found function 'to_ieee754_64'
-- SELECT to_ieee754_64(-4.9E-324); # error: errCode = 2, detailMessage = Can not found function 'to_ieee754_64'
-- SELECT from_ieee754_64(from_hex('0000000000000000')); # error: errCode = 2, detailMessage = Can not found function 'from_hex'
-- SELECT from_ieee754_64(from_hex('3FF0000000000000')); # error: errCode = 2, detailMessage = Can not found function 'from_hex'
-- SELECT from_ieee754_64(to_ieee754_64(3.1415926)); # error: errCode = 2, detailMessage = Can not found function 'to_ieee754_64'
-- SELECT from_ieee754_64(to_ieee754_64(NAN())); # error: errCode = 2, detailMessage = Can not found function 'NAN'
-- SELECT from_ieee754_64(to_ieee754_64(INFINITY())); # error: errCode = 2, detailMessage = Can not found function 'INFINITY'
-- SELECT from_ieee754_64(to_ieee754_64(-INFINITY())); # error: errCode = 2, detailMessage = Can not found function 'INFINITY'
-- SELECT from_ieee754_64(to_ieee754_64(1.7976931348623157E308)); # error: errCode = 2, detailMessage = Can not found function 'to_ieee754_64'
-- SELECT from_ieee754_64(to_ieee754_64(-1.7976931348623157E308)); # error: errCode = 2, detailMessage = Can not found function 'to_ieee754_64'
-- SELECT from_ieee754_64(to_ieee754_64(4.9E-324)); # error: errCode = 2, detailMessage = Can not found function 'to_ieee754_64'
-- SELECT from_ieee754_64(to_ieee754_64(-4.9E-324)); # error: errCode = 2, detailMessage = Can not found function 'to_ieee754_64'
-- SELECT lpad(x'1234', 7, x'45'); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT lpad(x'1234', 7, x'45');	             ^	Encountered: STRING LITERAL	Expected: ||, COMMA, .	
-- SELECT lpad(x'1234', 7, x'4524'); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT lpad(x'1234', 7, x'4524');	             ^	Encountered: STRING LITERAL	Expected: ||, COMMA, .	
-- SELECT lpad(x'1234', 3, x'4524'); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT lpad(x'1234', 3, x'4524');	             ^	Encountered: STRING LITERAL	Expected: ||, COMMA, .	
-- SELECT lpad(x'1234', 0, x'4524'); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT lpad(x'1234', 0, x'4524');	             ^	Encountered: STRING LITERAL	Expected: ||, COMMA, .	
-- SELECT lpad(x'1234', 1, x'4524'); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT lpad(x'1234', 1, x'4524');	             ^	Encountered: STRING LITERAL	Expected: ||, COMMA, .	
-- SELECT rpad(x'1234', 7, x'45'); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT rpad(x'1234', 7, x'45');	             ^	Encountered: STRING LITERAL	Expected: ||, COMMA, .	
-- SELECT rpad(x'1234', 7, x'4524'); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT rpad(x'1234', 7, x'4524');	             ^	Encountered: STRING LITERAL	Expected: ||, COMMA, .	
-- SELECT rpad(x'1234', 3, x'4524'); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT rpad(x'1234', 3, x'4524');	             ^	Encountered: STRING LITERAL	Expected: ||, COMMA, .	
-- SELECT rpad(x'23', 0, x'4524'); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT rpad(x'23', 0, x'4524');	             ^	Encountered: STRING LITERAL	Expected: ||, COMMA, .	
-- SELECT rpad(x'1234', 1, x'4524'); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT rpad(x'1234', 1, x'4524');	             ^	Encountered: STRING LITERAL	Expected: ||, COMMA, .	
-- SELECT md5(CAST('' AS VARBINARY)); # error: errCode = 2, detailMessage = Can not found function 'MD5_DIGEST'
-- SELECT md5(CAST('hashme' AS VARBINARY)); # error: errCode = 2, detailMessage = Can not found function 'MD5_DIGEST'
-- SELECT sha1(CAST('' AS VARBINARY)); # differ: doris : da39a3ee5e6b4b0d3255bfef95601890afd80709, presto : 2jmj7l5rSw0yVb/vlWAYkK/YBwk=
-- SELECT sha1(CAST('hashme' AS VARBINARY)); # differ: doris : fb78992e561929a6967d5328f49413fa99048d06, presto : +3iZLlYZKaaWfVMo9JQT+pkEjQY=
-- SELECT sha256(CAST('' AS VARBINARY)); # differ: doris : e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855, presto : 47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=
-- SELECT sha256(CAST('hashme' AS VARBINARY)); # differ: doris : 02208b9403a87df9f4ed6b2ee2657efaa589026b4cce9accc8e8a5bf3d693c86, presto : AiCLlAOoffn07Wsu4mV++qWJAmtMzprMyOilvz1pPIY=
-- SELECT sha512(CAST('' AS VARBINARY)); # differ: doris : cf83e1357eefb8bdf1542850d66d8007d620e4050b5715dc83f4a921d36ce9ce47d0d13c5d85f2b0ff8318d2877eec2f63b931bd47417a81a538327af927da3e, presto : z4PhNX7vuL3xVChQ1m2AB9Yg5AULVxXcg/SpIdNs6c5H0NE8XYXysP+DGNKHfuwvY7kxvUdBeoGlODJ6+SfaPg==
-- SELECT sha512(CAST('hashme' AS VARBINARY)); # differ: doris : 8a4b59fb9188d09b989ff596ac9cefbf2ed91ded8dcd9498e8bf2236814a92b23be6867e7fc340880e514f8fdf97e1f147ea4b0fd6c2da3557d0cf1c0b58a204, presto : iktZ+5GI0JuYn/WWrJzvvy7ZHe2NzZSY6L8iNoFKkrI75oZ+f8NAiA5RT4/fl+HxR+pLD9bC2jVX0M8cC1iiBA==
-- SELECT murmur3(CAST('' AS VARBINARY)); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT murmur3(CAST('' AS VARBINARY));	                          ^	Encountered: IDENTIFIER	Expected	
-- SELECT murmur3(CAST('hashme' AS VARBINARY)); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT murmur3(CAST('hashme' AS VARBINARY));	                                ^	Encountered: IDENTIFIER	Expected	
-- SELECT xxhash64(CAST('' AS VARBINARY)); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT xxhash64(CAST('' AS VARBINARY));	                           ^	Encountered: IDENTIFIER	Expected	
-- SELECT xxhash64(CAST('hashme' AS VARBINARY)); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT xxhash64(CAST('hashme' AS VARBINARY));	                                 ^	Encountered: IDENTIFIER	Expected	
-- SELECT spooky_hash_v2_32(CAST('' AS VARBINARY)); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT spooky_hash_v2_32(CAST('' AS VARBINARY));	                                    ^	Encountered: IDENTIFIER	Expected	
-- SELECT spooky_hash_v2_32(CAST('hello' AS VARBINARY)); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT spooky_hash_v2_32(CAST('hello' AS VARBINARY));	                                         ^	Encountered: IDENTIFIER	Expected	
-- SELECT spooky_hash_v2_64(CAST('' AS VARBINARY)); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT spooky_hash_v2_64(CAST('' AS VARBINARY));	                                    ^	Encountered: IDENTIFIER	Expected	
-- SELECT spooky_hash_v2_64(CAST('hello' AS VARBINARY)); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT spooky_hash_v2_64(CAST('hello' AS VARBINARY));	                                         ^	Encountered: IDENTIFIER	Expected	
-- SELECT crc32(to_utf8('CRC me!')); # error: errCode = 2, detailMessage = Can not found function 'to_utf8'
-- SELECT crc32(to_utf8('1234567890')); # error: errCode = 2, detailMessage = Can not found function 'to_utf8'
-- SELECT crc32(to_utf8(CAST(1234567890 AS VARCHAR))); # error: errCode = 2, detailMessage = Can not found function 'to_utf8'
-- SELECT crc32(to_utf8('ABCDEFGHIJK')); # error: errCode = 2, detailMessage = Can not found function 'to_utf8'
-- SELECT crc32(to_utf8('ABCDEFGHIJKLM')); # error: errCode = 2, detailMessage = Can not found function 'to_utf8'
-- SELECT SUBSTR(VARBINARY 'Quadratically', 5); # differ: doris : ratically, presto : cmF0aWNhbGx5
SELECT SUBSTR(VARBINARY 'Quadratically', 50);
-- SELECT SUBSTR(VARBINARY 'Quadratically', -5); # differ: doris : cally, presto : Y2FsbHk=
SELECT SUBSTR(VARBINARY 'Quadratically', -50);
SELECT SUBSTR(VARBINARY 'Quadratically', 0);
-- SELECT SUBSTR(VARBINARY 'Quadratically', 5, 6); # differ: doris : ratica, presto : cmF0aWNh
-- SELECT SUBSTR(VARBINARY 'Quadratically', 5, 10); # differ: doris : ratically, presto : cmF0aWNhbGx5
-- SELECT SUBSTR(VARBINARY 'Quadratically', 5, 50); # differ: doris : ratically, presto : cmF0aWNhbGx5
SELECT SUBSTR(VARBINARY 'Quadratically', 50, 10);
-- SELECT SUBSTR(VARBINARY 'Quadratically', -5, 4); # differ: doris : call, presto : Y2FsbA==
-- SELECT SUBSTR(VARBINARY 'Quadratically', -5, 40); # differ: doris : cally, presto : Y2FsbHk=
SELECT SUBSTR(VARBINARY 'Quadratically', -50, 4);
SELECT SUBSTR(VARBINARY 'Quadratically', 0, 4);
SELECT SUBSTR(VARBINARY 'Quadratically', 5, 0);
-- SELECT hmac_md5(CAST('' AS VARBINARY), CAST('key' AS VARBINARY)); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT hmac_md5(CAST('' AS VARBINARY), CAST('key' AS VA...	                           ^	Encountered: IDENTIFIER	Expected	
-- SELECT hmac_md5(CAST('hashme' AS VARBINARY), CAST('key' AS VARBINARY)); # error: errCode = 2, detailMessage = Syntax error in line 1:	...hmac_md5(CAST('hashme' AS VARBINARY), CAST('key' AS VA...	                             ^	Encountered: IDENTIFIER	Expected	
-- SELECT hmac_sha1(CAST('' AS VARBINARY), CAST('key' AS VARBINARY)); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT hmac_sha1(CAST('' AS VARBINARY), CAST('key' AS VA...	                            ^	Encountered: IDENTIFIER	Expected	
-- SELECT hmac_sha1(CAST('hashme' AS VARBINARY), CAST('key' AS VARBINARY)); # error: errCode = 2, detailMessage = Syntax error in line 1:	...mac_sha1(CAST('hashme' AS VARBINARY), CAST('key' AS VA...	                             ^	Encountered: IDENTIFIER	Expected	
-- SELECT hmac_sha256(CAST('' AS VARBINARY), CAST('key' AS VARBINARY)); # error: errCode = 2, detailMessage = Syntax error in line 1:	...CT hmac_sha256(CAST('' AS VARBINARY), CAST('key' AS VA...	                             ^	Encountered: IDENTIFIER	Expected	
-- SELECT hmac_sha256(CAST('hashme' AS VARBINARY), CAST('key' AS VARBINARY)); # error: errCode = 2, detailMessage = Syntax error in line 1:	...c_sha256(CAST('hashme' AS VARBINARY), CAST('key' AS VA...	                             ^	Encountered: IDENTIFIER	Expected	
-- SELECT hmac_sha512(CAST('' AS VARBINARY), CAST('key' AS VARBINARY)); # error: errCode = 2, detailMessage = Syntax error in line 1:	...CT hmac_sha512(CAST('' AS VARBINARY), CAST('key' AS VA...	                             ^	Encountered: IDENTIFIER	Expected	
-- SELECT hmac_sha512(CAST('hashme' AS VARBINARY), CAST('key' AS VARBINARY)); # error: errCode = 2, detailMessage = Syntax error in line 1:	...c_sha512(CAST('hashme' AS VARBINARY), CAST('key' AS VA...	                             ^	Encountered: IDENTIFIER	Expected	
SELECT reverse(CAST('' AS VARBINARY));
-- SELECT reverse(CAST('hello' AS VARBINARY)); # differ: doris : olleh, presto : b2xsZWg=
-- SELECT reverse(CAST('Quadratically' AS VARBINARY)); # differ: doris : yllacitardauQ, presto : eWxsYWNpdGFyZGF1UQ==
-- SELECT reverse(CAST('racecar' AS VARBINARY)); # differ: doris : racecar, presto : cmFjZWNhcg==
set debug_skip_fold_constant=true;
SELECT length(CAST('' AS VARBINARY));
SELECT length(CAST('a' AS VARBINARY));
SELECT length(CAST('abc' AS VARBINARY));
-- SELECT CONCAT(CAST('foo' AS VARBINARY), CAST ('bar' AS VARBINARY)); # differ: doris : foobar, presto : Zm9vYmFy
-- SELECT CONCAT(CAST('foo' AS VARBINARY), CAST ('bar' AS VARBINARY), CAST ('baz' AS VARBINARY)); # differ: doris : foobarbaz, presto : Zm9vYmFyYmF6
-- SELECT CONCAT(CAST('foo' AS VARBINARY), CAST ('bar' AS VARBINARY), CAST ('bazbaz' AS VARBINARY)); # differ: doris : foobarbazbaz, presto : Zm9vYmFyYmF6YmF6
-- SELECT CONCAT(X'000102', X'AAABAC', X'FDFEFF'); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT CONCAT(X'000102', X'AAABAC', X'FDFEFF');	               ^	Encountered: STRING LITERAL	Expected: ||, COMMA, .	
-- SELECT CONCAT(X'CAFFEE', X'F7', X'DE58'); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT CONCAT(X'CAFFEE', X'F7', X'DE58');	               ^	Encountered: STRING LITERAL	Expected: ||, COMMA, .	
-- SELECT CONCAT(X'58', X'F7'); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT CONCAT(X'58', X'F7');	               ^	Encountered: STRING LITERAL	Expected: ||, COMMA, .	
-- SELECT CONCAT(X'', X'58', X'F7'); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT CONCAT(X'', X'58', X'F7');	               ^	Encountered: STRING LITERAL	Expected: ||, COMMA, .	
-- SELECT CONCAT(X'58', X'', X'F7'); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT CONCAT(X'58', X'', X'F7');	               ^	Encountered: STRING LITERAL	Expected: ||, COMMA, .	
-- SELECT CONCAT(X'58', X'F7', X''); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT CONCAT(X'58', X'F7', X'');	               ^	Encountered: STRING LITERAL	Expected: ||, COMMA, .	
-- SELECT concat(X'', X'58', X'', X'F7', X''); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT concat(X'', X'58', X'', X'F7', X'');	               ^	Encountered: STRING LITERAL	Expected: ||, COMMA, .	
-- SELECT concat(X'', X'', X'', X'', X'', X''); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT concat(X'', X'', X'', X'', X'', X'');	               ^	Encountered: STRING LITERAL	Expected: ||, COMMA, .	
SELECT to_base64(CAST('' AS VARBINARY));
SELECT to_base64(CAST('a' AS VARBINARY));
SELECT to_base64(CAST('abc' AS VARBINARY));
SELECT to_base64(CAST('hello world' AS VARBINARY));
SELECT from_base64(to_base64(CAST('' AS VARBINARY)));
-- SELECT from_base64(to_base64(CAST('a' AS VARBINARY))); # differ: doris : a, presto : YQ==
-- SELECT from_base64(to_base64(CAST('abc' AS VARBINARY))); # differ: doris : abc, presto : YWJj
SELECT from_base64(CAST(to_base64(CAST('' AS VARBINARY)) AS VARBINARY));
-- SELECT from_base64(CAST(to_base64(CAST('a' AS VARBINARY)) AS VARBINARY)); # differ: doris : a, presto : YQ==
-- SELECT from_base64(CAST(to_base64(CAST('abc' AS VARBINARY)) AS VARBINARY)); # differ: doris : abc, presto : YWJj
-- SELECT to_base64url(CAST('a' AS VARBINARY)); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT to_base64url(CAST('a' AS VARBINARY));	                                ^	Encountered: IDENTIFIER	Expected	
-- SELECT to_base64url(CAST('abc' AS VARBINARY)); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT to_base64url(CAST('abc' AS VARBINARY));	                                  ^	Encountered: IDENTIFIER	Expected	
-- SELECT to_base64url(CAST('hello world' AS VARBINARY)); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT to_base64url(CAST('hello world' AS VARBINARY));	                                          ^	Encountered: IDENTIFIER	Expected	
-- SELECT from_base64url(to_base64url(CAST('' AS VARBINARY))); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT from_base64url(to_base64url(CAST('' AS VARBINARY)));	                                              ^	Encountered: IDENTIFIER	Expected	
-- SELECT from_base64url(to_base64url(CAST('a' AS VARBINARY))); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT from_base64url(to_base64url(CAST('a' AS VARBINARY)));	                                               ^	Encountered: IDENTIFIER	Expected	
-- SELECT from_base64url(to_base64url(CAST('abc' AS VARBINARY))); # error: errCode = 2, detailMessage = Syntax error in line 1:	...o_base64url(CAST('abc' AS VARBINARY)));	                             ^	Encountered: IDENTIFIER	Expected	
-- SELECT from_base64url(CAST(to_base64url(CAST('' AS VARBINARY)) AS VARBINARY)); # error: errCode = 2, detailMessage = Syntax error in line 1:	...T(to_base64url(CAST('' AS VARBINARY)) AS VARBINARY));	                             ^	Encountered: IDENTIFIER	Expected	
-- SELECT from_base64url(CAST(to_base64url(CAST('a' AS VARBINARY)) AS VARBINARY)); # error: errCode = 2, detailMessage = Syntax error in line 1:	...(to_base64url(CAST('a' AS VARBINARY)) AS VARBINARY));	                             ^	Encountered: IDENTIFIER	Expected	
-- SELECT from_base64url(CAST(to_base64url(CAST('abc' AS VARBINARY)) AS VARBINARY)); # error: errCode = 2, detailMessage = Syntax error in line 1:	...o_base64url(CAST('abc' AS VARBINARY)) AS VARBINARY));	                             ^	Encountered: IDENTIFIER	Expected	
SELECT to_hex(CAST('' AS VARBINARY));
SELECT to_hex(CAST('a' AS VARBINARY));
SELECT to_hex(CAST('abc' AS VARBINARY));
-- SELECT to_hex(CAST('hello world' AS VARBINARY)); # differ: doris : 68656c6c6f20776f726c64, presto : 68656C6C6F20776F726C64
SELECT from_hex('');
-- SELECT from_hex('61'); # differ: doris : a, presto : YQ==
-- SELECT from_hex('617a6f'); # differ: doris : azo, presto : YXpv
-- SELECT from_hex('617A6F'); # differ: doris : azo, presto : YXpv
SELECT from_hex(CAST('' AS VARBINARY));
-- SELECT from_hex(CAST('61' AS VARBINARY)); # differ: doris : a, presto : YQ==
-- SELECT from_hex(CAST('617a6F' AS VARBINARY)); # differ: doris : azo, presto : YXpv
-- SELECT to_big_endian_64(0); # error: errCode = 2, detailMessage = Can not found function 'to_big_endian_64'
-- SELECT to_big_endian_64(1); # error: errCode = 2, detailMessage = Can not found function 'to_big_endian_64'
-- SELECT to_big_endian_64(9223372036854775807); # error: errCode = 2, detailMessage = Can not found function 'to_big_endian_64'
-- SELECT to_big_endian_64(-9223372036854775807); # error: errCode = 2, detailMessage = Can not found function 'to_big_endian_64'
-- SELECT from_big_endian_64(from_hex('0000000000000000')); # error: errCode = 2, detailMessage = Can not found function 'from_hex'
-- SELECT from_big_endian_64(from_hex('0000000000000001')); # error: errCode = 2, detailMessage = Can not found function 'from_hex'
-- SELECT from_big_endian_64(from_hex('7FFFFFFFFFFFFFFF')); # error: errCode = 2, detailMessage = Can not found function 'from_hex'
-- SELECT from_big_endian_64(from_hex('8000000000000001')); # error: errCode = 2, detailMessage = Can not found function 'from_hex'
-- SELECT to_big_endian_32(0); # error: errCode = 2, detailMessage = Can not found function 'to_big_endian_32'
-- SELECT to_big_endian_32(1); # error: errCode = 2, detailMessage = Can not found function 'to_big_endian_32'
-- SELECT to_big_endian_32(2147483647); # error: errCode = 2, detailMessage = Can not found function 'to_big_endian_32'
-- SELECT to_big_endian_32(-2147483647); # error: errCode = 2, detailMessage = Can not found function 'to_big_endian_32'
-- SELECT from_big_endian_32(from_hex('00000000')); # error: errCode = 2, detailMessage = Can not found function 'from_hex'
-- SELECT from_big_endian_32(from_hex('00000001')); # error: errCode = 2, detailMessage = Can not found function 'from_hex'
-- SELECT from_big_endian_32(from_hex('7FFFFFFF')); # error: errCode = 2, detailMessage = Can not found function 'from_hex'
-- SELECT from_big_endian_32(from_hex('80000001')); # error: errCode = 2, detailMessage = Can not found function 'from_hex'
-- SELECT to_ieee754_32(CAST(0.0 AS REAL)); # error: errCode = 2, detailMessage = 	no viable alternative at input 'REAL'(line 1, pos 33)	
-- SELECT to_ieee754_32(CAST(1.0 AS REAL)); # error: errCode = 2, detailMessage = 	no viable alternative at input 'REAL'(line 1, pos 33)	
-- SELECT to_ieee754_32(CAST(3.14 AS REAL)); # error: errCode = 2, detailMessage = 	no viable alternative at input 'REAL'(line 1, pos 34)	
-- SELECT to_ieee754_32(CAST(NAN() AS REAL)); # error: errCode = 2, detailMessage = 	no viable alternative at input 'REAL'(line 1, pos 35)	
-- SELECT to_ieee754_32(CAST(INFINITY() AS REAL)); # error: errCode = 2, detailMessage = 	no viable alternative at input 'REAL'(line 1, pos 40)	
-- SELECT to_ieee754_32(CAST(-INFINITY() AS REAL)); # error: errCode = 2, detailMessage = 	no viable alternative at input 'REAL'(line 1, pos 41)	
-- SELECT to_ieee754_32(CAST(3.4028235E38 AS REAL)); # error: errCode = 2, detailMessage = 	no viable alternative at input 'REAL'(line 1, pos 42)	
-- SELECT to_ieee754_32(CAST(-3.4028235E38 AS REAL)); # error: errCode = 2, detailMessage = 	no viable alternative at input 'REAL'(line 1, pos 43)	
-- SELECT to_ieee754_32(CAST(1.4E-45 AS REAL)); # error: errCode = 2, detailMessage = 	no viable alternative at input 'REAL'(line 1, pos 37)	
-- SELECT to_ieee754_32(CAST(-1.4E-45 AS REAL)); # error: errCode = 2, detailMessage = 	no viable alternative at input 'REAL'(line 1, pos 38)	
-- SELECT from_ieee754_32(from_hex('3F800000')); # error: errCode = 2, detailMessage = Can not found function 'from_hex'
-- SELECT from_ieee754_32(to_ieee754_32(CAST(1.0 AS REAL))); # error: errCode = 2, detailMessage = 	no viable alternative at input 'REAL'(line 1, pos 49)	
-- SELECT from_ieee754_32(from_hex('4048F5C3')); # error: errCode = 2, detailMessage = Can not found function 'from_hex'
-- SELECT from_ieee754_32(to_ieee754_32(CAST(3.14 AS REAL))); # error: errCode = 2, detailMessage = 	no viable alternative at input 'REAL'(line 1, pos 50)	
-- SELECT from_ieee754_32(to_ieee754_32(CAST(NAN() AS REAL))); # error: errCode = 2, detailMessage = 	no viable alternative at input 'REAL'(line 1, pos 51)	
-- SELECT from_ieee754_32(to_ieee754_32(CAST(INFINITY() AS REAL))); # error: errCode = 2, detailMessage = 	no viable alternative at input 'REAL'(line 1, pos 56)	
-- SELECT from_ieee754_32(to_ieee754_32(CAST(-INFINITY() AS REAL))); # error: errCode = 2, detailMessage = 	no viable alternative at input 'REAL'(line 1, pos 57)	
-- SELECT from_ieee754_32(to_ieee754_32(CAST(3.4028235E38 AS REAL))); # error: errCode = 2, detailMessage = 	no viable alternative at input 'REAL'(line 1, pos 58)	
-- SELECT from_ieee754_32(to_ieee754_32(CAST(-3.4028235E38 AS REAL))); # error: errCode = 2, detailMessage = 	no viable alternative at input 'REAL'(line 1, pos 59)	
-- SELECT from_ieee754_32(to_ieee754_32(CAST(1.4E-45 AS REAL))); # error: errCode = 2, detailMessage = 	no viable alternative at input 'REAL'(line 1, pos 53)	
-- SELECT from_ieee754_32(to_ieee754_32(CAST(-1.4E-45 AS REAL))); # error: errCode = 2, detailMessage = 	no viable alternative at input 'REAL'(line 1, pos 54)	
-- SELECT to_ieee754_64(0.0); # error: errCode = 2, detailMessage = Can not found function 'to_ieee754_64'
-- SELECT to_ieee754_64(1.0); # error: errCode = 2, detailMessage = Can not found function 'to_ieee754_64'
-- SELECT to_ieee754_64(3.1415926); # error: errCode = 2, detailMessage = Can not found function 'to_ieee754_64'
-- SELECT to_ieee754_64(NAN()); # error: errCode = 2, detailMessage = Can not found function 'NAN'
-- SELECT to_ieee754_64(INFINITY()); # error: errCode = 2, detailMessage = Can not found function 'INFINITY'
-- SELECT to_ieee754_64(-INFINITY()); # error: errCode = 2, detailMessage = Can not found function 'INFINITY'
-- SELECT to_ieee754_64(1.7976931348623157E308); # error: errCode = 2, detailMessage = Can not found function 'to_ieee754_64'
-- SELECT to_ieee754_64(-1.7976931348623157E308); # error: errCode = 2, detailMessage = Can not found function 'to_ieee754_64'
-- SELECT to_ieee754_64(4.9E-324); # error: errCode = 2, detailMessage = Can not found function 'to_ieee754_64'
-- SELECT to_ieee754_64(-4.9E-324); # error: errCode = 2, detailMessage = Can not found function 'to_ieee754_64'
-- SELECT from_ieee754_64(from_hex('0000000000000000')); # error: errCode = 2, detailMessage = Can not found function 'from_hex'
-- SELECT from_ieee754_64(from_hex('3FF0000000000000')); # error: errCode = 2, detailMessage = Can not found function 'from_hex'
-- SELECT from_ieee754_64(to_ieee754_64(3.1415926)); # error: errCode = 2, detailMessage = Can not found function 'to_ieee754_64'
-- SELECT from_ieee754_64(to_ieee754_64(NAN())); # error: errCode = 2, detailMessage = Can not found function 'NAN'
-- SELECT from_ieee754_64(to_ieee754_64(INFINITY())); # error: errCode = 2, detailMessage = Can not found function 'INFINITY'
-- SELECT from_ieee754_64(to_ieee754_64(-INFINITY())); # error: errCode = 2, detailMessage = Can not found function 'INFINITY'
-- SELECT from_ieee754_64(to_ieee754_64(1.7976931348623157E308)); # error: errCode = 2, detailMessage = Can not found function 'to_ieee754_64'
-- SELECT from_ieee754_64(to_ieee754_64(-1.7976931348623157E308)); # error: errCode = 2, detailMessage = Can not found function 'to_ieee754_64'
-- SELECT from_ieee754_64(to_ieee754_64(4.9E-324)); # error: errCode = 2, detailMessage = Can not found function 'to_ieee754_64'
-- SELECT from_ieee754_64(to_ieee754_64(-4.9E-324)); # error: errCode = 2, detailMessage = Can not found function 'to_ieee754_64'
-- SELECT lpad(x'1234', 7, x'45'); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT lpad(x'1234', 7, x'45');	             ^	Encountered: STRING LITERAL	Expected: ||, COMMA, .	
-- SELECT lpad(x'1234', 7, x'4524'); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT lpad(x'1234', 7, x'4524');	             ^	Encountered: STRING LITERAL	Expected: ||, COMMA, .	
-- SELECT lpad(x'1234', 3, x'4524'); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT lpad(x'1234', 3, x'4524');	             ^	Encountered: STRING LITERAL	Expected: ||, COMMA, .	
-- SELECT lpad(x'1234', 0, x'4524'); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT lpad(x'1234', 0, x'4524');	             ^	Encountered: STRING LITERAL	Expected: ||, COMMA, .	
-- SELECT lpad(x'1234', 1, x'4524'); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT lpad(x'1234', 1, x'4524');	             ^	Encountered: STRING LITERAL	Expected: ||, COMMA, .	
-- SELECT rpad(x'1234', 7, x'45'); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT rpad(x'1234', 7, x'45');	             ^	Encountered: STRING LITERAL	Expected: ||, COMMA, .	
-- SELECT rpad(x'1234', 7, x'4524'); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT rpad(x'1234', 7, x'4524');	             ^	Encountered: STRING LITERAL	Expected: ||, COMMA, .	
-- SELECT rpad(x'1234', 3, x'4524'); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT rpad(x'1234', 3, x'4524');	             ^	Encountered: STRING LITERAL	Expected: ||, COMMA, .	
-- SELECT rpad(x'23', 0, x'4524'); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT rpad(x'23', 0, x'4524');	             ^	Encountered: STRING LITERAL	Expected: ||, COMMA, .	
-- SELECT rpad(x'1234', 1, x'4524'); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT rpad(x'1234', 1, x'4524');	             ^	Encountered: STRING LITERAL	Expected: ||, COMMA, .	
-- SELECT md5(CAST('' AS VARBINARY)); # error: errCode = 2, detailMessage = Can not found function 'MD5_DIGEST'
-- SELECT md5(CAST('hashme' AS VARBINARY)); # error: errCode = 2, detailMessage = Can not found function 'MD5_DIGEST'
-- SELECT sha1(CAST('' AS VARBINARY)); # differ: doris : da39a3ee5e6b4b0d3255bfef95601890afd80709, presto : 2jmj7l5rSw0yVb/vlWAYkK/YBwk=
-- SELECT sha1(CAST('hashme' AS VARBINARY)); # differ: doris : fb78992e561929a6967d5328f49413fa99048d06, presto : +3iZLlYZKaaWfVMo9JQT+pkEjQY=
-- SELECT sha256(CAST('' AS VARBINARY)); # differ: doris : e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855, presto : 47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=
-- SELECT sha256(CAST('hashme' AS VARBINARY)); # differ: doris : 02208b9403a87df9f4ed6b2ee2657efaa589026b4cce9accc8e8a5bf3d693c86, presto : AiCLlAOoffn07Wsu4mV++qWJAmtMzprMyOilvz1pPIY=
-- SELECT sha512(CAST('' AS VARBINARY)); # differ: doris : cf83e1357eefb8bdf1542850d66d8007d620e4050b5715dc83f4a921d36ce9ce47d0d13c5d85f2b0ff8318d2877eec2f63b931bd47417a81a538327af927da3e, presto : z4PhNX7vuL3xVChQ1m2AB9Yg5AULVxXcg/SpIdNs6c5H0NE8XYXysP+DGNKHfuwvY7kxvUdBeoGlODJ6+SfaPg==
-- SELECT sha512(CAST('hashme' AS VARBINARY)); # differ: doris : 8a4b59fb9188d09b989ff596ac9cefbf2ed91ded8dcd9498e8bf2236814a92b23be6867e7fc340880e514f8fdf97e1f147ea4b0fd6c2da3557d0cf1c0b58a204, presto : iktZ+5GI0JuYn/WWrJzvvy7ZHe2NzZSY6L8iNoFKkrI75oZ+f8NAiA5RT4/fl+HxR+pLD9bC2jVX0M8cC1iiBA==
-- SELECT murmur3(CAST('' AS VARBINARY)); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT murmur3(CAST('' AS VARBINARY));	                          ^	Encountered: IDENTIFIER	Expected	
-- SELECT murmur3(CAST('hashme' AS VARBINARY)); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT murmur3(CAST('hashme' AS VARBINARY));	                                ^	Encountered: IDENTIFIER	Expected	
-- SELECT xxhash64(CAST('' AS VARBINARY)); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT xxhash64(CAST('' AS VARBINARY));	                           ^	Encountered: IDENTIFIER	Expected	
-- SELECT xxhash64(CAST('hashme' AS VARBINARY)); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT xxhash64(CAST('hashme' AS VARBINARY));	                                 ^	Encountered: IDENTIFIER	Expected	
-- SELECT spooky_hash_v2_32(CAST('' AS VARBINARY)); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT spooky_hash_v2_32(CAST('' AS VARBINARY));	                                    ^	Encountered: IDENTIFIER	Expected	
-- SELECT spooky_hash_v2_32(CAST('hello' AS VARBINARY)); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT spooky_hash_v2_32(CAST('hello' AS VARBINARY));	                                         ^	Encountered: IDENTIFIER	Expected	
-- SELECT spooky_hash_v2_64(CAST('' AS VARBINARY)); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT spooky_hash_v2_64(CAST('' AS VARBINARY));	                                    ^	Encountered: IDENTIFIER	Expected	
-- SELECT spooky_hash_v2_64(CAST('hello' AS VARBINARY)); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT spooky_hash_v2_64(CAST('hello' AS VARBINARY));	                                         ^	Encountered: IDENTIFIER	Expected	
-- SELECT crc32(to_utf8('CRC me!')); # error: errCode = 2, detailMessage = Can not found function 'to_utf8'
-- SELECT crc32(to_utf8('1234567890')); # error: errCode = 2, detailMessage = Can not found function 'to_utf8'
-- SELECT crc32(to_utf8(CAST(1234567890 AS VARCHAR))); # error: errCode = 2, detailMessage = Can not found function 'to_utf8'
-- SELECT crc32(to_utf8('ABCDEFGHIJK')); # error: errCode = 2, detailMessage = Can not found function 'to_utf8'
-- SELECT crc32(to_utf8('ABCDEFGHIJKLM')); # error: errCode = 2, detailMessage = Can not found function 'to_utf8'
-- SELECT SUBSTR(VARBINARY 'Quadratically', 5); # differ: doris : ratically, presto : cmF0aWNhbGx5
SELECT SUBSTR(VARBINARY 'Quadratically', 50);
-- SELECT SUBSTR(VARBINARY 'Quadratically', -5); # differ: doris : cally, presto : Y2FsbHk=
SELECT SUBSTR(VARBINARY 'Quadratically', -50);
SELECT SUBSTR(VARBINARY 'Quadratically', 0);
-- SELECT SUBSTR(VARBINARY 'Quadratically', 5, 6); # differ: doris : ratica, presto : cmF0aWNh
-- SELECT SUBSTR(VARBINARY 'Quadratically', 5, 10); # differ: doris : ratically, presto : cmF0aWNhbGx5
-- SELECT SUBSTR(VARBINARY 'Quadratically', 5, 50); # differ: doris : ratically, presto : cmF0aWNhbGx5
SELECT SUBSTR(VARBINARY 'Quadratically', 50, 10);
-- SELECT SUBSTR(VARBINARY 'Quadratically', -5, 4); # differ: doris : call, presto : Y2FsbA==
-- SELECT SUBSTR(VARBINARY 'Quadratically', -5, 40); # differ: doris : cally, presto : Y2FsbHk=
SELECT SUBSTR(VARBINARY 'Quadratically', -50, 4);
SELECT SUBSTR(VARBINARY 'Quadratically', 0, 4);
SELECT SUBSTR(VARBINARY 'Quadratically', 5, 0);
-- SELECT hmac_md5(CAST('' AS VARBINARY), CAST('key' AS VARBINARY)); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT hmac_md5(CAST('' AS VARBINARY), CAST('key' AS VA...	                           ^	Encountered: IDENTIFIER	Expected	
-- SELECT hmac_md5(CAST('hashme' AS VARBINARY), CAST('key' AS VARBINARY)); # error: errCode = 2, detailMessage = Syntax error in line 1:	...hmac_md5(CAST('hashme' AS VARBINARY), CAST('key' AS VA...	                             ^	Encountered: IDENTIFIER	Expected	
-- SELECT hmac_sha1(CAST('' AS VARBINARY), CAST('key' AS VARBINARY)); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT hmac_sha1(CAST('' AS VARBINARY), CAST('key' AS VA...	                            ^	Encountered: IDENTIFIER	Expected	
-- SELECT hmac_sha1(CAST('hashme' AS VARBINARY), CAST('key' AS VARBINARY)); # error: errCode = 2, detailMessage = Syntax error in line 1:	...mac_sha1(CAST('hashme' AS VARBINARY), CAST('key' AS VA...	                             ^	Encountered: IDENTIFIER	Expected	
-- SELECT hmac_sha256(CAST('' AS VARBINARY), CAST('key' AS VARBINARY)); # error: errCode = 2, detailMessage = Syntax error in line 1:	...CT hmac_sha256(CAST('' AS VARBINARY), CAST('key' AS VA...	                             ^	Encountered: IDENTIFIER	Expected	
-- SELECT hmac_sha256(CAST('hashme' AS VARBINARY), CAST('key' AS VARBINARY)); # error: errCode = 2, detailMessage = Syntax error in line 1:	...c_sha256(CAST('hashme' AS VARBINARY), CAST('key' AS VA...	                             ^	Encountered: IDENTIFIER	Expected	
-- SELECT hmac_sha512(CAST('' AS VARBINARY), CAST('key' AS VARBINARY)); # error: errCode = 2, detailMessage = Syntax error in line 1:	...CT hmac_sha512(CAST('' AS VARBINARY), CAST('key' AS VA...	                             ^	Encountered: IDENTIFIER	Expected	
-- SELECT hmac_sha512(CAST('hashme' AS VARBINARY), CAST('key' AS VARBINARY)); # error: errCode = 2, detailMessage = Syntax error in line 1:	...c_sha512(CAST('hashme' AS VARBINARY), CAST('key' AS VA...	                             ^	Encountered: IDENTIFIER	Expected	
SELECT reverse(CAST('' AS VARBINARY));
-- SELECT reverse(CAST('hello' AS VARBINARY)); # differ: doris : olleh, presto : b2xsZWg=
-- SELECT reverse(CAST('Quadratically' AS VARBINARY)); # differ: doris : yllacitardauQ, presto : eWxsYWNpdGFyZGF1UQ==
-- SELECT reverse(CAST('racecar' AS VARBINARY)) # differ: doris : racecar, presto : cmFjZWNhcg==
