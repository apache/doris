-- ******************************
-- * Now check the behaviour of the decimal type
-- ******************************

select * from num_data order by 1,2;

-- ******************************
-- * Addition check
-- ******************************
drop table if exists num_result;
CREATE TABLE num_result (id1 int, id2 int, result decimal(38,10)) distributed by hash(id1) properties("replication_num"="1");
INSERT INTO num_result SELECT t1.id, t2.id, t1.val + t2.val
    FROM num_data t1, num_data t2;
SELECT t1.id1, t1.id2, t1.result, t2.expected
    FROM num_result t1, num_exp_add t2
    WHERE t1.id1 = t2.id1 AND t1.id2 = t2.id2
    AND t1.result != t2.expected order by 1,2,3,4;

drop table if exists num_result;
CREATE TABLE num_result (id1 int, id2 int, result decimal(38,10)) distributed by hash(id1) properties("replication_num"="1");
INSERT INTO num_result SELECT t1.id, t2.id, round(t1.val + t2.val, 10)
    FROM num_data t1, num_data t2;
SELECT t1.id1, t1.id2, t1.result, round(t2.expected, 10) as expected
    FROM num_result t1, num_exp_add t2
    WHERE t1.id1 = t2.id1 AND t1.id2 = t2.id2
    AND t1.result != round(t2.expected, 10) order by 1,2,3,4;

-- ******************************
-- * Subtraction check
-- ******************************
DROP TABLE num_result;
CREATE TABLE num_result (id1 int, id2 int, result decimal(38,10)) distributed by hash(id1) properties("replication_num"="1");
INSERT INTO num_result SELECT t1.id, t2.id, t1.val - t2.val
    FROM num_data t1, num_data t2;
SELECT t1.id1, t1.id2, t1.result, t2.expected
    FROM num_result t1, num_exp_sub t2
    WHERE t1.id1 = t2.id1 AND t1.id2 = t2.id2
    AND t1.result != t2.expected order by 1,2,3,4;

DROP TABLE num_result;
CREATE TABLE num_result (id1 int, id2 int, result decimal(38,10)) distributed by hash(id1) properties("replication_num"="1");
INSERT INTO num_result SELECT t1.id, t2.id, round(t1.val - t2.val, 40)
    FROM num_data t1, num_data t2;
SELECT t1.id1, t1.id2, t1.result, round(t2.expected, 40)
    FROM num_result t1, num_exp_sub t2
    WHERE t1.id1 = t2.id1 AND t1.id2 = t2.id2
    AND t1.result != round(t2.expected, 40) order by 1,2,3,4;

-- ******************************
-- * Multiply check
-- ******************************
set enable_decimal256=true;
DROP TABLE num_result;
CREATE TABLE num_result (id1 int, id2 int, result decimal(76,20)) distributed by hash(id1) properties("replication_num"="1");
INSERT INTO num_result SELECT t1.id, t2.id, t1.val * t2.val
    FROM num_data t1, num_data t2;
SELECT t1.id1, t1.id2, t1.result, t2.expected
    FROM num_result t1, num_exp_mul t2
    WHERE t1.id1 = t2.id1 AND t1.id2 = t2.id2
    AND t1.result != t2.expected order by 1,2,3,4;

DROP TABLE num_result;
CREATE TABLE num_result (id1 int, id2 int, result decimal(76,20)) distributed by hash(id1) properties("replication_num"="1");
INSERT INTO num_result SELECT t1.id, t2.id, round(t1.val * t2.val, 30)
    FROM num_data t1, num_data t2;
SELECT t1.id1, t1.id2, t1.result, round(t2.expected, 30) as expected
    FROM num_result t1, num_exp_mul t2
    WHERE t1.id1 = t2.id1 AND t1.id2 = t2.id2
    AND t1.result != round(t2.expected, 30) order by 1,2,3,4;

-- ******************************
-- * Division check
-- ******************************
DROP TABLE num_result;
CREATE TABLE num_result (id1 int, id2 int, result decimal(76,20)) distributed by hash(id1) properties("replication_num"="1");
INSERT INTO num_result SELECT t1.id, t2.id, cast(t1.val as decimal(37, 16)) / t2.val
    FROM num_data t1, num_data t2
    WHERE t2.val != '0.0';
SELECT t1.id1, t1.id2, t1.result, t2.expected
    FROM num_result t1, num_exp_div t2
    WHERE t1.id1 = t2.id1 AND t1.id2 = t2.id2
    AND t1.result != t2.expected order by 1,2,3,4;

DROP TABLE num_result;
CREATE TABLE num_result (id1 int, id2 int, result decimal(76,20)) distributed by hash(id1) properties("replication_num"="1");
INSERT INTO num_result SELECT t1.id, t2.id, round(cast(t1.val as decimal(37, 16)) / t2.val, 80)
    FROM num_data t1, num_data t2
    WHERE t2.val != '0.0';
SELECT t1.id1, t1.id2, t1.result, round(t2.expected, 80) as expected
    FROM num_result t1, num_exp_div t2
    WHERE t1.id1 = t2.id1 AND t1.id2 = t2.id2
    AND t1.result != round(t2.expected, 80) order by 1,2,3,4;

-- TODO: sqrt, ln, log currently return double
-- ******************************
-- * Square root check
-- ******************************
-- DROP TABLE num_result;
-- CREATE TABLE num_result (id1 int, id2 int, result decimal(38,20)) distributed by hash(id1) properties("replication_num"="1");
-- INSERT INTO num_result SELECT id, 0, SQRT(ABS(val))
--     FROM num_data;
-- SELECT t1.id1, t1.result, t2.expected
--     FROM num_result t1, num_exp_sqrt t2
--     WHERE t1.id1 = t2.id
--     AND t1.result != t2.expected order by 1,2,3;

-- ******************************
-- * Natural logarithm check
-- ******************************
-- DROP TABLE num_result;
-- CREATE TABLE num_result (id1 int, id2 int, result decimal(38,20)) distributed by hash(id1) properties("replication_num"="1");
-- INSERT INTO num_result SELECT id, 0, LN(ABS(val))
--     FROM num_data
--     WHERE val != '0.0';
-- SELECT t1.id1, t1.result, t2.expected
--     FROM num_result t1, num_exp_ln t2
--     WHERE t1.id1 = t2.id
--     AND t1.result != t2.expected order by 1,2,3;

-- ******************************
-- * Logarithm base 10 check
-- ******************************
-- DROP TABLE num_result;
-- CREATE TABLE num_result (id1 int, id2 int, result decimal(38,20)) distributed by hash(id1) properties("replication_num"="1");
-- INSERT INTO num_result SELECT id, 0, LOG(cast('10' as decimal), ABS(val))
--     FROM num_data
--     WHERE val != '0.0';
-- SELECT t1.id1, t1.result, t2.expected
--     FROM num_result t1, num_exp_log10 t2
--     WHERE t1.id1 = t2.id
--     AND t1.result != t2.expected order by 1,2,3;

-- ******************************
-- * POWER(10, LN(value)) check
-- ******************************
-- DROP TABLE num_result;
-- CREATE TABLE num_result (id1 int, id2 int, result decimal(38,10)) distributed by hash(id1) properties("replication_num"="1");
-- INSERT INTO num_result SELECT id, 0, POWER(cast('10' as decimal), LN(ABS(round(val,200))))
--     FROM num_data
--     WHERE val != '0.0';
-- SELECT t1.id1, t1.result, t2.expected
--     FROM num_result t1, num_exp_power_10_ln t2
--     WHERE t1.id1 = t2.id
--     AND t1.result != t2.expected;

SELECT power(cast('-2' as decimal), '3');
SELECT power(cast('-2' as decimal), '-1');

-- ******************************
-- * miscellaneous checks for things that have been broken in the past...
-- ******************************
-- decimal AVG used to fail on some platforms
SELECT AVG(val) FROM num_data;
SELECT MAX(val) FROM num_data;
SELECT MIN(val) FROM num_data;
-- TODO: stddev and VARIANCE result is same as MySQL but different with Postgres
-- postgres:
-- SELECT STDDEV(val) FROM num_data;
--             stddev             
-- -------------------------------
--  27791203.28758835329805617386
-- (1 row)
-- 
-- SELECT VARIANCE(val) FROM num_data;
--                variance               
-- --------------------------------------
--  772350980172061.69659105821915863601
-- (1 row)
SELECT STDDEV(val) FROM num_data;
SELECT VARIANCE(val) FROM num_data;

-- Check for appropriate rounding and overflow
drop table if exists fract_only;
CREATE TABLE fract_only (id int, val decimal(4,4)) distributed by hash(id) properties("replication_num"="1");
INSERT INTO fract_only VALUES (1, '0.0');
INSERT INTO fract_only VALUES (2, '0.1');
-- currently doris insert 0.9999
-- INSERT INTO fract_only VALUES (3, '1.0');	-- should fail
INSERT INTO fract_only VALUES (4, '-0.9999');
INSERT INTO fract_only VALUES (5, '0.99994');
-- INSERT INTO fract_only VALUES (6, '0.99995');  -- should fail
INSERT INTO fract_only VALUES (7, '0.00001');
INSERT INTO fract_only VALUES (8, '0.00017');
-- INSERT INTO fract_only VALUES (10, 'Inf');	-- should fail
-- INSERT INTO fract_only VALUES (11, '-Inf');	-- should fail
SELECT * FROM fract_only order by 1,2;

-- Check conversion to integers
-- SELECT (-9223372036854775808.5)::int8; -- should fail
SELECT cast(-9223372036854775808.4 as bigint); -- ok
SELECT cast(9223372036854775807.4 as bigint); -- ok
-- SELECT 9223372036854775807.5::int8; -- should fail
-- SELECT (-2147483648.5)::int; -- should fail
SELECT cast(-2147483648.4 as int); -- ok
SELECT cast(2147483647.4 as int); -- ok
-- SELECT 2147483647.5::int; -- should fail
-- SELECT (-32768.5)::int2; -- should fail
SELECT cast(-32768.4 as smallint); -- ok
SELECT cast(32767.4 as smallint); -- ok
-- SELECT 32767.5::int2; -- should fail

-- Simple check that ceil(), floor(), and round() work correctly
drop table if exists ceil_floor_round;
CREATE TABLE ceil_floor_round (a decimal(38,7)) distributed by hash(a) properties("replication_num"="1");
INSERT INTO ceil_floor_round VALUES ('-5.5');
INSERT INTO ceil_floor_round VALUES ('-5.499999');
INSERT INTO ceil_floor_round VALUES ('9.5');
INSERT INTO ceil_floor_round VALUES ('9.4999999');
INSERT INTO ceil_floor_round VALUES ('0.0');
INSERT INTO ceil_floor_round VALUES ('0.0000001');
INSERT INTO ceil_floor_round VALUES ('-0.000001');
SELECT a, ceil(a), ceiling(a), floor(a), round(a) FROM ceil_floor_round order by 1,2,3,4,5;
DROP TABLE ceil_floor_round;

-- TODO: strict mode
-- DROP TABLE if exists num_input_test;
-- CREATE TABLE num_input_test (n1 decimal(38,10)) distributed by hash(n1) properties("replication_num"="1");
-- INSERT INTO num_input_test(n1) VALUES (' 123');
-- INSERT INTO num_input_test(n1) VALUES ('   3245874    ');
-- INSERT INTO num_input_test(n1) VALUES ('  -93853');
-- INSERT INTO num_input_test(n1) VALUES ('555.50');
-- INSERT INTO num_input_test(n1) VALUES ('-555.50');
-- INSERT INTO num_input_test(n1) VALUES ('NaN ');
-- INSERT INTO num_input_test(n1) VALUES ('        nan');
-- INSERT INTO num_input_test(n1) VALUES (' inf ');
-- INSERT INTO num_input_test(n1) VALUES (' +inf ');
-- INSERT INTO num_input_test(n1) VALUES (' -inf ');
-- INSERT INTO num_input_test(n1) VALUES (' Infinity ');
-- INSERT INTO num_input_test(n1) VALUES (' +inFinity ');
-- INSERT INTO num_input_test(n1) VALUES (' -INFINITY ');
-- INSERT INTO num_input_test(n1) VALUES ('12_000_000_000');
-- INSERT INTO num_input_test(n1) VALUES ('12_000.123_456');
-- INSERT INTO num_input_test(n1) VALUES ('23_000_000_000e-1_0');
-- INSERT INTO num_input_test(n1) VALUES ('.000_000_000_123e1_0');
-- INSERT INTO num_input_test(n1) VALUES ('.000_000_000_123e+1_1');
-- INSERT INTO num_input_test(n1) VALUES ('0b10001110111100111100001001010');
-- INSERT INTO num_input_test(n1) VALUES ('  -0B_1010_1011_0101_0100_1010_1001_1000_1100_1110_1011_0001_1111_0000_1010_1101_0010  ');
-- INSERT INTO num_input_test(n1) VALUES ('  +0o112402761777 ');
-- INSERT INTO num_input_test(n1) VALUES ('-0O0012_5524_5230_6334_3167_0261');
-- INSERT INTO num_input_test(n1) VALUES ('-0x0000000000000000000000000deadbeef');
-- INSERT INTO num_input_test(n1) VALUES (' 0X_30b1_F33a_6DF0_bD4E_64DF_9BdA_7D15 ');
-- 
-- -- bad inputs
-- INSERT INTO num_input_test(n1) VALUES ('     ');
-- INSERT INTO num_input_test(n1) VALUES ('   1234   %');
-- INSERT INTO num_input_test(n1) VALUES ('xyz');
-- INSERT INTO num_input_test(n1) VALUES ('- 1234');
-- INSERT INTO num_input_test(n1) VALUES ('5 . 0');
-- INSERT INTO num_input_test(n1) VALUES ('5. 0   ');
-- INSERT INTO num_input_test(n1) VALUES ('');
-- INSERT INTO num_input_test(n1) VALUES (' N aN ');
-- INSERT INTO num_input_test(n1) VALUES ('+NaN');
-- INSERT INTO num_input_test(n1) VALUES ('-NaN');
-- INSERT INTO num_input_test(n1) VALUES ('+ infinity');
-- INSERT INTO num_input_test(n1) VALUES ('_123');
-- INSERT INTO num_input_test(n1) VALUES ('123_');
-- INSERT INTO num_input_test(n1) VALUES ('12__34');
-- INSERT INTO num_input_test(n1) VALUES ('123_.456');
-- INSERT INTO num_input_test(n1) VALUES ('123._456');
-- INSERT INTO num_input_test(n1) VALUES ('1.2e_34');
-- INSERT INTO num_input_test(n1) VALUES ('1.2e34_');
-- INSERT INTO num_input_test(n1) VALUES ('1.2e3__4');
-- INSERT INTO num_input_test(n1) VALUES ('0b1112');
-- INSERT INTO num_input_test(n1) VALUES ('0c1112');
-- INSERT INTO num_input_test(n1) VALUES ('0o12345678');
-- INSERT INTO num_input_test(n1) VALUES ('0x1eg');
-- INSERT INTO num_input_test(n1) VALUES ('0x12.34');
-- INSERT INTO num_input_test(n1) VALUES ('0x__1234');
-- INSERT INTO num_input_test(n1) VALUES ('0x1234_');
-- INSERT INTO num_input_test(n1) VALUES ('0x12__34');
-- 
-- SELECT * FROM num_input_test order by 1;

--
-- Test some corner cases for division
--

select cast(999999999999999999999 as decimal(21,0))/1000000000000000000000;
select mod(999999999999999999999,1000000000000000000000);
select cast(-9999999999999999999999 as decimal(22,0))/1000000000000000000000;
select mod(-9999999999999999999999,1000000000000000000000);
-- test integer division
-- select (-9999999999999999999999/1000000000000000000000)*1000000000000000000000 + mod(-9999999999999999999999,1000000000000000000000);
select mod (70.0,70) ;
select 70.0 / 70 ;
select 12345678901234567890 % 123;
select 12345678901234567890 / 123;

--
-- Test some corner cases for square root
--

-- select sqrt(cast(1.000000000000003 as decimal(16,15)));
-- select sqrt(cast(1.000000000000004 as decimal(16,15)));
-- select sqrt(cast(96627521408608.56340355805 as decimal(25,11)));
-- select sqrt(cast(96627521408608.56340355806 as decimal(25,11)));
-- select sqrt(cast(515549506212297735.073688290367 as decimal(31, 12)));
-- select sqrt(cast(515549506212297735.073688290368 as decimal(31, 12)));
-- select sqrt(8015491789940783531003294973900306);
-- select sqrt(8015491789940783531003294973900307);