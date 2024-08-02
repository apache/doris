set sql_dialect='presto';
set enable_fallback_to_original_planner=false;
set debug_skip_fold_constant=false;
SELECT abs(TINYINT '123');
SELECT abs(TINYINT '-123');
SELECT abs(CAST(NULL AS TINYINT));
SELECT abs(SMALLINT '123');
SELECT abs(SMALLINT '-123');
SELECT abs(CAST(NULL AS SMALLINT));
SELECT abs(123);
SELECT abs(-123);
SELECT abs(CAST(NULL AS INTEGER));
-- SELECT abs(BIGINT '123'); # differ: doris : 123, presto : 123
-- SELECT abs(BIGINT '-123'); # differ: doris : 123, presto : 123
-- SELECT abs(12300000000); # differ: doris : 12300000000, presto : 12300000000
-- SELECT abs(-12300000000); # differ: doris : 12300000000, presto : 12300000000
SELECT abs(CAST(NULL AS BIGINT));
SELECT abs(123.0E0);
SELECT abs(-123.0E0);
-- SELECT abs(123.45E0); # differ: doris : 123.45, presto : 123.45
-- SELECT abs(-123.45E0); # differ: doris : 123.45, presto : 123.45
SELECT abs(CAST(NULL AS DOUBLE));
SELECT abs(REAL '-754.1985');
-- SELECT abs(DECIMAL '123.45'); # differ: doris : 123.450000000, presto : 123.45
-- SELECT abs(DECIMAL '-123.45'); # differ: doris : 123.450000000, presto : 123.45
-- SELECT abs(DECIMAL '1234567890123456.78'); # differ: doris : 1234567890123456.780000000, presto : 1234567890123456.78
-- SELECT abs(DECIMAL '-1234567890123456.78'); # differ: doris : 1234567890123456.780000000, presto : 1234567890123456.78
-- SELECT abs(DECIMAL '12345678901234560.78'); # differ: doris : 12345678901234560.780000000, presto : 12345678901234560.78
-- SELECT abs(DECIMAL '-12345678901234560.78'); # differ: doris : 12345678901234560.780000000, presto : 12345678901234560.78
SELECT abs(CAST(NULL AS DECIMAL(1,0)));
SELECT atan2(1.0E0, NULL);
SELECT atan2(NULL, 1.0E0);
SELECT ceil(TINYINT '123');
SELECT ceil(TINYINT '-123');
SELECT ceil(CAST(NULL AS TINYINT));
SELECT ceil(SMALLINT '123');
SELECT ceil(SMALLINT '-123');
SELECT ceil(CAST(NULL AS SMALLINT));
SELECT ceil(123);
SELECT ceil(-123);
SELECT ceil(CAST(NULL AS INTEGER));
SELECT ceil(BIGINT '123');
SELECT ceil(BIGINT '-123');
SELECT ceil(12300000000);
SELECT ceil(-12300000000);
SELECT ceil(CAST(NULL as BIGINT));
SELECT ceil(123.0E0);
SELECT ceil(-123.0E0);
SELECT ceil(123.45E0);
SELECT ceil(-123.45E0);
SELECT ceil(CAST(NULL as DOUBLE));
SELECT ceil(REAL '123.0');
SELECT ceil(REAL '-123.0');
SELECT ceil(REAL '123.45');
SELECT ceil(REAL '-123.45');
SELECT ceiling(12300000000);
SELECT ceiling(-12300000000);
SELECT ceiling(CAST(NULL AS BIGINT));
SELECT ceiling(123.0E0);
SELECT ceiling(-123.0E0);
SELECT ceiling(123.45E0);
SELECT ceiling(-123.45E0);
SELECT ceiling(REAL '123.0');
SELECT ceiling(REAL '-123.0');
SELECT ceiling(REAL '123.45');
SELECT ceiling(REAL '-123.45');
-- SELECT ceiling(DECIMAL '0'); # differ: doris : 0, presto : 0
-- SELECT ceiling(CAST(DECIMAL '0.00' AS DECIMAL(3,2))); # differ: doris : 0, presto : 0
-- SELECT ceiling(CAST(DECIMAL '0.00' AS DECIMAL(3,2))); # differ: doris : 0, presto : 0
-- SELECT ceiling(CAST(DECIMAL '0.01' AS DECIMAL(3,2))); # differ: doris : 1, presto : 1
-- SELECT ceiling(CAST(DECIMAL '-0.01' AS DECIMAL(3,2))); # differ: doris : 0, presto : 0
-- SELECT ceiling(CAST(DECIMAL '0.49' AS DECIMAL(3,2))); # differ: doris : 1, presto : 1
-- SELECT ceiling(CAST(DECIMAL '-0.49' AS DECIMAL(3,2))); # differ: doris : 0, presto : 0
-- SELECT ceiling(CAST(DECIMAL '0.50' AS DECIMAL(3,2))); # differ: doris : 1, presto : 1
-- SELECT ceiling(CAST(DECIMAL '-0.50' AS DECIMAL(3,2))); # differ: doris : 0, presto : 0
-- SELECT ceiling(CAST(DECIMAL '0.99' AS DECIMAL(3,2))); # differ: doris : 1, presto : 1
-- SELECT ceiling(CAST(DECIMAL '-0.99' AS DECIMAL(3,2))); # differ: doris : 0, presto : 0
-- SELECT ceiling(DECIMAL '123'); # differ: doris : 123, presto : 123
-- SELECT ceiling(DECIMAL '-123'); # differ: doris : -123, presto : -123
-- SELECT ceiling(DECIMAL '123.00'); # differ: doris : 123, presto : 123
-- SELECT ceiling(DECIMAL '-123.00'); # differ: doris : -123, presto : -123
-- SELECT ceiling(DECIMAL '123.01'); # differ: doris : 124, presto : 124
-- SELECT ceiling(DECIMAL '-123.01'); # differ: doris : -123, presto : -123
-- SELECT ceiling(DECIMAL '123.45'); # differ: doris : 124, presto : 124
-- SELECT ceiling(DECIMAL '-123.45'); # differ: doris : -123, presto : -123
-- SELECT ceiling(DECIMAL '123.49'); # differ: doris : 124, presto : 124
-- SELECT ceiling(DECIMAL '-123.49'); # differ: doris : -123, presto : -123
-- SELECT ceiling(DECIMAL '123.50'); # differ: doris : 124, presto : 124
-- SELECT ceiling(DECIMAL '-123.50'); # differ: doris : -123, presto : -123
-- SELECT ceiling(DECIMAL '123.99'); # differ: doris : 124, presto : 124
-- SELECT ceiling(DECIMAL '-123.99'); # differ: doris : -123, presto : -123
-- SELECT ceiling(DECIMAL '999.9'); # differ: doris : 1000, presto : 1000
-- SELECT ceiling(CAST(DECIMAL '0000000000000000000' AS DECIMAL(19,0))); # differ: doris : 0, presto : 0
-- SELECT ceiling(CAST(DECIMAL '000000000000000000.00' AS DECIMAL(20,2))); # differ: doris : 0, presto : 0
-- SELECT ceiling(CAST(DECIMAL '000000000000000000.01' AS DECIMAL(20,2))); # differ: doris : 1, presto : 1
-- SELECT ceiling(CAST(DECIMAL '-000000000000000000.01' AS DECIMAL(20,2))); # differ: doris : 0, presto : 0
-- SELECT ceiling(CAST(DECIMAL '000000000000000000.49' AS DECIMAL(20,2))); # differ: doris : 1, presto : 1
-- SELECT ceiling(CAST(DECIMAL '-000000000000000000.49' AS DECIMAL(20,2))); # differ: doris : 0, presto : 0
-- SELECT ceiling(CAST(DECIMAL '000000000000000000.50' AS DECIMAL(20,2))); # differ: doris : 1, presto : 1
-- SELECT ceiling(CAST(DECIMAL '-000000000000000000.50' AS DECIMAL(20,2))); # differ: doris : 0, presto : 0
-- SELECT ceiling(CAST(DECIMAL '000000000000000000.99' AS DECIMAL(20,2))); # differ: doris : 1, presto : 1
-- SELECT ceiling(CAST(DECIMAL '-000000000000000000.99' AS DECIMAL(20,2))); # differ: doris : 0, presto : 0
-- SELECT ceiling(DECIMAL '123456789012345678'); # differ: doris : 123456789012345678, presto : 123456789012345678
-- SELECT ceiling(DECIMAL '-123456789012345678'); # differ: doris : -123456789012345678, presto : -123456789012345678
-- SELECT ceiling(DECIMAL '123456789012345678.00'); # differ: doris : 123456789012345678, presto : 123456789012345678
-- SELECT ceiling(DECIMAL '-123456789012345678.00'); # differ: doris : -123456789012345678, presto : -123456789012345678
-- SELECT ceiling(DECIMAL '123456789012345678.01'); # differ: doris : 123456789012345679, presto : 123456789012345679
-- SELECT ceiling(DECIMAL '-123456789012345678.01'); # differ: doris : -123456789012345678, presto : -123456789012345678
-- SELECT ceiling(DECIMAL '123456789012345678.99'); # differ: doris : 123456789012345679, presto : 123456789012345679
-- SELECT ceiling(DECIMAL '-123456789012345678.99'); # differ: doris : -123456789012345678, presto : -123456789012345678
-- SELECT ceiling(DECIMAL '123456789012345678.49'); # differ: doris : 123456789012345679, presto : 123456789012345679
-- SELECT ceiling(DECIMAL '-123456789012345678.49'); # differ: doris : -123456789012345678, presto : -123456789012345678
-- SELECT ceiling(DECIMAL '123456789012345678.50'); # differ: doris : 123456789012345679, presto : 123456789012345679
-- SELECT ceiling(DECIMAL '-123456789012345678.50'); # differ: doris : -123456789012345678, presto : -123456789012345678
-- SELECT ceiling(DECIMAL '999999999999999999.9'); # differ: doris : 1000000000000000000, presto : 1000000000000000000
-- SELECT ceiling(DECIMAL '1234567890123456.78'); # differ: doris : 1234567890123457, presto : 1234567890123457
-- SELECT ceiling(DECIMAL '-1234567890123456.78'); # differ: doris : -1234567890123456, presto : -1234567890123456
SELECT ceiling(CAST(NULL AS DOUBLE));
SELECT ceiling(CAST(NULL AS REAL));
SELECT ceiling(CAST(NULL AS DECIMAL(1,0)));
SELECT ceiling(CAST(NULL AS DECIMAL(25,5)));
SELECT truncate(17.18E0);
SELECT truncate(-17.18E0);
SELECT truncate(17.88E0);
SELECT truncate(-17.88E0);
SELECT truncate(REAL '17.18');
SELECT truncate(REAL '-17.18');
SELECT truncate(REAL '17.88');
SELECT truncate(REAL '-17.88');
-- SELECT truncate(DECIMAL '-1234'); # differ: doris : -1234.0, presto : -1234
-- SELECT truncate(DECIMAL '1234.56'); # differ: doris : 1234.0, presto : 1234
-- SELECT truncate(DECIMAL '-1234.56'); # differ: doris : -1234.0, presto : -1234
-- SELECT truncate(DECIMAL '123456789123456.999'); # differ: doris : 123456789123457.0, presto : 123456789123456
-- SELECT truncate(DECIMAL '-123456789123456.999'); # differ: doris : -123456789123457.0, presto : -123456789123456
-- SELECT truncate(DECIMAL '1.99999999999999999999999999'); # differ: doris : 2.0, presto : 1
-- SELECT truncate(DECIMAL '-1.99999999999999999999999999'); # differ: doris : -2.0, presto : -1
-- SELECT truncate(DECIMAL '1234567890123456789012'); # differ: doris : 1.2345678901234568e+21, presto : 1234567890123456789012
-- SELECT truncate(DECIMAL '-1234567890123456789012'); # differ: doris : -1.2345678901234568e+21, presto : -1234567890123456789012
-- SELECT truncate(DECIMAL '1234567890123456789012.999'); # differ: doris : 1.2345678901234568e+21, presto : 1234567890123456789012
-- SELECT truncate(DECIMAL '-1234567890123456789012.999'); # differ: doris : -1.2345678901234568e+21, presto : -1234567890123456789012
-- SELECT truncate(DECIMAL '1234', 1); # differ: doris : 1234.0, presto : 1234
-- SELECT truncate(DECIMAL '1234', -1); # differ: doris : 1230, presto : 1230
-- SELECT truncate(DECIMAL '1234.56', 1); # differ: doris : 1234.5, presto : 1234.50
-- SELECT truncate(DECIMAL '1234.56', -1); # differ: doris : 1230, presto : 1230.00
-- SELECT truncate(DECIMAL '-1234.56', 1); # differ: doris : -1234.5, presto : -1234.50
-- SELECT truncate(DECIMAL '1239.99', 1); # differ: doris : 1239.9, presto : 1239.90
-- SELECT truncate(DECIMAL '-1239.99', 1); # differ: doris : -1239.9, presto : -1239.90
-- SELECT truncate(DECIMAL '1239.999', 2); # differ: doris : 1239.99, presto : 1239.990
-- SELECT truncate(DECIMAL '1239.999', -2); # differ: doris : 1200, presto : 1200.000
-- SELECT truncate(DECIMAL '123456789123456.999', 2); # differ: doris : 123456789123456.99, presto : 123456789123456.990
-- SELECT truncate(DECIMAL '123456789123456.999', -2); # differ: doris : 123456789123400, presto : 123456789123400.000
-- SELECT truncate(DECIMAL '1234', -4); # differ: doris : 0, presto : 0
-- SELECT truncate(DECIMAL '1234.56', -4); # differ: doris : 0, presto : 0.00
-- SELECT truncate(DECIMAL '-1234.56', -4); # differ: doris : 0, presto : 0.00
-- SELECT truncate(DECIMAL '1234.56', 3); # differ: doris : 1234.560, presto : 1234.56
-- SELECT truncate(DECIMAL '-1234.56', 3); # differ: doris : -1234.560, presto : -1234.56
-- SELECT truncate(DECIMAL '1234567890123456789012', 1); # differ: doris : 1234567890123456789012.0, presto : 1234567890123456789012
-- SELECT truncate(DECIMAL '1234567890123456789012', -1); # differ: doris : 1234567890123456789010, presto : 1234567890123456789010
-- SELECT truncate(DECIMAL '1234567890123456789012.23', 1); # differ: doris : 1234567890123456789012.2, presto : 1234567890123456789012.20
-- SELECT truncate(DECIMAL '1234567890123456789012.23', -1); # differ: doris : 1234567890123456789010, presto : 1234567890123456789010.00
-- SELECT truncate(DECIMAL '123456789012345678999.99', -1); # differ: doris : 123456789012345678990, presto : 123456789012345678990.00
-- SELECT truncate(DECIMAL '-123456789012345678999.99', -1); # differ: doris : -123456789012345678990, presto : -123456789012345678990.00
-- SELECT truncate(DECIMAL '123456789012345678999.999', 2); # differ: doris : 123456789012345678999.99, presto : 123456789012345678999.990
-- SELECT truncate(DECIMAL '123456789012345678999.999', -2); # differ: doris : 123456789012345678900, presto : 123456789012345678900.000
-- SELECT truncate(DECIMAL '123456789012345678901', -21); # differ: doris : 123456788998261831120704696330, presto : 0
-- SELECT truncate(DECIMAL '123456789012345678901.23', -21); # differ: doris : 123456788998261831120704696330, presto : 0.00
-- SELECT truncate(DECIMAL '123456789012345678901.23', 3); # differ: doris : 123456789012345678901.230, presto : 123456789012345678901.23
-- SELECT truncate(DECIMAL '-123456789012345678901.23', 3); # differ: doris : -123456789012345678901.230, presto : -123456789012345678901.23
SELECT truncate(CAST(NULL AS DOUBLE));
SELECT truncate(CAST(NULL AS DECIMAL(1,0)), -1);
SELECT truncate(CAST(NULL AS DECIMAL(1,0)));
SELECT truncate(CAST(NULL AS DECIMAL(18,5)));
SELECT truncate(CAST(NULL AS DECIMAL(25,2)));
-- SELECT truncate(NULL, NULL); # error: errCode = 2, detailMessage = class org.apache.doris.nereids.trees.expressions.literal.NullLiteral cannot be cast to class org.apache.doris.nereids.trees.expressions.literal.IntegerLikeLiteral (org.apache.doris.nereids.trees.expressions.literal.NullLiteral and org.apache.doris.nereids.trees.expressions.literal.IntegerLikeLiteral are in unnamed module of loader 'app')
SELECT e();
SELECT floor(TINYINT '123');
SELECT floor(TINYINT '-123');
SELECT floor(CAST(NULL AS TINYINT));
SELECT floor(SMALLINT '123');
SELECT floor(SMALLINT '-123');
SELECT floor(CAST(NULL AS SMALLINT));
SELECT floor(123);
SELECT floor(-123);
SELECT floor(CAST(NULL AS INTEGER));
SELECT floor(BIGINT '123');
SELECT floor(BIGINT '-123');
SELECT floor(12300000000);
SELECT floor(-12300000000);
SELECT floor(CAST(NULL as BIGINT));
SELECT floor(123.0E0);
SELECT floor(-123.0E0);
SELECT floor(123.45E0);
SELECT floor(-123.45E0);
SELECT floor(REAL '123.0');
SELECT floor(REAL '-123.0');
SELECT floor(REAL '123.45');
SELECT floor(REAL '-123.45');
-- SELECT floor(DECIMAL '0'); # differ: doris : 0, presto : 0
-- SELECT floor(CAST(DECIMAL '0.00' AS DECIMAL(3,2))); # differ: doris : 0, presto : 0
-- SELECT floor(CAST(DECIMAL '0.00' AS DECIMAL(3,2))); # differ: doris : 0, presto : 0
-- SELECT floor(CAST(DECIMAL '0.01' AS DECIMAL(3,2))); # differ: doris : 0, presto : 0
-- SELECT floor(CAST(DECIMAL '-0.01' AS DECIMAL(3,2))); # differ: doris : -1, presto : -1
-- SELECT floor(CAST(DECIMAL '0.49' AS DECIMAL(3,2))); # differ: doris : 0, presto : 0
-- SELECT floor(CAST(DECIMAL '-0.49' AS DECIMAL(3,2))); # differ: doris : -1, presto : -1
-- SELECT floor(CAST(DECIMAL '0.50' AS DECIMAL(3,2))); # differ: doris : 0, presto : 0
-- SELECT floor(CAST(DECIMAL '-0.50' AS DECIMAL(3,2))); # differ: doris : -1, presto : -1
-- SELECT floor(CAST(DECIMAL '0.99' AS DECIMAL(3,2))); # differ: doris : 0, presto : 0
-- SELECT floor(CAST(DECIMAL '-0.99' AS DECIMAL(3,2))); # differ: doris : -1, presto : -1
-- SELECT floor(DECIMAL '123'); # differ: doris : 123, presto : 123
-- SELECT floor(DECIMAL '-123'); # differ: doris : -123, presto : -123
-- SELECT floor(DECIMAL '123.00'); # differ: doris : 123, presto : 123
-- SELECT floor(DECIMAL '-123.00'); # differ: doris : -123, presto : -123
-- SELECT floor(DECIMAL '123.01'); # differ: doris : 123, presto : 123
-- SELECT floor(DECIMAL '-123.01'); # differ: doris : -124, presto : -124
-- SELECT floor(DECIMAL '123.45'); # differ: doris : 123, presto : 123
-- SELECT floor(DECIMAL '-123.45'); # differ: doris : -124, presto : -124
-- SELECT floor(DECIMAL '123.49'); # differ: doris : 123, presto : 123
-- SELECT floor(DECIMAL '-123.49'); # differ: doris : -124, presto : -124
-- SELECT floor(DECIMAL '123.50'); # differ: doris : 123, presto : 123
-- SELECT floor(DECIMAL '-123.50'); # differ: doris : -124, presto : -124
-- SELECT floor(DECIMAL '123.99'); # differ: doris : 123, presto : 123
-- SELECT floor(DECIMAL '-123.99'); # differ: doris : -124, presto : -124
-- SELECT floor(DECIMAL '-999.9'); # differ: doris : -1000, presto : -1000
-- SELECT floor(CAST(DECIMAL '0000000000000000000' AS DECIMAL(19,0))); # differ: doris : 0, presto : 0
-- SELECT floor(CAST(DECIMAL '000000000000000000.00' AS DECIMAL(20,2))); # differ: doris : 0, presto : 0
-- SELECT floor(CAST(DECIMAL '000000000000000000.01' AS DECIMAL(20,2))); # differ: doris : 0, presto : 0
-- SELECT floor(CAST(DECIMAL '-000000000000000000.01' AS DECIMAL(20,2))); # differ: doris : -1, presto : -1
-- SELECT floor(CAST(DECIMAL '000000000000000000.49' AS DECIMAL(20,2))); # differ: doris : 0, presto : 0
-- SELECT floor(CAST(DECIMAL '-000000000000000000.49' AS DECIMAL(20,2))); # differ: doris : -1, presto : -1
-- SELECT floor(CAST(DECIMAL '000000000000000000.50' AS DECIMAL(20,2))); # differ: doris : 0, presto : 0
-- SELECT floor(CAST(DECIMAL '-000000000000000000.50' AS DECIMAL(20,2))); # differ: doris : -1, presto : -1
-- SELECT floor(CAST(DECIMAL '000000000000000000.99' AS DECIMAL(20,2))); # differ: doris : 0, presto : 0
-- SELECT floor(CAST(DECIMAL '-000000000000000000.99' AS DECIMAL(20,2))); # differ: doris : -1, presto : -1
-- SELECT floor(DECIMAL '123456789012345678'); # differ: doris : 123456789012345678, presto : 123456789012345678
-- SELECT floor(DECIMAL '-123456789012345678'); # differ: doris : -123456789012345678, presto : -123456789012345678
-- SELECT floor(DECIMAL '123456789012345678.00'); # differ: doris : 123456789012345678, presto : 123456789012345678
-- SELECT floor(DECIMAL '-123456789012345678.00'); # differ: doris : -123456789012345678, presto : -123456789012345678
-- SELECT floor(DECIMAL '123456789012345678.01'); # differ: doris : 123456789012345678, presto : 123456789012345678
-- SELECT floor(DECIMAL '-123456789012345678.01'); # differ: doris : -123456789012345679, presto : -123456789012345679
-- SELECT floor(DECIMAL '123456789012345678.99'); # differ: doris : 123456789012345678, presto : 123456789012345678
-- SELECT floor(DECIMAL '-123456789012345678.49'); # differ: doris : -123456789012345679, presto : -123456789012345679
-- SELECT floor(DECIMAL '123456789012345678.49'); # differ: doris : 123456789012345678, presto : 123456789012345678
-- SELECT floor(DECIMAL '-123456789012345678.50'); # differ: doris : -123456789012345679, presto : -123456789012345679
-- SELECT floor(DECIMAL '123456789012345678.50'); # differ: doris : 123456789012345678, presto : 123456789012345678
-- SELECT floor(DECIMAL '-123456789012345678.99'); # differ: doris : -123456789012345679, presto : -123456789012345679
-- SELECT floor(DECIMAL '-999999999999999999.9'); # differ: doris : -1000000000000000000, presto : -1000000000000000000
-- SELECT floor(DECIMAL '1234567890123456.78'); # differ: doris : 1234567890123456, presto : 1234567890123456
-- SELECT floor(DECIMAL '-1234567890123456.78'); # differ: doris : -1234567890123457, presto : -1234567890123457
SELECT floor(CAST(NULL as REAL));
SELECT floor(CAST(NULL as DOUBLE));
SELECT floor(CAST(NULL as DECIMAL(1,0)));
SELECT floor(CAST(NULL as DECIMAL(25,5)));
SELECT log(5.0E0, NULL);
SELECT log(NULL, 5.0E0);
-- SELECT mod(DECIMAL '0.0', DECIMAL '2.0'); # differ: doris : 0E-9, presto : 0.0
SELECT mod(NULL, 5.0E0);
-- SELECT mod(DECIMAL '0.0', DECIMAL '2.0'); # differ: doris : 0E-9, presto : 0.0
-- SELECT mod(DECIMAL '13.0', DECIMAL '5.0'); # differ: doris : 3.000000000, presto : 3.0
-- SELECT mod(DECIMAL '-13.0', DECIMAL '5.0'); # differ: doris : -3.000000000, presto : -3.0
-- SELECT mod(DECIMAL '13.0', DECIMAL '-5.0'); # differ: doris : 3.000000000, presto : 3.0
-- SELECT mod(DECIMAL '-13.0', DECIMAL '-5.0'); # differ: doris : -3.000000000, presto : -3.0
-- SELECT mod(DECIMAL '5.0', DECIMAL '2.5'); # differ: doris : 0E-9, presto : 0.0
-- SELECT mod(DECIMAL '5.0', DECIMAL '2.05'); # differ: doris : 0.900000000, presto : 0.90
-- SELECT mod(DECIMAL '5.0', DECIMAL '2.55'); # differ: doris : 2.450000000, presto : 2.45
-- SELECT mod(DECIMAL '5.0001', DECIMAL '2.55'); # differ: doris : 2.450100000, presto : 2.4501
-- SELECT mod(DECIMAL '123456789012345670', DECIMAL '123456789012345669'); # differ: doris : 1.000000000, presto : 1
-- SELECT mod(DECIMAL '12345678901234567.90', DECIMAL '12345678901234567.89'); # differ: doris : 0.010000000, presto : 0.01
SELECT mod(DECIMAL '5.0', CAST(NULL as DECIMAL(1,0)));
SELECT mod(CAST(NULL as DECIMAL(1,0)), DECIMAL '5.0');
SELECT pi();
-- SELECT nan(); # error: errCode = 2, detailMessage = Can not found function 'nan'
-- SELECT infinity(); # error: errCode = 2, detailMessage = Can not found function 'infinity'
-- SELECT is_infinite(1.0E0 / 0.0E0); # error: errCode = 2, detailMessage = Can not found function 'is_infinite'
-- SELECT is_infinite(0.0E0 / 0.0E0); # error: errCode = 2, detailMessage = Can not found function 'is_infinite'
-- SELECT is_infinite(1.0E0 / 1.0E0); # error: errCode = 2, detailMessage = Can not found function 'is_infinite'
-- SELECT is_infinite(REAL '1.0' / REAL '0.0'); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT is_infinite(REAL '1.0' / REAL '0.0');	                   ^	Encountered: REAL	Expected: REAL is keyword, maybe `REAL`	
-- SELECT is_infinite(REAL '0.0' / REAL '0.0'); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT is_infinite(REAL '0.0' / REAL '0.0');	                   ^	Encountered: REAL	Expected: REAL is keyword, maybe `REAL`	
-- SELECT is_infinite(REAL '1.0' / REAL '1.0'); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT is_infinite(REAL '1.0' / REAL '1.0');	                   ^	Encountered: REAL	Expected: REAL is keyword, maybe `REAL`	
-- SELECT is_infinite(NULL); # error: errCode = 2, detailMessage = Can not found function 'is_infinite'
-- SELECT is_finite(100000); # error: errCode = 2, detailMessage = Can not found function 'is_finite'
-- SELECT is_finite(rand() / 0.0E0); # error: errCode = 2, detailMessage = Can not found function 'is_finite'
-- SELECT is_finite(REAL '754.2008E0'); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT is_finite(REAL '754.2008E0');	                 ^	Encountered: REAL	Expected: REAL is keyword, maybe `REAL`	
-- SELECT is_finite(rand() / REAL '0.0E0'); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT is_finite(rand() / REAL '0.0E0');	                          ^	Encountered: REAL	Expected: REAL is keyword, maybe `REAL`	
-- SELECT is_finite(NULL); # error: errCode = 2, detailMessage = Can not found function 'is_finite'
-- SELECT is_nan(0.0E0 / 0.0E0); # error: errCode = 2, detailMessage = Can not found function 'IS_NAN'
-- SELECT is_nan(0.0E0 / 1.0E0); # error: errCode = 2, detailMessage = Can not found function 'IS_NAN'
-- SELECT is_nan(infinity() / infinity()); # error: errCode = 2, detailMessage = Can not found function 'infinity'
-- SELECT is_nan(nan()); # error: errCode = 2, detailMessage = Can not found function 'nan'
-- SELECT is_nan(REAL 'NaN'); # error: errCode = 2, detailMessage = Can not found function 'IS_NAN'
-- SELECT is_nan(REAL '0.0' / REAL '0.0'); # error: errCode = 2, detailMessage = Can not found function 'IS_NAN'
-- SELECT is_nan(REAL '0.0' / 1.0E0); # error: errCode = 2, detailMessage = Can not found function 'IS_NAN'
-- SELECT is_nan(infinity() / infinity()); # error: errCode = 2, detailMessage = Can not found function 'infinity'
-- SELECT is_nan(nan()); # error: errCode = 2, detailMessage = Can not found function 'nan'
-- SELECT is_nan(NULL); # error: errCode = 2, detailMessage = Can not found function 'IS_NAN'
SELECT power(5.0E0, NULL);
SELECT power(NULL, 5.0E0);
SELECT pow(5.0E0, 2.0E0);
SELECT round(TINYINT '3');
SELECT round(TINYINT '-3');
SELECT round(CAST(NULL as TINYINT));
SELECT round(SMALLINT '3');
SELECT round(SMALLINT '-3');
SELECT round(CAST(NULL as SMALLINT));
SELECT round(3);
SELECT round(-3);
SELECT round(CAST(NULL as INTEGER));
SELECT round(BIGINT '3');
SELECT round(BIGINT '-3');
SELECT round(CAST(NULL as BIGINT));
SELECT round( 3000000000);
SELECT round(-3000000000);
SELECT round(3.0E0);
SELECT round(-3.0E0);
SELECT round(3.499E0);
SELECT round(-3.499E0);
SELECT round(3.5E0);
SELECT round(-3.5E0);
SELECT round(-3.5001E0);
SELECT round(-3.99E0);
SELECT round(REAL '3.0');
SELECT round(REAL '-3.0');
SELECT round(REAL '3.499');
SELECT round(REAL '-3.499');
SELECT round(REAL '3.5');
SELECT round(REAL '-3.5');
SELECT round(REAL '-3.5001');
SELECT round(REAL '-3.99');
SELECT round(CAST(NULL as DOUBLE));
SELECT round(TINYINT '3', TINYINT '0');
SELECT round(TINYINT '3', 0);
SELECT round(SMALLINT '3', SMALLINT '0');
SELECT round(SMALLINT '3', 0);
SELECT round(3, 0);
SELECT round(-3, 0);
SELECT round(-3, INTEGER '0');
SELECT round(BIGINT '3', 0);
SELECT round( 3000000000, 0);
SELECT round(-3000000000, 0);
SELECT round(3.0E0, 0);
SELECT round(-3.0E0, 0);
SELECT round(3.499E0, 0);
SELECT round(-3.499E0, 0);
SELECT round(3.5E0, 0);
SELECT round(-3.5E0, 0);
SELECT round(-3.5001E0, 0);
SELECT round(-3.99E0, 0);
SELECT round(0.3E0);
SELECT round(-0.3E0);
-- SELECT round(923e0, 16); # differ: doris : 923, presto : 922.3372036854776
-- SELECT round(DOUBLE '3000.1234567890123456789', 16); # differ: doris : 3000.1234567890124, presto : 922.3372036854776
-- SELECT round(DOUBLE '1.8E292', 16); # differ: doris : inf, presto : 922.3372036854776
-- SELECT round(DOUBLE '-1.8E292', 16); # differ: doris : -inf, presto : -922.3372036854776
SELECT round(TINYINT '3', TINYINT '1');
SELECT round(TINYINT '3', 1);
SELECT round(SMALLINT '3', SMALLINT '1');
SELECT round(SMALLINT '3', 1);
SELECT round(REAL '3.0', 0);
SELECT round(REAL '-3.0', 0);
SELECT round(REAL '3.499', 0);
SELECT round(REAL '-3.499', 0);
SELECT round(REAL '3.5', 0);
SELECT round(REAL '-3.5', 0);
SELECT round(REAL '-3.5001', 0);
SELECT round(REAL '-3.99', 0);
-- SELECT round(REAL '923', 16); # differ: doris : 923.0, presto : 922.3372
-- SELECT round(REAL '3000.1234567890123456789', 16); # differ: doris : 3000.1235, presto : 922.3372
-- SELECT round(REAL '3.4028235e+38', 271); # differ: doris : 3.4028235e+38, presto : 0.0
-- SELECT round(REAL '-3.4028235e+38', 271); # differ: doris : -3.4028235e+38, presto : -0.0
SELECT round(3, 1);
SELECT round(-3, 1);
SELECT round(-3, INTEGER '1');
-- SELECT round(-3, CAST(NULL as INTEGER)); # error: errCode = 2, detailMessage = class org.apache.doris.nereids.trees.expressions.literal.NullLiteral cannot be cast to class org.apache.doris.nereids.trees.expressions.literal.IntegerLikeLiteral (org.apache.doris.nereids.trees.expressions.literal.NullLiteral and org.apache.doris.nereids.trees.expressions.literal.IntegerLikeLiteral are in unnamed module of loader 'app')
SELECT round(BIGINT '3', 1);
SELECT round( 3000000000, 1);
SELECT round(-3000000000, 1);
-- SELECT round(CAST(NULL as BIGINT), CAST(NULL as INTEGER)); # error: errCode = 2, detailMessage = class org.apache.doris.nereids.trees.expressions.literal.NullLiteral cannot be cast to class org.apache.doris.nereids.trees.expressions.literal.IntegerLikeLiteral (org.apache.doris.nereids.trees.expressions.literal.NullLiteral and org.apache.doris.nereids.trees.expressions.literal.IntegerLikeLiteral are in unnamed module of loader 'app')
SELECT round(CAST(NULL as BIGINT), 1);
SELECT round(3.0E0, 1);
SELECT round(-3.0E0, 1);
SELECT round(3.499E0, 1);
SELECT round(-3.499E0, 1);
SELECT round(3.5E0, 1);
SELECT round(-3.5E0, 1);
SELECT round(-3.5001E0, 1);
SELECT round(-3.99E0, 1);
SELECT round(REAL '3.0', 1);
SELECT round(REAL '-3.0', 1);
SELECT round(REAL '3.499', 1);
SELECT round(REAL '-3.499', 1);
SELECT round(REAL '3.5', 1);
SELECT round(REAL '-3.5', 1);
SELECT round(REAL '-3.5001', 1);
SELECT round(REAL '-3.99', 1);
SELECT round(TINYINT '9', -1);
SELECT round(TINYINT '-9', -1);
SELECT round(TINYINT '5', -1);
SELECT round(TINYINT '-5', -1);
SELECT round(TINYINT '-14', -1);
SELECT round(TINYINT '12', -1);
SELECT round(TINYINT '18', -1);
SELECT round(TINYINT '18', -2);
SELECT round(TINYINT '18', -3);
SELECT round(TINYINT '127', -2);
SELECT round(TINYINT '127', -3);
SELECT round(TINYINT '-128', -2);
SELECT round(TINYINT '-128', -3);
SELECT round(SMALLINT '99', -1);
SELECT round(SMALLINT '99', -2);
SELECT round(SMALLINT '99', -3);
SELECT round(SMALLINT '-99', -1);
SELECT round(SMALLINT '-99', -2);
SELECT round(SMALLINT '-99', -3);
SELECT round(SMALLINT '32767', -4);
SELECT round(SMALLINT '32767', -5);
SELECT round(SMALLINT '-32768', -4);
SELECT round(SMALLINT '-32768', -5);
SELECT round(99, -1);
SELECT round(-99, -1);
SELECT round(99, INTEGER '-1');
SELECT round(-99, INTEGER '-1');
SELECT round(12355, -2);
SELECT round(12345, -2);
SELECT round(2147483647, -9);
SELECT round(2147483647, -10);
SELECT round( 3999999999, -1);
SELECT round(-3999999999, -1);
SELECT round(9223372036854775807, -2);
SELECT round(9223372036854775807, -17);
SELECT round(9223372036854775807, -18);
SELECT round(-9223372036854775807, -17);
SELECT round(-9223372036854775807, -18);
-- SELECT round(DECIMAL '0'); # differ: doris : 0, presto : 0
-- SELECT round(DECIMAL '0.1'); # differ: doris : 0, presto : 0
-- SELECT round(DECIMAL '-0.1'); # differ: doris : 0, presto : 0
-- SELECT round(DECIMAL '3'); # differ: doris : 3, presto : 3
-- SELECT round(DECIMAL '-3'); # differ: doris : -3, presto : -3
-- SELECT round(DECIMAL '3.0'); # differ: doris : 3, presto : 3
-- SELECT round(DECIMAL '-3.0'); # differ: doris : -3, presto : -3
-- SELECT round(DECIMAL '3.49'); # differ: doris : 3, presto : 3
-- SELECT round(DECIMAL '-3.49'); # differ: doris : -3, presto : -3
-- SELECT round(DECIMAL '3.50'); # differ: doris : 4, presto : 4
-- SELECT round(DECIMAL '-3.50'); # differ: doris : -4, presto : -4
-- SELECT round(DECIMAL '3.99'); # differ: doris : 4, presto : 4
-- SELECT round(DECIMAL '-3.99'); # differ: doris : -4, presto : -4
-- SELECT round(DECIMAL '9.99'); # differ: doris : 10, presto : 10
-- SELECT round(DECIMAL '-9.99'); # differ: doris : -10, presto : -10
-- SELECT round(DECIMAL '9999.9'); # differ: doris : 10000, presto : 10000
-- SELECT round(DECIMAL '-9999.9'); # differ: doris : -10000, presto : -10000
-- SELECT round(DECIMAL '1000000000000.9999'); # differ: doris : 1000000000001, presto : 1000000000001
-- SELECT round(DECIMAL '-1000000000000.9999'); # differ: doris : -1000000000001, presto : -1000000000001
-- SELECT round(DECIMAL '10000000000000000'); # differ: doris : 10000000000000000, presto : 10000000000000000
-- SELECT round(DECIMAL '-10000000000000000'); # differ: doris : -10000000000000000, presto : -10000000000000000
-- SELECT round(DECIMAL '9999999999999999.99'); # differ: doris : 10000000000000000, presto : 10000000000000000
-- SELECT round(DECIMAL '99999999999999999.9'); # differ: doris : 100000000000000000, presto : 100000000000000000
-- SELECT round(CAST(0 AS DECIMAL(18,0))); # differ: doris : 0, presto : 0
-- SELECT round(CAST(0 AS DECIMAL(18,1))); # differ: doris : 0, presto : 0
-- SELECT round(CAST(0 AS DECIMAL(18,2))); # differ: doris : 0, presto : 0
-- SELECT round(CAST(DECIMAL '0.1' AS DECIMAL(18,1))); # differ: doris : 0, presto : 0
-- SELECT round(CAST(DECIMAL '-0.1' AS DECIMAL(18,1))); # differ: doris : 0, presto : 0
-- SELECT round(DECIMAL '3000000000000000000000'); # differ: doris : 3000000000000000000000, presto : 3000000000000000000000
-- SELECT round(DECIMAL '-3000000000000000000000'); # differ: doris : -3000000000000000000000, presto : -3000000000000000000000
-- SELECT round(DECIMAL '3000000000000000000000.0'); # differ: doris : 3000000000000000000000, presto : 3000000000000000000000
-- SELECT round(DECIMAL '-3000000000000000000000.0'); # differ: doris : -3000000000000000000000, presto : -3000000000000000000000
-- SELECT round(DECIMAL '3000000000000000000000.49'); # differ: doris : 3000000000000000000000, presto : 3000000000000000000000
-- SELECT round(DECIMAL '-3000000000000000000000.49'); # differ: doris : -3000000000000000000000, presto : -3000000000000000000000
-- SELECT round(DECIMAL '3000000000000000000000.50'); # differ: doris : 3000000000000000000001, presto : 3000000000000000000001
-- SELECT round(DECIMAL '-3000000000000000000000.50'); # differ: doris : -3000000000000000000001, presto : -3000000000000000000001
-- SELECT round(DECIMAL '3000000000000000000000.99'); # differ: doris : 3000000000000000000001, presto : 3000000000000000000001
-- SELECT round(DECIMAL '-3000000000000000000000.99'); # differ: doris : -3000000000000000000001, presto : -3000000000000000000001
-- SELECT round(DECIMAL '9999999999999999999999.99'); # differ: doris : 10000000000000000000000, presto : 10000000000000000000000
-- SELECT round(DECIMAL '-9999999999999999999999.99'); # differ: doris : -10000000000000000000000, presto : -10000000000000000000000
-- SELECT round(DECIMAL '1000000000000000000000000000000000.9999'); # differ: doris : 100000000000000000000000000000, presto : 1000000000000000000000000000000001
-- SELECT round(DECIMAL '-1000000000000000000000000000000000.9999'); # differ: doris : -100000000000000000000000000000, presto : -1000000000000000000000000000000001
-- SELECT round(DECIMAL '10000000000000000000000000000000000000'); # differ: doris : 100000000000000000000000000000, presto : 10000000000000000000000000000000000000
-- SELECT round(DECIMAL '-10000000000000000000000000000000000000'); # differ: doris : -100000000000000000000000000000, presto : -10000000000000000000000000000000000000
-- SELECT round(DECIMAL '3000000000000000.000000'); # differ: doris : 3000000000000000, presto : 3000000000000000
-- SELECT round(DECIMAL '-3000000000000000.000000'); # differ: doris : -3000000000000000, presto : -3000000000000000
-- SELECT round(DECIMAL '3000000000000000.499999'); # differ: doris : 3000000000000000, presto : 3000000000000000
-- SELECT round(DECIMAL '-3000000000000000.499999'); # differ: doris : -3000000000000000, presto : -3000000000000000
-- SELECT round(DECIMAL '3000000000000000.500000'); # differ: doris : 3000000000000001, presto : 3000000000000001
-- SELECT round(DECIMAL '-3000000000000000.500000'); # differ: doris : -3000000000000001, presto : -3000000000000001
-- SELECT round(DECIMAL '3000000000000000.999999'); # differ: doris : 3000000000000001, presto : 3000000000000001
-- SELECT round(DECIMAL '-3000000000000000.999999'); # differ: doris : -3000000000000001, presto : -3000000000000001
-- SELECT round(DECIMAL '9999999999999999.999999'); # differ: doris : 10000000000000000, presto : 10000000000000000
-- SELECT round(DECIMAL '-9999999999999999.999999'); # differ: doris : -10000000000000000, presto : -10000000000000000
-- SELECT round(DECIMAL '3', 1); # differ: doris : 3.0, presto : 3
-- SELECT round(DECIMAL '-3', 1); # differ: doris : -3.0, presto : -3
-- SELECT round(DECIMAL '3.0', 1); # differ: doris : 3.0, presto : 3.0
-- SELECT round(DECIMAL '-3.0', 1); # differ: doris : -3.0, presto : -3.0
-- SELECT round(DECIMAL '3.449', 1); # differ: doris : 3.4, presto : 3.400
-- SELECT round(DECIMAL '-3.449', 1); # differ: doris : -3.4, presto : -3.400
-- SELECT round(DECIMAL '3.450', 1); # differ: doris : 3.5, presto : 3.500
-- SELECT round(DECIMAL '-3.450', 1); # differ: doris : -3.5, presto : -3.500
-- SELECT round(DECIMAL '3.99', 1); # differ: doris : 4.0, presto : 4.00
-- SELECT round(DECIMAL '-3.99', 1); # differ: doris : -4.0, presto : -4.00
-- SELECT round(DECIMAL '9.99', 1); # differ: doris : 10.0, presto : 10.00
-- SELECT round(DECIMAL '-9.99', 1); # differ: doris : -10.0, presto : -10.00
-- SELECT round(DECIMAL '0.3', 0); # differ: doris : 0, presto : 0.0
-- SELECT round(DECIMAL '0.7', 0); # differ: doris : 1, presto : 0.0
-- SELECT round(DECIMAL '1.7', 0); # differ: doris : 2, presto : 2.0
-- SELECT round(DECIMAL '-0.3', 0); # differ: doris : 0, presto : 0.0
-- SELECT round(DECIMAL '-0.7', 0); # differ: doris : -1, presto : 0.0
-- SELECT round(DECIMAL '-1.7', 0); # differ: doris : -2, presto : -2.0
-- SELECT round(DECIMAL '0.7', -1); # differ: doris : 0, presto : 0.0
-- SELECT round(DECIMAL '1.7', -1); # differ: doris : 0, presto : 0.0
-- SELECT round(DECIMAL '7.1', -1); # differ: doris : 10, presto : 0.0
-- SELECT round(DECIMAL '0.3', -1); # differ: doris : 0, presto : 0.0
-- SELECT round(DECIMAL '33.3', -2); # differ: doris : 0, presto : 0.0
-- SELECT round(CAST(DECIMAL '0.7' AS decimal(20, 1)), -19); # differ: doris : 0, presto : 0.0
-- SELECT round(DECIMAL '0.00', 1); # differ: doris : 0.0, presto : 0.00
-- SELECT round(DECIMAL '1234', 7); # differ: doris : 1234.0000000, presto : 1234
-- SELECT round(DECIMAL '-1234', 7); # differ: doris : -1234.0000000, presto : -1234
-- SELECT round(DECIMAL '1234', -7); # differ: doris : 0, presto : 0
-- SELECT round(DECIMAL '-1234', -7); # differ: doris : 0, presto : 0
-- SELECT round(DECIMAL '1234.5678', 7); # differ: doris : 1234.5678000, presto : 1234.5678
-- SELECT round(DECIMAL '-1234.5678', 7); # differ: doris : -1234.5678000, presto : -1234.5678
-- SELECT round(DECIMAL '1234.5678', -2); # differ: doris : 1200, presto : 1200.0000
-- SELECT round(DECIMAL '-1234.5678', -2); # differ: doris : -1200, presto : -1200.0000
-- SELECT round(DECIMAL '1254.5678', -2); # differ: doris : 1300, presto : 1300.0000
-- SELECT round(DECIMAL '-1254.5678', -2); # differ: doris : -1300, presto : -1300.0000
-- SELECT round(DECIMAL '1234.5678', -7); # differ: doris : 0, presto : 0.0000
-- SELECT round(DECIMAL '-1234.5678', -7); # differ: doris : 0, presto : 0.0000
-- SELECT round(DECIMAL '99', -1); # differ: doris : 100, presto : 100
-- SELECT round(DECIMAL '1234567890123456789', 1); # differ: doris : 1234567890123456789.0, presto : 1234567890123456789
-- SELECT round(DECIMAL '-1234567890123456789', 1); # differ: doris : -1234567890123456789.0, presto : -1234567890123456789
-- SELECT round(DECIMAL '123456789012345678.0', 1); # differ: doris : 123456789012345678.0, presto : 123456789012345678.0
-- SELECT round(DECIMAL '-123456789012345678.0', 1); # differ: doris : -123456789012345678.0, presto : -123456789012345678.0
-- SELECT round(DECIMAL '123456789012345678.449', 1); # differ: doris : 123456789012345678.4, presto : 123456789012345678.400
-- SELECT round(DECIMAL '-123456789012345678.449', 1); # differ: doris : -123456789012345678.4, presto : -123456789012345678.400
-- SELECT round(DECIMAL '123456789012345678.45', 1); # differ: doris : 123456789012345678.5, presto : 123456789012345678.50
-- SELECT round(DECIMAL '-123456789012345678.45', 1); # differ: doris : -123456789012345678.5, presto : -123456789012345678.50
-- SELECT round(DECIMAL '123456789012345678.501', 1); # differ: doris : 123456789012345678.5, presto : 123456789012345678.500
-- SELECT round(DECIMAL '-123456789012345678.501', 1); # differ: doris : -123456789012345678.5, presto : -123456789012345678.500
-- SELECT round(DECIMAL '999999999999999999.99', 1); # differ: doris : 1000000000000000000.0, presto : 1000000000000000000.00
-- SELECT round(DECIMAL '-999999999999999999.99', 1); # differ: doris : -1000000000000000000.0, presto : -1000000000000000000.00
-- SELECT round(DECIMAL '1234567890123456789', 7); # differ: doris : 1234567890123456789.0000000, presto : 1234567890123456789
-- SELECT round(DECIMAL '-1234567890123456789', 7); # differ: doris : -1234567890123456789.0000000, presto : -1234567890123456789
-- SELECT round(DECIMAL '123456789012345678.99', 7); # differ: doris : 123456789012345678.9900000, presto : 123456789012345678.99
-- SELECT round(DECIMAL '-123456789012345678.99', 7); # differ: doris : -123456789012345678.9900000, presto : -123456789012345678.99
-- SELECT round(DECIMAL '123456789012345611.99', -2); # differ: doris : 123456789012345600, presto : 123456789012345600.00
-- SELECT round(DECIMAL '-123456789012345611.99', -2); # differ: doris : -123456789012345600, presto : -123456789012345600.00
-- SELECT round(DECIMAL '123456789012345678.99', -2); # differ: doris : 123456789012345700, presto : 123456789012345700.00
-- SELECT round(DECIMAL '-123456789012345678.99', -2); # differ: doris : -123456789012345700, presto : -123456789012345700.00
-- SELECT round(DECIMAL '123456789012345678.99', -30); # differ: doris : 123456790068172987395858690, presto : 0.00
-- SELECT round(DECIMAL '-123456789012345678.99', -30); # differ: doris : -123456790068172987395858690, presto : 0.00
-- SELECT round(DECIMAL '9999999999999999999999999999999999999.9', 1); # differ: doris : 100000000000000000000000000000.0, presto : 9999999999999999999999999999999999999.9
-- SELECT round(DECIMAL  '1329123201320737513', -3); # differ: doris : 1329123201320738000, presto : 1329123201320738000
-- SELECT round(DECIMAL '-1329123201320737513', -3); # differ: doris : -1329123201320738000, presto : -1329123201320738000
-- SELECT round(DECIMAL  '1329123201320739513', -3); # differ: doris : 1329123201320740000, presto : 1329123201320740000
-- SELECT round(DECIMAL '-1329123201320739513', -3); # differ: doris : -1329123201320740000, presto : -1329123201320740000
-- SELECT round(DECIMAL  '9999999999999999999', -3); # differ: doris : 10000000000000000000, presto : 10000000000000000000
-- SELECT round(DECIMAL '-9999999999999999999', -3); # differ: doris : -10000000000000000000, presto : -10000000000000000000
-- SELECT round(DECIMAL '9999999999999999.99', 1); # differ: doris : 10000000000000000.0, presto : 10000000000000000.00
-- SELECT round(DECIMAL '-9999999999999999.99', 1); # differ: doris : -10000000000000000.0, presto : -10000000000000000.00
-- SELECT round(DECIMAL '9999999999999999.99', -1); # differ: doris : 10000000000000000, presto : 10000000000000000.00
-- SELECT round(DECIMAL '-9999999999999999.99', -1); # differ: doris : -10000000000000000, presto : -10000000000000000.00
-- SELECT round(DECIMAL '9999999999999999.99', 2); # differ: doris : 9999999999999999.99, presto : 9999999999999999.99
-- SELECT round(DECIMAL '-9999999999999999.99', 2); # differ: doris : -9999999999999999.99, presto : -9999999999999999.99
-- SELECT round(DECIMAL '329123201320737513', -3); # differ: doris : 329123201320738000, presto : 329123201320738000
-- SELECT round(DECIMAL '-329123201320737513', -3); # differ: doris : -329123201320738000, presto : -329123201320738000
-- SELECT round(DECIMAL '329123201320739513', -3); # differ: doris : 329123201320740000, presto : 329123201320740000
-- SELECT round(DECIMAL '-329123201320739513', -3); # differ: doris : -329123201320740000, presto : -329123201320740000
-- SELECT round(DECIMAL '999999999999999999', -3); # differ: doris : 1000000000000000000, presto : 1000000000000000000
-- SELECT round(DECIMAL '-999999999999999999', -3); # differ: doris : -1000000000000000000, presto : -1000000000000000000
SELECT round(CAST(NULL as DOUBLE), CAST(NULL as INTEGER));
-- SELECT round(-3.0E0, CAST(NULL as INTEGER)); # error: errCode = 2, detailMessage = class org.apache.doris.nereids.trees.expressions.literal.NullLiteral cannot be cast to class org.apache.doris.nereids.trees.expressions.literal.IntegerLikeLiteral (org.apache.doris.nereids.trees.expressions.literal.NullLiteral and org.apache.doris.nereids.trees.expressions.literal.IntegerLikeLiteral are in unnamed module of loader 'app')
SELECT round(CAST(NULL as DOUBLE), 1);
-- SELECT round(CAST(NULL as DECIMAL(1,0)), CAST(NULL as INTEGER)); # error: errCode = 2, detailMessage = class org.apache.doris.nereids.trees.expressions.literal.NullLiteral cannot be cast to class org.apache.doris.nereids.trees.expressions.literal.IntegerLikeLiteral (org.apache.doris.nereids.trees.expressions.literal.NullLiteral and org.apache.doris.nereids.trees.expressions.literal.IntegerLikeLiteral are in unnamed module of loader 'app')
-- SELECT round(DECIMAL '-3.0', CAST(NULL as INTEGER)); # error: errCode = 2, detailMessage = class org.apache.doris.nereids.trees.expressions.literal.NullLiteral cannot be cast to class org.apache.doris.nereids.trees.expressions.literal.IntegerLikeLiteral (org.apache.doris.nereids.trees.expressions.literal.NullLiteral and org.apache.doris.nereids.trees.expressions.literal.IntegerLikeLiteral are in unnamed module of loader 'app')
SELECT round(CAST(NULL as DECIMAL(1,0)), 1);
SELECT round(CAST(NULL as DECIMAL(17,2)), 1);
SELECT round(CAST(NULL as DECIMAL(20,2)), 1);
-- SELECT round(nan(), 2); # error: errCode = 2, detailMessage = Can not found function 'nan'
-- SELECT round(1.0E0 / 0, 2); # differ: doris : None, presto : Infinity
-- SELECT round(-1.0E0 / 0, 2); # differ: doris : None, presto : -Infinity
SELECT sign(CAST(NULL as TINYINT));
SELECT sign(CAST(NULL as SMALLINT));
SELECT sign(CAST(NULL as INTEGER));
SELECT sign(CAST(NULL as BIGINT));
SELECT sign(CAST(NULL as DOUBLE));
SELECT sign(CAST(NULL as DECIMAL(2,1)));
SELECT sign(CAST(NULL as DECIMAL(38,0)));
SELECT sign(DOUBLE '+Infinity');
SELECT sign(DOUBLE '-Infinity');
-- SELECT sign(DECIMAL '0'); # differ: doris : 0, presto : 0
-- SELECT sign(DECIMAL '123'); # differ: doris : 1, presto : 1
-- SELECT sign(DECIMAL '-123'); # differ: doris : -1, presto : -1
-- SELECT sign(DECIMAL '123.000000000000000'); # differ: doris : 1, presto : 1
-- SELECT sign(DECIMAL '-123.000000000000000'); # differ: doris : -1, presto : -1
-- SELECT sign(DECIMAL '0.000000000000000000'); # differ: doris : 0, presto : 0
-- SELECT sign(DECIMAL '1230.000000000000000'); # differ: doris : 1, presto : 1
-- SELECT sign(DECIMAL '-1230.000000000000000'); # differ: doris : -1, presto : -1
SELECT greatest(TINYINT '1', TINYINT '2');
SELECT greatest(TINYINT '-1', TINYINT '-2');
SELECT greatest(TINYINT '5', TINYINT '4', TINYINT '3', TINYINT '2', TINYINT '1', TINYINT '2', TINYINT '3', TINYINT '4', TINYINT '1', TINYINT '5');
SELECT greatest(TINYINT '-1');
SELECT greatest(TINYINT '5', TINYINT '4', CAST(NULL AS TINYINT), TINYINT '3');
SELECT greatest(SMALLINT '1', SMALLINT '2');
SELECT greatest(SMALLINT '-1', SMALLINT '-2');
SELECT greatest(SMALLINT '5', SMALLINT '4', SMALLINT '3', SMALLINT '2', SMALLINT '1', SMALLINT '2', SMALLINT '3', SMALLINT '4', SMALLINT '1', SMALLINT '5');
SELECT greatest(SMALLINT '-1');
SELECT greatest(SMALLINT '5', SMALLINT '4', CAST(NULL AS SMALLINT), SMALLINT '3');
SELECT greatest(1, 2);
SELECT greatest(-1, -2);
SELECT greatest(5, 4, 3, 2, 1, 2, 3, 4, 1, 5);
SELECT greatest(-1);
SELECT greatest(5, 4, CAST(NULL AS INTEGER), 3);
SELECT greatest(10000000000, 20000000000);
SELECT greatest(-10000000000, -20000000000);
SELECT greatest(5000000000, 4, 3, 2, 1000000000, 2, 3, 4, 1, 5000000000);
SELECT greatest(-10000000000);
SELECT greatest(5000000000, 4000000000, CAST(NULL as BIGINT), 3000000000);
-- SELECT greatest(1.5E0, 2.3E0); # differ: doris : 2.3, presto : 2.3
SELECT greatest(-1.5E0, -2.3E0);
-- SELECT greatest(-1.5E0, -2.3E0, -5/3); # differ: doris : -1.500000000000000, presto : -1.0
-- SELECT greatest(1.5E0, -infinity(), infinity()); # error: errCode = 2, detailMessage = Can not found function 'infinity'
SELECT greatest(5, 4, CAST(NULL as DOUBLE), 3);
-- SELECT greatest(NaN(), 5, 4, 3); # error: errCode = 2, detailMessage = Can not found function 'NaN'
-- SELECT greatest(5, 4, NaN(), 3); # error: errCode = 2, detailMessage = Can not found function 'NaN'
-- SELECT greatest(5, 4, 3, NaN()); # error: errCode = 2, detailMessage = Can not found function 'NaN'
-- SELECT greatest(NaN()); # error: errCode = 2, detailMessage = Can not found function 'NaN'
-- SELECT greatest(NaN(), NaN(), NaN()); # error: errCode = 2, detailMessage = Can not found function 'NaN'
SELECT greatest(REAL '1.5', REAL '2.3');
SELECT greatest(REAL '-1.5', REAL '-2.3');
-- SELECT greatest(REAL '-1.5', REAL '-2.3', CAST(-5/3 AS REAL)); # differ: doris : -1.5, presto : -1.0
-- SELECT greatest(REAL '1.5', CAST(infinity() AS REAL)); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT greatest(REAL '1.5', CAST(infinity() AS REAL));	                ^	Encountered: REAL	Expected: REAL is keyword, maybe `REAL`	
SELECT greatest(REAL '5', REAL '4', CAST(NULL as REAL), REAL '3');
-- SELECT greatest(CAST(NaN() as REAL), REAL '5', REAL '4', REAL '3'); # error: errCode = 2, detailMessage = Syntax error in line 1:	...test(CAST(NaN() as REAL), REAL '5', REAL '4', REAL '3');	                             ^	Encountered: REAL	Expected: REAL is keyword, maybe `REAL`	
-- SELECT greatest(REAL '5', REAL '4', CAST(NaN() as REAL), REAL '3'); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT greatest(REAL '5', REAL '4', CAST(NaN...	                ^	Encountered: REAL	Expected: REAL is keyword, maybe `REAL`	
-- SELECT greatest(REAL '5', REAL '4', REAL '3', CAST(NaN() as REAL)); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT greatest(REAL '5', REAL '4', REAL '3'...	                ^	Encountered: REAL	Expected: REAL is keyword, maybe `REAL`	
-- SELECT greatest(CAST(NaN() as REAL)); # error: errCode = 2, detailMessage = 	no viable alternative at input 'REAL'(line 1, pos 30)	
-- SELECT greatest(CAST(NaN() as REAL), CAST(NaN() as REAL), CAST(NaN() as REAL)); # error: errCode = 2, detailMessage = 	no viable alternative at input 'REAL'(line 1, pos 30)	
-- SELECT greatest(1.0, 2.0); # differ: doris : 2.0, presto : 2.0
-- SELECT greatest(1.0, -2.0); # differ: doris : 1.0, presto : 1.0
-- SELECT greatest(1.0, 1.1, 1.2, 1.3); # differ: doris : 1.3, presto : 1.3
SELECT greatest(1, 20000000000);
SELECT greatest(1, BIGINT '2');
SELECT greatest(1.0E0, 2);
SELECT greatest(1, 2.0E0);
SELECT greatest(1.0E0, 2);
SELECT greatest(5.0E0, 4, CAST(NULL as DOUBLE), 3);
SELECT greatest(5.0E0, 4, CAST(NULL as BIGINT), 3);
SELECT greatest(1.0, 2.0E0);
-- SELECT greatest(5, 4, 3.0, 2); # differ: doris : 5.0, presto : 5.0
SELECT greatest(1E0);
-- SELECT greatest(rand()); # differ: doris : 0.9714789880992111, presto : 0.500741220883488
-- SELECT greatest(ROW(1.5E0), ROW(2.3E0)); # error: errCode = 2, detailMessage = can not cast from origin type STRUCT<StructField ( name=col1, dataType=DECIMALV3(2, 1), nullable=true )> to target type=VARCHAR(65533)
-- SELECT greatest(ROW(-1.5E0), ROW(-2.3E0)); # error: errCode = 2, detailMessage = can not cast from origin type STRUCT<StructField ( name=col1, dataType=DECIMALV3(2, 1), nullable=true )> to target type=VARCHAR(65533)
-- SELECT greatest(ROW(-1.5E0), ROW(-2.3E0), ROW(-5/3)); # error: errCode = 2, detailMessage = can not cast from origin type STRUCT<StructField ( name=col1, dataType=DECIMALV3(2, 1), nullable=true )> to target type=VARCHAR(65533)
-- SELECT greatest(ROW(1.5E0), ROW(-infinity()), ROW(infinity())); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT greatest(ROW(1.5E0), ROW(-infinity())...	                ^	Encountered: ROW	Expected: ROW is keyword, maybe `ROW`	
-- SELECT greatest(ROW(5), ROW(4), CAST(NULL as ROW(DOUBLE)), ROW(3)); # error: errCode = 2, detailMessage = Syntax error in line 1:	...ST(NULL AS STRUCT<`DOUBLE`>), STRUCT(3))	                             ^	Encountered: >	Expected	
-- SELECT greatest(ROW(NaN()), ROW(5), ROW(4), ROW(3)); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT greatest(ROW(NaN()), ROW(5), ROW(4), ROW(3));	                ^	Encountered: ROW	Expected: ROW is keyword, maybe `ROW`	
-- SELECT greatest(ROW(5), ROW(4), ROW(NaN()), ROW(3)); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT greatest(ROW(5), ROW(4), ROW(NaN()), ROW(3));	                ^	Encountered: ROW	Expected: ROW is keyword, maybe `ROW`	
-- SELECT greatest(ROW(5), ROW(4), ROW(3), ROW(NaN())); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT greatest(ROW(5), ROW(4), ROW(3), ROW(NaN()));	                ^	Encountered: ROW	Expected: ROW is keyword, maybe `ROW`	
-- SELECT greatest(ROW(NaN())); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT greatest(ROW(NaN()));	                ^	Encountered: ROW	Expected: ROW is keyword, maybe `ROW`	
-- SELECT greatest(ROW(NaN()), ROW(NaN()), ROW(NaN())); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT greatest(ROW(NaN()), ROW(NaN()), ROW(NaN()));	                ^	Encountered: ROW	Expected: ROW is keyword, maybe `ROW`	
-- SELECT greatest(ROW(REAL '1.5'), ROW(REAL '2.3')); # error: errCode = 2, detailMessage = can not cast from origin type STRUCT<StructField ( name=col1, dataType=FLOAT, nullable=true )> to target type=VARCHAR(65533)
-- SELECT greatest(ROW(REAL '-1.5'), ROW(REAL '-2.3')); # error: errCode = 2, detailMessage = can not cast from origin type STRUCT<StructField ( name=col1, dataType=FLOAT, nullable=true )> to target type=VARCHAR(65533)
-- SELECT greatest(ROW(REAL '-1.5'), ROW(REAL '-2.3'), ROW(CAST(-5/3 AS REAL))); # error: errCode = 2, detailMessage = can not cast from origin type STRUCT<StructField ( name=col1, dataType=FLOAT, nullable=true )> to target type=VARCHAR(65533)
-- SELECT greatest(ROW(REAL '1.5'), ROW(CAST(infinity() AS REAL))); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT greatest(ROW(REAL '1.5'), ROW(CAST(in...	                ^	Encountered: ROW	Expected: ROW is keyword, maybe `ROW`	
-- SELECT greatest(ROW(REAL '5'), ROW(REAL '4'), CAST(NULL as ROW(REAL)), ROW(REAL '3')); # error: errCode = 2, detailMessage = Syntax error in line 1:	...CAST(NULL AS STRUCT<`REAL`>), STRUCT(CAST('3' AS FLOAT)))	                             ^	Encountered: >	Expected	
-- SELECT greatest(ROW(CAST(NaN() as REAL)), ROW(REAL '5'), ROW(REAL '4'), ROW(REAL '3')); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT greatest(ROW(CAST(NaN() as REAL)), RO...	                ^	Encountered: ROW	Expected: ROW is keyword, maybe `ROW`	
-- SELECT greatest(ROW(REAL '5'), ROW(REAL '4'), ROW(CAST(NaN() as REAL)), ROW(REAL '3')); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT greatest(ROW(REAL '5'), ROW(REAL '4')...	                ^	Encountered: ROW	Expected: ROW is keyword, maybe `ROW`	
-- SELECT greatest(ROW(REAL '5'), ROW(REAL '4'), ROW(REAL '3'), ROW(CAST(NaN() as REAL))); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT greatest(ROW(REAL '5'), ROW(REAL '4')...	                ^	Encountered: ROW	Expected: ROW is keyword, maybe `ROW`	
-- SELECT greatest(ROW(CAST(NaN() as REAL))); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT greatest(ROW(CAST(NaN() as REAL)));	                ^	Encountered: ROW	Expected: ROW is keyword, maybe `ROW`	
-- SELECT greatest(ROW(CAST(NaN() as REAL)), ROW(CAST(NaN() as REAL)), ROW(CAST(NaN() as REAL))); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT greatest(ROW(CAST(NaN() as REAL)), RO...	                ^	Encountered: ROW	Expected: ROW is keyword, maybe `ROW`	
-- SELECT greatest(ARRAY[1.5E0], ARRAY[2.3E0]); # error: errCode = 2, detailMessage = can not cast from origin type ARRAY<DECIMALV3(2, 1)> to target type=VARCHAR(65533)
-- SELECT greatest(ARRAY[-1.5E0], ARRAY[-2.3E0]); # error: errCode = 2, detailMessage = can not cast from origin type ARRAY<DECIMALV3(2, 1)> to target type=VARCHAR(65533)
-- SELECT greatest(ARRAY[-1.5E0], ARRAY[-2.3E0], ARRAY[-5/3]); # error: errCode = 2, detailMessage = can not cast from origin type ARRAY<DECIMALV3(2, 1)> to target type=VARCHAR(65533)
-- SELECT greatest(ARRAY[1.5E0], ARRAY[-infinity()], ARRAY[infinity()]); # error: errCode = 2, detailMessage = Unknown column 'ARRAY' in 'table list' in PROJECT clause
-- SELECT greatest(ARRAY[5], ARRAY[4], CAST(NULL as ARRAY(DOUBLE)), ARRAY[3]); # error: errCode = 2, detailMessage = can not cast from origin type ARRAY<TINYINT> to target type=VARCHAR(65533)
-- SELECT greatest(ARRAY[NaN()], ARRAY[5], ARRAY[4], ARRAY[3]); # error: errCode = 2, detailMessage = Unknown column 'ARRAY' in 'table list' in PROJECT clause
-- SELECT greatest(ARRAY[5], ARRAY[4], ARRAY[NaN()], ARRAY[3]); # error: errCode = 2, detailMessage = Unknown column 'ARRAY' in 'table list' in PROJECT clause
-- SELECT greatest(ARRAY[5], ARRAY[4], ARRAY[3], ARRAY[NaN()]); # error: errCode = 2, detailMessage = Unknown column 'ARRAY' in 'table list' in PROJECT clause
-- SELECT greatest(ARRAY[NaN()]); # error: errCode = 2, detailMessage = Unknown column 'ARRAY' in 'table list' in PROJECT clause
-- SELECT greatest(ARRAY[NaN()], ARRAY[NaN()], ARRAY[NaN()]); # error: errCode = 2, detailMessage = Unknown column 'ARRAY' in 'table list' in PROJECT clause
-- SELECT greatest(ARRAY[REAL '1.5'], ARRAY[REAL '2.3']); # error: errCode = 2, detailMessage = can not cast from origin type ARRAY<FLOAT> to target type=VARCHAR(65533)
-- SELECT greatest(ARRAY[REAL '-1.5'], ARRAY[REAL '-2.3']); # error: errCode = 2, detailMessage = can not cast from origin type ARRAY<FLOAT> to target type=VARCHAR(65533)
-- SELECT greatest(ARRAY[REAL '-1.5'], ARRAY[REAL '-2.3'], ARRAY[CAST(-5/3 AS REAL)]); # error: errCode = 2, detailMessage = can not cast from origin type ARRAY<FLOAT> to target type=VARCHAR(65533)
-- SELECT greatest(ARRAY[REAL '1.5'], ARRAY[CAST(infinity() AS REAL)]); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT greatest(ARRAY[REAL '1.5'], ARRAY[CAST(infi...	                      ^	Encountered: REAL	Expected: REAL is keyword, maybe `REAL`	
-- SELECT greatest(ARRAY[REAL '5'], ARRAY[REAL '4'], CAST(NULL as ARRAY(REAL)), ARRAY[REAL '3']); # error: errCode = 2, detailMessage = can not cast from origin type ARRAY<FLOAT> to target type=VARCHAR(65533)
-- SELECT greatest(ARRAY[CAST(NaN() as REAL)], ARRAY[REAL '5'], ARRAY[REAL '4'], ARRAY[REAL '3']); # error: errCode = 2, detailMessage = Syntax error in line 1:	...ST(NaN() as REAL)], ARRAY[REAL '5'], ARRAY[REAL '4'], ...	                             ^	Encountered: REAL	Expected: REAL is keyword, maybe `REAL`	
-- SELECT greatest(ARRAY[REAL '5'], ARRAY[REAL '4'], ARRAY[CAST(NaN() as REAL)], ARRAY[REAL '3']); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT greatest(ARRAY[REAL '5'], ARRAY[REAL '4'], ...	                      ^	Encountered: REAL	Expected: REAL is keyword, maybe `REAL`	
-- SELECT greatest(ARRAY[REAL '5'], ARRAY[REAL '4'], ARRAY[REAL '3'], ARRAY[CAST(NaN() as REAL)]); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT greatest(ARRAY[REAL '5'], ARRAY[REAL '4'], ...	                      ^	Encountered: REAL	Expected: REAL is keyword, maybe `REAL`	
-- SELECT greatest(ARRAY[CAST(NaN() as REAL)]); # error: errCode = 2, detailMessage = 	no viable alternative at input '[CAST(NaN() as REAL'(line 1, pos 36)	
-- SELECT greatest(ARRAY[CAST(NaN() as REAL)], ARRAY[CAST(NaN() as REAL)], ARRAY[CAST(NaN() as REAL)]); # error: errCode = 2, detailMessage = 	no viable alternative at input '[CAST(NaN() as REAL'(line 1, pos 36)	
SELECT least(TINYINT '1', TINYINT '2');
SELECT least(TINYINT '-1', TINYINT '-2');
SELECT least(TINYINT '5', TINYINT '4', TINYINT '3', TINYINT '2', TINYINT '1', TINYINT '2', TINYINT '3', TINYINT '4', TINYINT '1', TINYINT '5');
SELECT least(TINYINT '-1');
SELECT least(TINYINT '5', TINYINT '4', CAST(NULL AS TINYINT), TINYINT '3');
SELECT least(SMALLINT '1', SMALLINT '2');
SELECT least(SMALLINT '-1', SMALLINT '-2');
SELECT least(SMALLINT '5', SMALLINT '4', SMALLINT '3', SMALLINT '2', SMALLINT '1', SMALLINT '2', SMALLINT '3', SMALLINT '4', SMALLINT '1', SMALLINT '5');
SELECT least(SMALLINT '-1');
SELECT least(SMALLINT '5', SMALLINT '4', CAST(NULL AS SMALLINT), SMALLINT '3');
SELECT least(1, 2);
SELECT least(-1, -2);
SELECT least(5, 4, 3, 2, 1, 2, 3, 4, 1, 5);
SELECT least(-1);
SELECT least(5, 4, CAST(NULL AS INTEGER), 3);
SELECT least(10000000000, 20000000000);
SELECT least(-10000000000, -20000000000);
SELECT least(50000000000, 40000000000, 30000000000, 20000000000, 50000000000);
SELECT least(-10000000000);
SELECT least(500000000, 400000000, CAST(NULL as BIGINT), 300000000);
SELECT least(1.5E0, 2.3E0);
-- SELECT least(-1.5E0, -2.3E0); # differ: doris : -2.3, presto : -2.3
-- SELECT least(-1.5E0, -2.3E0, -5/3); # differ: doris : -2.300000000000000, presto : -2.3
-- SELECT least(1.5E0, -infinity(), infinity()); # error: errCode = 2, detailMessage = Can not found function 'infinity'
SELECT least(5, 4, CAST(NULL as DOUBLE), 3);
-- SELECT least(NaN(), 5, 4, 3); # error: errCode = 2, detailMessage = Can not found function 'NaN'
-- SELECT least(5, 4, NaN(), 3); # error: errCode = 2, detailMessage = Can not found function 'NaN'
-- SELECT least(5, 4, 3, NaN()); # error: errCode = 2, detailMessage = Can not found function 'NaN'
-- SELECT least(NaN()); # error: errCode = 2, detailMessage = Can not found function 'NaN'
-- SELECT least(NaN(), NaN(), NaN()); # error: errCode = 2, detailMessage = Can not found function 'NaN'
SELECT least(REAL '1.5', REAL '2.3');
SELECT least(REAL '-1.5', REAL '-2.3');
SELECT least(REAL '-1.5', REAL '-2.3', CAST(-5/3 AS REAL));
-- SELECT least(REAL '1.5', CAST(-infinity() AS REAL)); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT least(REAL '1.5', CAST(-infinity() AS REAL));	             ^	Encountered: REAL	Expected: REAL is keyword, maybe `REAL`	
SELECT least(REAL '5', REAL '4', CAST(NULL as REAL), REAL '3');
-- SELECT least(CAST(NaN() as REAL), REAL '5', REAL '4', REAL '3'); # error: errCode = 2, detailMessage = Syntax error in line 1:	...east(CAST(NaN() as REAL), REAL '5', REAL '4', REAL '3');	                             ^	Encountered: REAL	Expected: REAL is keyword, maybe `REAL`	
-- SELECT least(REAL '5', REAL '4', CAST(NaN() as REAL), REAL '3'); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT least(REAL '5', REAL '4', CAST(NaN...	             ^	Encountered: REAL	Expected: REAL is keyword, maybe `REAL`	
-- SELECT least(REAL '5', REAL '4', REAL '3', CAST(NaN() as REAL)); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT least(REAL '5', REAL '4', REAL '3'...	             ^	Encountered: REAL	Expected: REAL is keyword, maybe `REAL`	
-- SELECT least(CAST(NaN() as REAL)); # error: errCode = 2, detailMessage = 	no viable alternative at input 'REAL'(line 1, pos 27)	
-- SELECT least(CAST(NaN() as REAL), CAST(NaN() as REAL), CAST(NaN() as REAL)); # error: errCode = 2, detailMessage = 	no viable alternative at input 'REAL'(line 1, pos 27)	
-- SELECT least(1.0, 2.0); # differ: doris : 1.0, presto : 1.0
-- SELECT least(1.0, -2.0); # differ: doris : -2.0, presto : -2.0
-- SELECT least(1.0, 1.1, 1.2, 1.3); # differ: doris : 1.0, presto : 1.0
SELECT least(1, 20000000000);
SELECT least(1, BIGINT '2');
SELECT least(1.0E0, 2);
SELECT least(1, 2.0E0);
SELECT least(1.0E0, 2);
SELECT least(5.0E0, 4, CAST(NULL as DOUBLE), 3);
SELECT least(5.0E0, 4, CAST(NULL as BIGINT), 3);
SELECT least(1.0, 2.0E0);
-- SELECT least(5, 4, 3.0, 2); # differ: doris : 2.0, presto : 2.0
-- SELECT least(ROW(1.5E0), ROW(2.3E0)); # error: errCode = 2, detailMessage = can not cast from origin type STRUCT<StructField ( name=col1, dataType=DECIMALV3(2, 1), nullable=true )> to target type=VARCHAR(65533)
-- SELECT least(ROW(-1.5E0), ROW(-2.3E0)); # error: errCode = 2, detailMessage = can not cast from origin type STRUCT<StructField ( name=col1, dataType=DECIMALV3(2, 1), nullable=true )> to target type=VARCHAR(65533)
-- SELECT least(ROW(-1.5E0), ROW(-2.3E0), ROW(-5/3)); # error: errCode = 2, detailMessage = can not cast from origin type STRUCT<StructField ( name=col1, dataType=DECIMALV3(2, 1), nullable=true )> to target type=VARCHAR(65533)
-- SELECT least(ROW(1.5E0), ROW(-infinity()), ROW(infinity())); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT least(ROW(1.5E0), ROW(-infinity()), ROW(infinity()));	             ^	Encountered: ROW	Expected: ROW is keyword, maybe `ROW`	
-- SELECT least(ROW(5), ROW(4), CAST(NULL as ROW(DOUBLE)), ROW(3)); # error: errCode = 2, detailMessage = Syntax error in line 1:	...ST(NULL AS STRUCT<`DOUBLE`>), STRUCT(3))	                             ^	Encountered: >	Expected	
-- SELECT least(ROW(NaN()), ROW(5), ROW(4), ROW(3)); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT least(ROW(NaN()), ROW(5), ROW(4), ROW(3));	             ^	Encountered: ROW	Expected: ROW is keyword, maybe `ROW`	
-- SELECT least(ROW(5), ROW(4), ROW(NaN()), ROW(3)); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT least(ROW(5), ROW(4), ROW(NaN()), ROW(3));	             ^	Encountered: ROW	Expected: ROW is keyword, maybe `ROW`	
-- SELECT least(ROW(5), ROW(4), ROW(3), ROW(NaN())); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT least(ROW(5), ROW(4), ROW(3), ROW(NaN()));	             ^	Encountered: ROW	Expected: ROW is keyword, maybe `ROW`	
-- SELECT least(ROW(NaN())); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT least(ROW(NaN()));	             ^	Encountered: ROW	Expected: ROW is keyword, maybe `ROW`	
-- SELECT least(ROW(NaN()), ROW(NaN()), ROW(NaN())); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT least(ROW(NaN()), ROW(NaN()), ROW(NaN()));	             ^	Encountered: ROW	Expected: ROW is keyword, maybe `ROW`	
-- SELECT least(ROW(REAL '1.5'), ROW(REAL '2.3')); # error: errCode = 2, detailMessage = can not cast from origin type STRUCT<StructField ( name=col1, dataType=FLOAT, nullable=true )> to target type=VARCHAR(65533)
-- SELECT least(ROW(REAL '-1.5'), ROW(REAL '-2.3')); # error: errCode = 2, detailMessage = can not cast from origin type STRUCT<StructField ( name=col1, dataType=FLOAT, nullable=true )> to target type=VARCHAR(65533)
-- SELECT least(ROW(REAL '-1.5'), ROW(REAL '-2.3'), ROW(CAST(-5/3 AS REAL))); # error: errCode = 2, detailMessage = can not cast from origin type STRUCT<StructField ( name=col1, dataType=FLOAT, nullable=true )> to target type=VARCHAR(65533)
-- SELECT least(ROW(REAL '1.5'), ROW(CAST(-infinity() AS REAL)), ROW(CAST(infinity() AS REAL))); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT least(ROW(REAL '1.5'), ROW(CAST(-i...	             ^	Encountered: ROW	Expected: ROW is keyword, maybe `ROW`	
-- SELECT least(ROW(REAL '5'), ROW(REAL '4'), CAST(NULL as ROW(REAL)), ROW(REAL '3')); # error: errCode = 2, detailMessage = Syntax error in line 1:	...CAST(NULL AS STRUCT<`REAL`>), STRUCT(CAST('3' AS FLOAT)))	                             ^	Encountered: >	Expected	
-- SELECT least(ROW(CAST(NaN() as REAL)), ROW(REAL '5'), ROW(REAL '4'), ROW(REAL '3')); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT least(ROW(CAST(NaN() as REAL)), RO...	             ^	Encountered: ROW	Expected: ROW is keyword, maybe `ROW`	
-- SELECT least(ROW(REAL '5'), ROW(REAL '4'), ROW(CAST(NaN() as REAL)), ROW(REAL '3')); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT least(ROW(REAL '5'), ROW(REAL '4')...	             ^	Encountered: ROW	Expected: ROW is keyword, maybe `ROW`	
-- SELECT least(ROW(REAL '5'), ROW(REAL '4'), ROW(REAL '3'), ROW(CAST(NaN() as REAL))); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT least(ROW(REAL '5'), ROW(REAL '4')...	             ^	Encountered: ROW	Expected: ROW is keyword, maybe `ROW`	
-- SELECT least(ROW(CAST(NaN() as REAL))); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT least(ROW(CAST(NaN() as REAL)));	             ^	Encountered: ROW	Expected: ROW is keyword, maybe `ROW`	
-- SELECT least(ROW(CAST(NaN() as REAL)), ROW(CAST(NaN() as REAL)), ROW(CAST(NaN() as REAL))); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT least(ROW(CAST(NaN() as REAL)), RO...	             ^	Encountered: ROW	Expected: ROW is keyword, maybe `ROW`	
-- SELECT least(ARRAY[1.5E0], ARRAY[2.3E0]); # error: errCode = 2, detailMessage = can not cast from origin type ARRAY<DECIMALV3(2, 1)> to target type=VARCHAR(65533)
-- SELECT least(ARRAY[-1.5E0], ARRAY[-2.3E0]); # error: errCode = 2, detailMessage = can not cast from origin type ARRAY<DECIMALV3(2, 1)> to target type=VARCHAR(65533)
-- SELECT least(ARRAY[-1.5E0], ARRAY[-2.3E0], ARRAY[-5/3]); # error: errCode = 2, detailMessage = can not cast from origin type ARRAY<DECIMALV3(2, 1)> to target type=VARCHAR(65533)
-- SELECT least(ARRAY[1.5E0], ARRAY[-infinity()], ARRAY[infinity()]); # error: errCode = 2, detailMessage = Unknown column 'ARRAY' in 'table list' in PROJECT clause
-- SELECT least(ARRAY[5], ARRAY[4], CAST(NULL as ARRAY(DOUBLE)), ARRAY[3]); # error: errCode = 2, detailMessage = can not cast from origin type ARRAY<TINYINT> to target type=VARCHAR(65533)
-- SELECT least(ARRAY[NaN()], ARRAY[5], ARRAY[4], ARRAY[3]); # error: errCode = 2, detailMessage = Unknown column 'ARRAY' in 'table list' in PROJECT clause
-- SELECT least(ARRAY[5], ARRAY[4], ARRAY[NaN()], ARRAY[3]); # error: errCode = 2, detailMessage = Unknown column 'ARRAY' in 'table list' in PROJECT clause
-- SELECT least(ARRAY[5], ARRAY[4], ARRAY[3], ARRAY[NaN()]); # error: errCode = 2, detailMessage = Unknown column 'ARRAY' in 'table list' in PROJECT clause
-- SELECT least(ARRAY[NaN()]); # error: errCode = 2, detailMessage = Unknown column 'ARRAY' in 'table list' in PROJECT clause
-- SELECT least(ARRAY[NaN()], ARRAY[NaN()], ARRAY[NaN()]); # error: errCode = 2, detailMessage = Unknown column 'ARRAY' in 'table list' in PROJECT clause
-- SELECT least(ARRAY[REAL '1.5'], ARRAY[REAL '2.3']); # error: errCode = 2, detailMessage = can not cast from origin type ARRAY<FLOAT> to target type=VARCHAR(65533)
-- SELECT least(ARRAY[REAL '-1.5'], ARRAY[REAL '-2.3']); # error: errCode = 2, detailMessage = can not cast from origin type ARRAY<FLOAT> to target type=VARCHAR(65533)
-- SELECT least(ARRAY[REAL '-1.5'], ARRAY[REAL '-2.3'], ARRAY[CAST(-5/3 AS REAL)]); # error: errCode = 2, detailMessage = can not cast from origin type ARRAY<FLOAT> to target type=VARCHAR(65533)
-- SELECT least(ARRAY[REAL '1.5'], ARRAY[CAST(-infinity() AS REAL)], ARRAY[CAST(infinity() AS REAL)]); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT least(ARRAY[REAL '1.5'], ARRAY[CAST(-inf...	                   ^	Encountered: REAL	Expected: REAL is keyword, maybe `REAL`	
-- SELECT least(ARRAY[REAL '5'], ARRAY[REAL '4'], CAST(NULL as ARRAY(REAL)), ARRAY[REAL '3']); # error: errCode = 2, detailMessage = can not cast from origin type ARRAY<FLOAT> to target type=VARCHAR(65533)
-- SELECT least(ARRAY[CAST(NaN() as REAL)], ARRAY[REAL '5'], ARRAY[REAL '4'], ARRAY[REAL '3']); # error: errCode = 2, detailMessage = Syntax error in line 1:	...ST(NaN() as REAL)], ARRAY[REAL '5'], ARRAY[REAL '4'], ...	                             ^	Encountered: REAL	Expected: REAL is keyword, maybe `REAL`	
-- SELECT least(ARRAY[REAL '5'], ARRAY[REAL '4'], ARRAY[CAST(NaN() as REAL)], ARRAY[REAL '3']); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT least(ARRAY[REAL '5'], ARRAY[REAL '4'], ...	                   ^	Encountered: REAL	Expected: REAL is keyword, maybe `REAL`	
-- SELECT least(ARRAY[REAL '5'], ARRAY[REAL '4'], ARRAY[REAL '3'], ARRAY[CAST(NaN() as REAL)]); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT least(ARRAY[REAL '5'], ARRAY[REAL '4'], ...	                   ^	Encountered: REAL	Expected: REAL is keyword, maybe `REAL`	
-- SELECT least(ARRAY[CAST(NaN() as REAL)]); # error: errCode = 2, detailMessage = 	no viable alternative at input '[CAST(NaN() as REAL'(line 1, pos 33)	
-- SELECT least(ARRAY[CAST(NaN() as REAL)], ARRAY[CAST(NaN() as REAL)], ARRAY[CAST(NaN() as REAL)]); # error: errCode = 2, detailMessage = 	no viable alternative at input '[CAST(NaN() as REAL'(line 1, pos 33)	
-- SELECT to_base(2147483648, 16); # error: errCode = 2, detailMessage = Can not found function 'to_base'
-- SELECT to_base(255, 2); # error: errCode = 2, detailMessage = Can not found function 'to_base'
-- SELECT to_base(-2147483647, 16); # error: errCode = 2, detailMessage = Can not found function 'to_base'
-- SELECT to_base(NULL, 16); # error: errCode = 2, detailMessage = Can not found function 'to_base'
-- SELECT to_base(-2147483647, NULL); # error: errCode = 2, detailMessage = Can not found function 'to_base'
-- SELECT to_base(NULL, NULL); # error: errCode = 2, detailMessage = Can not found function 'to_base'
-- SELECT from_base('80000000', 16); # error: errCode = 2, detailMessage = Can not found function 'FROM_BASE'
-- SELECT from_base('11111111', 2); # error: errCode = 2, detailMessage = Can not found function 'FROM_BASE'
-- SELECT from_base('-7fffffff', 16); # error: errCode = 2, detailMessage = Can not found function 'FROM_BASE'
-- SELECT from_base('9223372036854775807', 10); # error: errCode = 2, detailMessage = Can not found function 'FROM_BASE'
-- SELECT from_base('-9223372036854775808', 10); # error: errCode = 2, detailMessage = Can not found function 'FROM_BASE'
-- SELECT from_base(NULL, 10); # error: errCode = 2, detailMessage = Can not found function 'FROM_BASE'
-- SELECT from_base('-9223372036854775808', NULL); # error: errCode = 2, detailMessage = Can not found function 'FROM_BASE'
-- SELECT from_base(NULL, NULL); # error: errCode = 2, detailMessage = Can not found function 'FROM_BASE'
SELECT width_bucket(3.14E0, 0, 4, 3);
SELECT width_bucket(2, 0, 4, 3);
-- SELECT width_bucket(infinity(), 0, 4, 3); # error: errCode = 2, detailMessage = Can not found function 'infinity'
SELECT width_bucket(-1, 0, 3.2E0, 4);
-- SELECT width_bucket(3.14E0, 4, 0, 3); # differ: doris : 0, presto : 1
-- SELECT width_bucket(2, 4, 0, 3); # differ: doris : 0, presto : 2
-- SELECT width_bucket(infinity(), 4, 0, 3); # error: errCode = 2, detailMessage = Can not found function 'infinity'
-- SELECT width_bucket(-1, 3.2E0, 0, 4); # differ: doris : 0, presto : 5
-- SELECT width_bucket(3.14E0, array[0.0E0, 2.0E0, 4.0E0]); # error: errCode = 2, detailMessage = Can not found function 'WIDTH_BUCKET' which has 2 arity. Candidate functions are: [WIDTH_BUCKETorg.apache.doris.nereids.trees.expressions.functions.BuiltinFunctionBuilder@3fa5c17d]
-- SELECT width_bucket(infinity(), array[0.0E0, 2.0E0, 4.0E0]); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT width_bucket(infinity(), array[0.0E0, 2.0E0, 4.0E0]);	                                           ^	Encountered: COMMA	Expected: ||	
-- SELECT width_bucket(-1, array[0.0E0, 1.2E0, 3.3E0, 4.5E0]); # error: errCode = 2, detailMessage = Can not found function 'WIDTH_BUCKET' which has 2 arity. Candidate functions are: [WIDTH_BUCKETorg.apache.doris.nereids.trees.expressions.functions.BuiltinFunctionBuilder@3fa5c17d]
-- SELECT width_bucket(3.145E0, array[0.0E0]); # error: errCode = 2, detailMessage = Can not found function 'WIDTH_BUCKET' which has 2 arity. Candidate functions are: [WIDTH_BUCKETorg.apache.doris.nereids.trees.expressions.functions.BuiltinFunctionBuilder@3fa5c17d]
-- SELECT width_bucket(-3.145E0, array[0.0E0]); # error: errCode = 2, detailMessage = Can not found function 'WIDTH_BUCKET' which has 2 arity. Candidate functions are: [WIDTH_BUCKETorg.apache.doris.nereids.trees.expressions.functions.BuiltinFunctionBuilder@3fa5c17d]
-- SELECT width_bucket(1.5E0, array[1.0E0, 2.3E0, 2.0E0]); # error: errCode = 2, detailMessage = Can not found function 'WIDTH_BUCKET' which has 2 arity. Candidate functions are: [WIDTH_BUCKETorg.apache.doris.nereids.trees.expressions.functions.BuiltinFunctionBuilder@3fa5c17d]
-- SELECT cosine_similarity(map(ARRAY['a', 'b'], ARRAY[1.0E0, 2.0E0]), map(ARRAY['c', 'b'], ARRAY[1.0E0, 3.0E0])); # error: errCode = 2, detailMessage = Syntax error in line 1:	...e_similarity(map(ARRAY['a', 'b'], ARRAY[1.0E0, 2.0E0])...	                             ^	Encountered: COMMA	Expected: ||	
-- SELECT cosine_similarity(map(ARRAY['a', 'b', 'c'], ARRAY[1.0E0, 2.0E0, -1.0E0]), map(ARRAY['c', 'b'], ARRAY[1.0E0, 3.0E0])); # error: errCode = 2, detailMessage = Syntax error in line 1:	...e_similarity(map(ARRAY['a', 'b', 'c'], ARRAY[1.0E0, 2....	                             ^	Encountered: COMMA	Expected: ||	
-- SELECT cosine_similarity(map(ARRAY['a', 'b', 'c'], ARRAY[1.0E0, 2.0E0, -1.0E0]), map(ARRAY['d', 'e'], ARRAY[1.0E0, 3.0E0])); # error: errCode = 2, detailMessage = Syntax error in line 1:	...e_similarity(map(ARRAY['a', 'b', 'c'], ARRAY[1.0E0, 2....	                             ^	Encountered: COMMA	Expected: ||	
-- SELECT cosine_similarity(null, map(ARRAY['c', 'b'], ARRAY[1.0E0, 3.0E0])); # error: errCode = 2, detailMessage = Syntax error in line 1:	...larity(null, map(ARRAY['c', 'b'], ARRAY[1.0E0, 3.0E0]));	                             ^	Encountered: COMMA	Expected: ||	
-- SELECT cosine_similarity(map(ARRAY['a', 'b'], ARRAY[1.0E0, null]), map(ARRAY['c', 'b'], ARRAY[1.0E0, 3.0E0])); # error: errCode = 2, detailMessage = Syntax error in line 1:	...e_similarity(map(ARRAY['a', 'b'], ARRAY[1.0E0, null]),...	                             ^	Encountered: COMMA	Expected: ||	
-- SELECT inverse_normal_cdf(0, 1, 0.3); # error: errCode = 2, detailMessage = Can not found function 'inverse_normal_cdf'
-- SELECT inverse_normal_cdf(10, 9, 0.9); # error: errCode = 2, detailMessage = Can not found function 'inverse_normal_cdf'
-- SELECT inverse_normal_cdf(0.5, 0.25, 0.65); # error: errCode = 2, detailMessage = Can not found function 'inverse_normal_cdf'
-- SELECT normal_cdf(0, 1, 1.96); # error: errCode = 2, detailMessage = Can not found function 'normal_cdf'
-- SELECT normal_cdf(10, 9, 10); # error: errCode = 2, detailMessage = Can not found function 'normal_cdf'
-- SELECT normal_cdf(-1.5, 2.1, -7.8); # error: errCode = 2, detailMessage = Can not found function 'normal_cdf'
-- SELECT normal_cdf(0, 1, infinity()); # error: errCode = 2, detailMessage = Can not found function 'infinity'
-- SELECT normal_cdf(0, 1, -infinity()); # error: errCode = 2, detailMessage = Can not found function 'infinity'
-- SELECT normal_cdf(infinity(), 1, 0); # error: errCode = 2, detailMessage = Can not found function 'infinity'
-- SELECT normal_cdf(-infinity(), 1, 0); # error: errCode = 2, detailMessage = Can not found function 'infinity'
-- SELECT normal_cdf(0, infinity(), 0); # error: errCode = 2, detailMessage = Can not found function 'infinity'
-- SELECT normal_cdf(nan(), 1, 0); # error: errCode = 2, detailMessage = Can not found function 'nan'
-- SELECT normal_cdf(0, 1, nan()); # error: errCode = 2, detailMessage = Can not found function 'nan'
-- SELECT inverse_beta_cdf(3, 3.6, 0.0); # error: errCode = 2, detailMessage = Can not found function 'inverse_beta_cdf'
-- SELECT inverse_beta_cdf(3, 3.6, 1.0); # error: errCode = 2, detailMessage = Can not found function 'inverse_beta_cdf'
-- SELECT inverse_beta_cdf(3, 3.6, 0.3); # error: errCode = 2, detailMessage = Can not found function 'inverse_beta_cdf'
-- SELECT inverse_beta_cdf(3, 3.6, 0.95); # error: errCode = 2, detailMessage = Can not found function 'inverse_beta_cdf'
-- SELECT beta_cdf(3, 3.6, 0.0); # error: errCode = 2, detailMessage = Can not found function 'beta_cdf'
-- SELECT beta_cdf(3, 3.6, 1.0); # error: errCode = 2, detailMessage = Can not found function 'beta_cdf'
-- SELECT beta_cdf(3, 3.6, 0.3); # error: errCode = 2, detailMessage = Can not found function 'beta_cdf'
-- SELECT beta_cdf(3, 3.6, 0.9); # error: errCode = 2, detailMessage = Can not found function 'beta_cdf'
-- SELECT wilson_interval_lower(1250, 1310, 1.96e0); # error: errCode = 2, detailMessage = Can not found function 'wilson_interval_lower'
-- SELECT wilson_interval_upper(1250, 1310, 1.96e0); # error: errCode = 2, detailMessage = Can not found function 'wilson_interval_upper'
set debug_skip_fold_constant=true;
SELECT abs(TINYINT '123');
SELECT abs(TINYINT '-123');
SELECT abs(CAST(NULL AS TINYINT));
SELECT abs(SMALLINT '123');
SELECT abs(SMALLINT '-123');
SELECT abs(CAST(NULL AS SMALLINT));
SELECT abs(123);
SELECT abs(-123);
SELECT abs(CAST(NULL AS INTEGER));
-- SELECT abs(BIGINT '123'); # differ: doris : 123, presto : 123
-- SELECT abs(BIGINT '-123'); # differ: doris : 123, presto : 123
-- SELECT abs(12300000000); # differ: doris : 12300000000, presto : 12300000000
-- SELECT abs(-12300000000); # differ: doris : 12300000000, presto : 12300000000
SELECT abs(CAST(NULL AS BIGINT));
SELECT abs(123.0E0);
SELECT abs(-123.0E0);
-- SELECT abs(123.45E0); # differ: doris : 123.45, presto : 123.45
-- SELECT abs(-123.45E0); # differ: doris : 123.45, presto : 123.45
SELECT abs(CAST(NULL AS DOUBLE));
SELECT abs(REAL '-754.1985');
-- SELECT abs(DECIMAL '123.45'); # differ: doris : 123.450000000, presto : 123.45
-- SELECT abs(DECIMAL '-123.45'); # differ: doris : 123.450000000, presto : 123.45
-- SELECT abs(DECIMAL '1234567890123456.78'); # differ: doris : 1234567890123456.780000000, presto : 1234567890123456.78
-- SELECT abs(DECIMAL '-1234567890123456.78'); # differ: doris : 1234567890123456.780000000, presto : 1234567890123456.78
-- SELECT abs(DECIMAL '12345678901234560.78'); # differ: doris : 12345678901234560.780000000, presto : 12345678901234560.78
-- SELECT abs(DECIMAL '-12345678901234560.78'); # differ: doris : 12345678901234560.780000000, presto : 12345678901234560.78
SELECT abs(CAST(NULL AS DECIMAL(1,0)));
SELECT atan2(1.0E0, NULL);
SELECT atan2(NULL, 1.0E0);
SELECT ceil(TINYINT '123');
SELECT ceil(TINYINT '-123');
SELECT ceil(CAST(NULL AS TINYINT));
SELECT ceil(SMALLINT '123');
SELECT ceil(SMALLINT '-123');
SELECT ceil(CAST(NULL AS SMALLINT));
SELECT ceil(123);
SELECT ceil(-123);
SELECT ceil(CAST(NULL AS INTEGER));
SELECT ceil(BIGINT '123');
SELECT ceil(BIGINT '-123');
SELECT ceil(12300000000);
SELECT ceil(-12300000000);
SELECT ceil(CAST(NULL as BIGINT));
SELECT ceil(123.0E0);
SELECT ceil(-123.0E0);
SELECT ceil(123.45E0);
SELECT ceil(-123.45E0);
SELECT ceil(CAST(NULL as DOUBLE));
SELECT ceil(REAL '123.0');
SELECT ceil(REAL '-123.0');
SELECT ceil(REAL '123.45');
SELECT ceil(REAL '-123.45');
SELECT ceiling(12300000000);
SELECT ceiling(-12300000000);
SELECT ceiling(CAST(NULL AS BIGINT));
SELECT ceiling(123.0E0);
SELECT ceiling(-123.0E0);
SELECT ceiling(123.45E0);
SELECT ceiling(-123.45E0);
SELECT ceiling(REAL '123.0');
SELECT ceiling(REAL '-123.0');
SELECT ceiling(REAL '123.45');
SELECT ceiling(REAL '-123.45');
-- SELECT ceiling(DECIMAL '0'); # differ: doris : 0, presto : 0
-- SELECT ceiling(CAST(DECIMAL '0.00' AS DECIMAL(3,2))); # differ: doris : 0, presto : 0
-- SELECT ceiling(CAST(DECIMAL '0.00' AS DECIMAL(3,2))); # differ: doris : 0, presto : 0
-- SELECT ceiling(CAST(DECIMAL '0.01' AS DECIMAL(3,2))); # differ: doris : 1, presto : 1
-- SELECT ceiling(CAST(DECIMAL '-0.01' AS DECIMAL(3,2))); # differ: doris : 0, presto : 0
-- SELECT ceiling(CAST(DECIMAL '0.49' AS DECIMAL(3,2))); # differ: doris : 1, presto : 1
-- SELECT ceiling(CAST(DECIMAL '-0.49' AS DECIMAL(3,2))); # differ: doris : 0, presto : 0
-- SELECT ceiling(CAST(DECIMAL '0.50' AS DECIMAL(3,2))); # differ: doris : 1, presto : 1
-- SELECT ceiling(CAST(DECIMAL '-0.50' AS DECIMAL(3,2))); # differ: doris : 0, presto : 0
-- SELECT ceiling(CAST(DECIMAL '0.99' AS DECIMAL(3,2))); # differ: doris : 1, presto : 1
-- SELECT ceiling(CAST(DECIMAL '-0.99' AS DECIMAL(3,2))); # differ: doris : 0, presto : 0
-- SELECT ceiling(DECIMAL '123'); # differ: doris : 123, presto : 123
-- SELECT ceiling(DECIMAL '-123'); # differ: doris : -123, presto : -123
-- SELECT ceiling(DECIMAL '123.00'); # differ: doris : 123, presto : 123
-- SELECT ceiling(DECIMAL '-123.00'); # differ: doris : -123, presto : -123
-- SELECT ceiling(DECIMAL '123.01'); # differ: doris : 124, presto : 124
-- SELECT ceiling(DECIMAL '-123.01'); # differ: doris : -123, presto : -123
-- SELECT ceiling(DECIMAL '123.45'); # differ: doris : 124, presto : 124
-- SELECT ceiling(DECIMAL '-123.45'); # differ: doris : -123, presto : -123
-- SELECT ceiling(DECIMAL '123.49'); # differ: doris : 124, presto : 124
-- SELECT ceiling(DECIMAL '-123.49'); # differ: doris : -123, presto : -123
-- SELECT ceiling(DECIMAL '123.50'); # differ: doris : 124, presto : 124
-- SELECT ceiling(DECIMAL '-123.50'); # differ: doris : -123, presto : -123
-- SELECT ceiling(DECIMAL '123.99'); # differ: doris : 124, presto : 124
-- SELECT ceiling(DECIMAL '-123.99'); # differ: doris : -123, presto : -123
-- SELECT ceiling(DECIMAL '999.9'); # differ: doris : 1000, presto : 1000
-- SELECT ceiling(CAST(DECIMAL '0000000000000000000' AS DECIMAL(19,0))); # differ: doris : 0, presto : 0
-- SELECT ceiling(CAST(DECIMAL '000000000000000000.00' AS DECIMAL(20,2))); # differ: doris : 0, presto : 0
-- SELECT ceiling(CAST(DECIMAL '000000000000000000.01' AS DECIMAL(20,2))); # differ: doris : 1, presto : 1
-- SELECT ceiling(CAST(DECIMAL '-000000000000000000.01' AS DECIMAL(20,2))); # differ: doris : 0, presto : 0
-- SELECT ceiling(CAST(DECIMAL '000000000000000000.49' AS DECIMAL(20,2))); # differ: doris : 1, presto : 1
-- SELECT ceiling(CAST(DECIMAL '-000000000000000000.49' AS DECIMAL(20,2))); # differ: doris : 0, presto : 0
-- SELECT ceiling(CAST(DECIMAL '000000000000000000.50' AS DECIMAL(20,2))); # differ: doris : 1, presto : 1
-- SELECT ceiling(CAST(DECIMAL '-000000000000000000.50' AS DECIMAL(20,2))); # differ: doris : 0, presto : 0
-- SELECT ceiling(CAST(DECIMAL '000000000000000000.99' AS DECIMAL(20,2))); # differ: doris : 1, presto : 1
-- SELECT ceiling(CAST(DECIMAL '-000000000000000000.99' AS DECIMAL(20,2))); # differ: doris : 0, presto : 0
-- SELECT ceiling(DECIMAL '123456789012345678'); # differ: doris : 123456789012345678, presto : 123456789012345678
-- SELECT ceiling(DECIMAL '-123456789012345678'); # differ: doris : -123456789012345678, presto : -123456789012345678
-- SELECT ceiling(DECIMAL '123456789012345678.00'); # differ: doris : 123456789012345678, presto : 123456789012345678
-- SELECT ceiling(DECIMAL '-123456789012345678.00'); # differ: doris : -123456789012345678, presto : -123456789012345678
-- SELECT ceiling(DECIMAL '123456789012345678.01'); # differ: doris : 123456789012345679, presto : 123456789012345679
-- SELECT ceiling(DECIMAL '-123456789012345678.01'); # differ: doris : -123456789012345678, presto : -123456789012345678
-- SELECT ceiling(DECIMAL '123456789012345678.99'); # differ: doris : 123456789012345679, presto : 123456789012345679
-- SELECT ceiling(DECIMAL '-123456789012345678.99'); # differ: doris : -123456789012345678, presto : -123456789012345678
-- SELECT ceiling(DECIMAL '123456789012345678.49'); # differ: doris : 123456789012345679, presto : 123456789012345679
-- SELECT ceiling(DECIMAL '-123456789012345678.49'); # differ: doris : -123456789012345678, presto : -123456789012345678
-- SELECT ceiling(DECIMAL '123456789012345678.50'); # differ: doris : 123456789012345679, presto : 123456789012345679
-- SELECT ceiling(DECIMAL '-123456789012345678.50'); # differ: doris : -123456789012345678, presto : -123456789012345678
-- SELECT ceiling(DECIMAL '999999999999999999.9'); # differ: doris : 1000000000000000000, presto : 1000000000000000000
-- SELECT ceiling(DECIMAL '1234567890123456.78'); # differ: doris : 1234567890123457, presto : 1234567890123457
-- SELECT ceiling(DECIMAL '-1234567890123456.78'); # differ: doris : -1234567890123456, presto : -1234567890123456
SELECT ceiling(CAST(NULL AS DOUBLE));
SELECT ceiling(CAST(NULL AS REAL));
SELECT ceiling(CAST(NULL AS DECIMAL(1,0)));
SELECT ceiling(CAST(NULL AS DECIMAL(25,5)));
SELECT truncate(17.18E0);
SELECT truncate(-17.18E0);
SELECT truncate(17.88E0);
SELECT truncate(-17.88E0);
SELECT truncate(REAL '17.18');
SELECT truncate(REAL '-17.18');
SELECT truncate(REAL '17.88');
SELECT truncate(REAL '-17.88');
-- SELECT truncate(DECIMAL '-1234'); # differ: doris : -1234.0, presto : -1234
-- SELECT truncate(DECIMAL '1234.56'); # differ: doris : 1234.0, presto : 1234
-- SELECT truncate(DECIMAL '-1234.56'); # differ: doris : -1234.0, presto : -1234
-- SELECT truncate(DECIMAL '123456789123456.999'); # differ: doris : 123456789123457.0, presto : 123456789123456
-- SELECT truncate(DECIMAL '-123456789123456.999'); # differ: doris : -123456789123457.0, presto : -123456789123456
-- SELECT truncate(DECIMAL '1.99999999999999999999999999'); # differ: doris : 2.0, presto : 1
-- SELECT truncate(DECIMAL '-1.99999999999999999999999999'); # differ: doris : -2.0, presto : -1
-- SELECT truncate(DECIMAL '1234567890123456789012'); # differ: doris : 1.2345678901234568e+21, presto : 1234567890123456789012
-- SELECT truncate(DECIMAL '-1234567890123456789012'); # differ: doris : -1.2345678901234568e+21, presto : -1234567890123456789012
-- SELECT truncate(DECIMAL '1234567890123456789012.999'); # differ: doris : 1.2345678901234568e+21, presto : 1234567890123456789012
-- SELECT truncate(DECIMAL '-1234567890123456789012.999'); # differ: doris : -1.2345678901234568e+21, presto : -1234567890123456789012
-- SELECT truncate(DECIMAL '1234', 1); # differ: doris : 1234.0, presto : 1234
-- SELECT truncate(DECIMAL '1234', -1); # differ: doris : 1230, presto : 1230
-- SELECT truncate(DECIMAL '1234.56', 1); # differ: doris : 1234.5, presto : 1234.50
-- SELECT truncate(DECIMAL '1234.56', -1); # differ: doris : 1230, presto : 1230.00
-- SELECT truncate(DECIMAL '-1234.56', 1); # differ: doris : -1234.5, presto : -1234.50
-- SELECT truncate(DECIMAL '1239.99', 1); # differ: doris : 1239.9, presto : 1239.90
-- SELECT truncate(DECIMAL '-1239.99', 1); # differ: doris : -1239.9, presto : -1239.90
-- SELECT truncate(DECIMAL '1239.999', 2); # differ: doris : 1239.99, presto : 1239.990
-- SELECT truncate(DECIMAL '1239.999', -2); # differ: doris : 1200, presto : 1200.000
-- SELECT truncate(DECIMAL '123456789123456.999', 2); # differ: doris : 123456789123456.99, presto : 123456789123456.990
-- SELECT truncate(DECIMAL '123456789123456.999', -2); # differ: doris : 123456789123400, presto : 123456789123400.000
-- SELECT truncate(DECIMAL '1234', -4); # differ: doris : 0, presto : 0
-- SELECT truncate(DECIMAL '1234.56', -4); # differ: doris : 0, presto : 0.00
-- SELECT truncate(DECIMAL '-1234.56', -4); # differ: doris : 0, presto : 0.00
-- SELECT truncate(DECIMAL '1234.56', 3); # differ: doris : 1234.560, presto : 1234.56
-- SELECT truncate(DECIMAL '-1234.56', 3); # differ: doris : -1234.560, presto : -1234.56
-- SELECT truncate(DECIMAL '1234567890123456789012', 1); # differ: doris : 1234567890123456789012.0, presto : 1234567890123456789012
-- SELECT truncate(DECIMAL '1234567890123456789012', -1); # differ: doris : 1234567890123456789010, presto : 1234567890123456789010
-- SELECT truncate(DECIMAL '1234567890123456789012.23', 1); # differ: doris : 1234567890123456789012.2, presto : 1234567890123456789012.20
-- SELECT truncate(DECIMAL '1234567890123456789012.23', -1); # differ: doris : 1234567890123456789010, presto : 1234567890123456789010.00
-- SELECT truncate(DECIMAL '123456789012345678999.99', -1); # differ: doris : 123456789012345678990, presto : 123456789012345678990.00
-- SELECT truncate(DECIMAL '-123456789012345678999.99', -1); # differ: doris : -123456789012345678990, presto : -123456789012345678990.00
-- SELECT truncate(DECIMAL '123456789012345678999.999', 2); # differ: doris : 123456789012345678999.99, presto : 123456789012345678999.990
-- SELECT truncate(DECIMAL '123456789012345678999.999', -2); # differ: doris : 123456789012345678900, presto : 123456789012345678900.000
-- SELECT truncate(DECIMAL '123456789012345678901', -21); # differ: doris : 123456788998261831120704696330, presto : 0
-- SELECT truncate(DECIMAL '123456789012345678901.23', -21); # differ: doris : 123456788998261831120704696330, presto : 0.00
-- SELECT truncate(DECIMAL '123456789012345678901.23', 3); # differ: doris : 123456789012345678901.230, presto : 123456789012345678901.23
-- SELECT truncate(DECIMAL '-123456789012345678901.23', 3); # differ: doris : -123456789012345678901.230, presto : -123456789012345678901.23
SELECT truncate(CAST(NULL AS DOUBLE));
SELECT truncate(CAST(NULL AS DECIMAL(1,0)), -1);
SELECT truncate(CAST(NULL AS DECIMAL(1,0)));
SELECT truncate(CAST(NULL AS DECIMAL(18,5)));
SELECT truncate(CAST(NULL AS DECIMAL(25,2)));
-- SELECT truncate(NULL, NULL); # error: errCode = 2, detailMessage = class org.apache.doris.nereids.trees.expressions.literal.NullLiteral cannot be cast to class org.apache.doris.nereids.trees.expressions.literal.IntegerLikeLiteral (org.apache.doris.nereids.trees.expressions.literal.NullLiteral and org.apache.doris.nereids.trees.expressions.literal.IntegerLikeLiteral are in unnamed module of loader 'app')
SELECT e();
SELECT floor(TINYINT '123');
SELECT floor(TINYINT '-123');
SELECT floor(CAST(NULL AS TINYINT));
SELECT floor(SMALLINT '123');
SELECT floor(SMALLINT '-123');
SELECT floor(CAST(NULL AS SMALLINT));
SELECT floor(123);
SELECT floor(-123);
SELECT floor(CAST(NULL AS INTEGER));
SELECT floor(BIGINT '123');
SELECT floor(BIGINT '-123');
SELECT floor(12300000000);
SELECT floor(-12300000000);
SELECT floor(CAST(NULL as BIGINT));
SELECT floor(123.0E0);
SELECT floor(-123.0E0);
SELECT floor(123.45E0);
SELECT floor(-123.45E0);
SELECT floor(REAL '123.0');
SELECT floor(REAL '-123.0');
SELECT floor(REAL '123.45');
SELECT floor(REAL '-123.45');
-- SELECT floor(DECIMAL '0'); # differ: doris : 0, presto : 0
-- SELECT floor(CAST(DECIMAL '0.00' AS DECIMAL(3,2))); # differ: doris : 0, presto : 0
-- SELECT floor(CAST(DECIMAL '0.00' AS DECIMAL(3,2))); # differ: doris : 0, presto : 0
-- SELECT floor(CAST(DECIMAL '0.01' AS DECIMAL(3,2))); # differ: doris : 0, presto : 0
-- SELECT floor(CAST(DECIMAL '-0.01' AS DECIMAL(3,2))); # differ: doris : -1, presto : -1
-- SELECT floor(CAST(DECIMAL '0.49' AS DECIMAL(3,2))); # differ: doris : 0, presto : 0
-- SELECT floor(CAST(DECIMAL '-0.49' AS DECIMAL(3,2))); # differ: doris : -1, presto : -1
-- SELECT floor(CAST(DECIMAL '0.50' AS DECIMAL(3,2))); # differ: doris : 0, presto : 0
-- SELECT floor(CAST(DECIMAL '-0.50' AS DECIMAL(3,2))); # differ: doris : -1, presto : -1
-- SELECT floor(CAST(DECIMAL '0.99' AS DECIMAL(3,2))); # differ: doris : 0, presto : 0
-- SELECT floor(CAST(DECIMAL '-0.99' AS DECIMAL(3,2))); # differ: doris : -1, presto : -1
-- SELECT floor(DECIMAL '123'); # differ: doris : 123, presto : 123
-- SELECT floor(DECIMAL '-123'); # differ: doris : -123, presto : -123
-- SELECT floor(DECIMAL '123.00'); # differ: doris : 123, presto : 123
-- SELECT floor(DECIMAL '-123.00'); # differ: doris : -123, presto : -123
-- SELECT floor(DECIMAL '123.01'); # differ: doris : 123, presto : 123
-- SELECT floor(DECIMAL '-123.01'); # differ: doris : -124, presto : -124
-- SELECT floor(DECIMAL '123.45'); # differ: doris : 123, presto : 123
-- SELECT floor(DECIMAL '-123.45'); # differ: doris : -124, presto : -124
-- SELECT floor(DECIMAL '123.49'); # differ: doris : 123, presto : 123
-- SELECT floor(DECIMAL '-123.49'); # differ: doris : -124, presto : -124
-- SELECT floor(DECIMAL '123.50'); # differ: doris : 123, presto : 123
-- SELECT floor(DECIMAL '-123.50'); # differ: doris : -124, presto : -124
-- SELECT floor(DECIMAL '123.99'); # differ: doris : 123, presto : 123
-- SELECT floor(DECIMAL '-123.99'); # differ: doris : -124, presto : -124
-- SELECT floor(DECIMAL '-999.9'); # differ: doris : -1000, presto : -1000
-- SELECT floor(CAST(DECIMAL '0000000000000000000' AS DECIMAL(19,0))); # differ: doris : 0, presto : 0
-- SELECT floor(CAST(DECIMAL '000000000000000000.00' AS DECIMAL(20,2))); # differ: doris : 0, presto : 0
-- SELECT floor(CAST(DECIMAL '000000000000000000.01' AS DECIMAL(20,2))); # differ: doris : 0, presto : 0
-- SELECT floor(CAST(DECIMAL '-000000000000000000.01' AS DECIMAL(20,2))); # differ: doris : -1, presto : -1
-- SELECT floor(CAST(DECIMAL '000000000000000000.49' AS DECIMAL(20,2))); # differ: doris : 0, presto : 0
-- SELECT floor(CAST(DECIMAL '-000000000000000000.49' AS DECIMAL(20,2))); # differ: doris : -1, presto : -1
-- SELECT floor(CAST(DECIMAL '000000000000000000.50' AS DECIMAL(20,2))); # differ: doris : 0, presto : 0
-- SELECT floor(CAST(DECIMAL '-000000000000000000.50' AS DECIMAL(20,2))); # differ: doris : -1, presto : -1
-- SELECT floor(CAST(DECIMAL '000000000000000000.99' AS DECIMAL(20,2))); # differ: doris : 0, presto : 0
-- SELECT floor(CAST(DECIMAL '-000000000000000000.99' AS DECIMAL(20,2))); # differ: doris : -1, presto : -1
-- SELECT floor(DECIMAL '123456789012345678'); # differ: doris : 123456789012345678, presto : 123456789012345678
-- SELECT floor(DECIMAL '-123456789012345678'); # differ: doris : -123456789012345678, presto : -123456789012345678
-- SELECT floor(DECIMAL '123456789012345678.00'); # differ: doris : 123456789012345678, presto : 123456789012345678
-- SELECT floor(DECIMAL '-123456789012345678.00'); # differ: doris : -123456789012345678, presto : -123456789012345678
-- SELECT floor(DECIMAL '123456789012345678.01'); # differ: doris : 123456789012345678, presto : 123456789012345678
-- SELECT floor(DECIMAL '-123456789012345678.01'); # differ: doris : -123456789012345679, presto : -123456789012345679
-- SELECT floor(DECIMAL '123456789012345678.99'); # differ: doris : 123456789012345678, presto : 123456789012345678
-- SELECT floor(DECIMAL '-123456789012345678.49'); # differ: doris : -123456789012345679, presto : -123456789012345679
-- SELECT floor(DECIMAL '123456789012345678.49'); # differ: doris : 123456789012345678, presto : 123456789012345678
-- SELECT floor(DECIMAL '-123456789012345678.50'); # differ: doris : -123456789012345679, presto : -123456789012345679
-- SELECT floor(DECIMAL '123456789012345678.50'); # differ: doris : 123456789012345678, presto : 123456789012345678
-- SELECT floor(DECIMAL '-123456789012345678.99'); # differ: doris : -123456789012345679, presto : -123456789012345679
-- SELECT floor(DECIMAL '-999999999999999999.9'); # differ: doris : -1000000000000000000, presto : -1000000000000000000
-- SELECT floor(DECIMAL '1234567890123456.78'); # differ: doris : 1234567890123456, presto : 1234567890123456
-- SELECT floor(DECIMAL '-1234567890123456.78'); # differ: doris : -1234567890123457, presto : -1234567890123457
SELECT floor(CAST(NULL as REAL));
SELECT floor(CAST(NULL as DOUBLE));
SELECT floor(CAST(NULL as DECIMAL(1,0)));
SELECT floor(CAST(NULL as DECIMAL(25,5)));
SELECT log(5.0E0, NULL);
SELECT log(NULL, 5.0E0);
-- SELECT mod(DECIMAL '0.0', DECIMAL '2.0'); # differ: doris : 0E-9, presto : 0.0
SELECT mod(NULL, 5.0E0);
-- SELECT mod(DECIMAL '0.0', DECIMAL '2.0'); # differ: doris : 0E-9, presto : 0.0
-- SELECT mod(DECIMAL '13.0', DECIMAL '5.0'); # differ: doris : 3.000000000, presto : 3.0
-- SELECT mod(DECIMAL '-13.0', DECIMAL '5.0'); # differ: doris : -3.000000000, presto : -3.0
-- SELECT mod(DECIMAL '13.0', DECIMAL '-5.0'); # differ: doris : 3.000000000, presto : 3.0
-- SELECT mod(DECIMAL '-13.0', DECIMAL '-5.0'); # differ: doris : -3.000000000, presto : -3.0
-- SELECT mod(DECIMAL '5.0', DECIMAL '2.5'); # differ: doris : 0E-9, presto : 0.0
-- SELECT mod(DECIMAL '5.0', DECIMAL '2.05'); # differ: doris : 0.900000000, presto : 0.90
-- SELECT mod(DECIMAL '5.0', DECIMAL '2.55'); # differ: doris : 2.450000000, presto : 2.45
-- SELECT mod(DECIMAL '5.0001', DECIMAL '2.55'); # differ: doris : 2.450100000, presto : 2.4501
-- SELECT mod(DECIMAL '123456789012345670', DECIMAL '123456789012345669'); # differ: doris : 1.000000000, presto : 1
-- SELECT mod(DECIMAL '12345678901234567.90', DECIMAL '12345678901234567.89'); # differ: doris : 0.010000000, presto : 0.01
SELECT mod(DECIMAL '5.0', CAST(NULL as DECIMAL(1,0)));
SELECT mod(CAST(NULL as DECIMAL(1,0)), DECIMAL '5.0');
SELECT pi();
-- SELECT nan(); # error: errCode = 2, detailMessage = Can not found function 'nan'
-- SELECT infinity(); # error: errCode = 2, detailMessage = Can not found function 'infinity'
-- SELECT is_infinite(1.0E0 / 0.0E0); # error: errCode = 2, detailMessage = Can not found function 'is_infinite'
-- SELECT is_infinite(0.0E0 / 0.0E0); # error: errCode = 2, detailMessage = Can not found function 'is_infinite'
-- SELECT is_infinite(1.0E0 / 1.0E0); # error: errCode = 2, detailMessage = Can not found function 'is_infinite'
-- SELECT is_infinite(REAL '1.0' / REAL '0.0'); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT is_infinite(REAL '1.0' / REAL '0.0');	                   ^	Encountered: REAL	Expected: REAL is keyword, maybe `REAL`	
-- SELECT is_infinite(REAL '0.0' / REAL '0.0'); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT is_infinite(REAL '0.0' / REAL '0.0');	                   ^	Encountered: REAL	Expected: REAL is keyword, maybe `REAL`	
-- SELECT is_infinite(REAL '1.0' / REAL '1.0'); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT is_infinite(REAL '1.0' / REAL '1.0');	                   ^	Encountered: REAL	Expected: REAL is keyword, maybe `REAL`	
-- SELECT is_infinite(NULL); # error: errCode = 2, detailMessage = Can not found function 'is_infinite'
-- SELECT is_finite(100000); # error: errCode = 2, detailMessage = Can not found function 'is_finite'
-- SELECT is_finite(rand() / 0.0E0); # error: errCode = 2, detailMessage = Can not found function 'is_finite'
-- SELECT is_finite(REAL '754.2008E0'); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT is_finite(REAL '754.2008E0');	                 ^	Encountered: REAL	Expected: REAL is keyword, maybe `REAL`	
-- SELECT is_finite(rand() / REAL '0.0E0'); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT is_finite(rand() / REAL '0.0E0');	                          ^	Encountered: REAL	Expected: REAL is keyword, maybe `REAL`	
-- SELECT is_finite(NULL); # error: errCode = 2, detailMessage = Can not found function 'is_finite'
-- SELECT is_nan(0.0E0 / 0.0E0); # error: errCode = 2, detailMessage = Can not found function 'IS_NAN'
-- SELECT is_nan(0.0E0 / 1.0E0); # error: errCode = 2, detailMessage = Can not found function 'IS_NAN'
-- SELECT is_nan(infinity() / infinity()); # error: errCode = 2, detailMessage = Can not found function 'infinity'
-- SELECT is_nan(nan()); # error: errCode = 2, detailMessage = Can not found function 'nan'
-- SELECT is_nan(REAL 'NaN'); # error: errCode = 2, detailMessage = Can not found function 'IS_NAN'
-- SELECT is_nan(REAL '0.0' / REAL '0.0'); # error: errCode = 2, detailMessage = Can not found function 'IS_NAN'
-- SELECT is_nan(REAL '0.0' / 1.0E0); # error: errCode = 2, detailMessage = Can not found function 'IS_NAN'
-- SELECT is_nan(infinity() / infinity()); # error: errCode = 2, detailMessage = Can not found function 'infinity'
-- SELECT is_nan(nan()); # error: errCode = 2, detailMessage = Can not found function 'nan'
-- SELECT is_nan(NULL); # error: errCode = 2, detailMessage = Can not found function 'IS_NAN'
SELECT power(5.0E0, NULL);
SELECT power(NULL, 5.0E0);
SELECT pow(5.0E0, 2.0E0);
SELECT round(TINYINT '3');
SELECT round(TINYINT '-3');
SELECT round(CAST(NULL as TINYINT));
SELECT round(SMALLINT '3');
SELECT round(SMALLINT '-3');
SELECT round(CAST(NULL as SMALLINT));
SELECT round(3);
SELECT round(-3);
SELECT round(CAST(NULL as INTEGER));
SELECT round(BIGINT '3');
SELECT round(BIGINT '-3');
SELECT round(CAST(NULL as BIGINT));
SELECT round( 3000000000);
SELECT round(-3000000000);
SELECT round(3.0E0);
SELECT round(-3.0E0);
SELECT round(3.499E0);
SELECT round(-3.499E0);
SELECT round(3.5E0);
SELECT round(-3.5E0);
SELECT round(-3.5001E0);
SELECT round(-3.99E0);
SELECT round(REAL '3.0');
SELECT round(REAL '-3.0');
SELECT round(REAL '3.499');
SELECT round(REAL '-3.499');
SELECT round(REAL '3.5');
SELECT round(REAL '-3.5');
SELECT round(REAL '-3.5001');
SELECT round(REAL '-3.99');
SELECT round(CAST(NULL as DOUBLE));
SELECT round(TINYINT '3', TINYINT '0');
SELECT round(TINYINT '3', 0);
SELECT round(SMALLINT '3', SMALLINT '0');
SELECT round(SMALLINT '3', 0);
SELECT round(3, 0);
SELECT round(-3, 0);
SELECT round(-3, INTEGER '0');
SELECT round(BIGINT '3', 0);
SELECT round( 3000000000, 0);
SELECT round(-3000000000, 0);
SELECT round(3.0E0, 0);
SELECT round(-3.0E0, 0);
SELECT round(3.499E0, 0);
SELECT round(-3.499E0, 0);
SELECT round(3.5E0, 0);
SELECT round(-3.5E0, 0);
SELECT round(-3.5001E0, 0);
SELECT round(-3.99E0, 0);
SELECT round(0.3E0);
SELECT round(-0.3E0);
-- SELECT round(923e0, 16); # differ: doris : 923, presto : 922.3372036854776
-- SELECT round(DOUBLE '3000.1234567890123456789', 16); # differ: doris : 3000.1234567890124, presto : 922.3372036854776
-- SELECT round(DOUBLE '1.8E292', 16); # differ: doris : inf, presto : 922.3372036854776
-- SELECT round(DOUBLE '-1.8E292', 16); # differ: doris : -inf, presto : -922.3372036854776
SELECT round(TINYINT '3', TINYINT '1');
SELECT round(TINYINT '3', 1);
SELECT round(SMALLINT '3', SMALLINT '1');
SELECT round(SMALLINT '3', 1);
SELECT round(REAL '3.0', 0);
SELECT round(REAL '-3.0', 0);
SELECT round(REAL '3.499', 0);
SELECT round(REAL '-3.499', 0);
SELECT round(REAL '3.5', 0);
SELECT round(REAL '-3.5', 0);
SELECT round(REAL '-3.5001', 0);
SELECT round(REAL '-3.99', 0);
-- SELECT round(REAL '923', 16); # differ: doris : 923.0, presto : 922.3372
-- SELECT round(REAL '3000.1234567890123456789', 16); # differ: doris : 3000.12353515625, presto : 922.3372
-- SELECT round(REAL '3.4028235e+38', 271); # differ: doris : 3.4028234663852886e+38, presto : 0.0
-- SELECT round(REAL '-3.4028235e+38', 271); # differ: doris : -3.4028234663852886e+38, presto : -0.0
SELECT round(3, 1);
SELECT round(-3, 1);
SELECT round(-3, INTEGER '1');
SELECT round(-3, CAST(NULL as INTEGER));
SELECT round(BIGINT '3', 1);
SELECT round( 3000000000, 1);
SELECT round(-3000000000, 1);
SELECT round(CAST(NULL as BIGINT), CAST(NULL as INTEGER));
SELECT round(CAST(NULL as BIGINT), 1);
SELECT round(3.0E0, 1);
SELECT round(-3.0E0, 1);
SELECT round(3.499E0, 1);
SELECT round(-3.499E0, 1);
SELECT round(3.5E0, 1);
SELECT round(-3.5E0, 1);
SELECT round(-3.5001E0, 1);
SELECT round(-3.99E0, 1);
SELECT round(REAL '3.0', 1);
SELECT round(REAL '-3.0', 1);
SELECT round(REAL '3.499', 1);
SELECT round(REAL '-3.499', 1);
SELECT round(REAL '3.5', 1);
SELECT round(REAL '-3.5', 1);
SELECT round(REAL '-3.5001', 1);
SELECT round(REAL '-3.99', 1);
SELECT round(TINYINT '9', -1);
SELECT round(TINYINT '-9', -1);
SELECT round(TINYINT '5', -1);
SELECT round(TINYINT '-5', -1);
SELECT round(TINYINT '-14', -1);
SELECT round(TINYINT '12', -1);
SELECT round(TINYINT '18', -1);
SELECT round(TINYINT '18', -2);
SELECT round(TINYINT '18', -3);
SELECT round(TINYINT '127', -2);
SELECT round(TINYINT '127', -3);
SELECT round(TINYINT '-128', -2);
SELECT round(TINYINT '-128', -3);
SELECT round(SMALLINT '99', -1);
SELECT round(SMALLINT '99', -2);
SELECT round(SMALLINT '99', -3);
SELECT round(SMALLINT '-99', -1);
SELECT round(SMALLINT '-99', -2);
SELECT round(SMALLINT '-99', -3);
SELECT round(SMALLINT '32767', -4);
SELECT round(SMALLINT '32767', -5);
SELECT round(SMALLINT '-32768', -4);
SELECT round(SMALLINT '-32768', -5);
SELECT round(99, -1);
SELECT round(-99, -1);
SELECT round(99, INTEGER '-1');
SELECT round(-99, INTEGER '-1');
SELECT round(12355, -2);
SELECT round(12345, -2);
SELECT round(2147483647, -9);
SELECT round(2147483647, -10);
SELECT round( 3999999999, -1);
SELECT round(-3999999999, -1);
SELECT round(9223372036854775807, -2);
SELECT round(9223372036854775807, -17);
SELECT round(9223372036854775807, -18);
SELECT round(-9223372036854775807, -17);
SELECT round(-9223372036854775807, -18);
-- SELECT round(DECIMAL '0'); # differ: doris : 0, presto : 0
-- SELECT round(DECIMAL '0.1'); # differ: doris : 0, presto : 0
-- SELECT round(DECIMAL '-0.1'); # differ: doris : 0, presto : 0
-- SELECT round(DECIMAL '3'); # differ: doris : 3, presto : 3
-- SELECT round(DECIMAL '-3'); # differ: doris : -3, presto : -3
-- SELECT round(DECIMAL '3.0'); # differ: doris : 3, presto : 3
-- SELECT round(DECIMAL '-3.0'); # differ: doris : -3, presto : -3
-- SELECT round(DECIMAL '3.49'); # differ: doris : 3, presto : 3
-- SELECT round(DECIMAL '-3.49'); # differ: doris : -3, presto : -3
-- SELECT round(DECIMAL '3.50'); # differ: doris : 4, presto : 4
-- SELECT round(DECIMAL '-3.50'); # differ: doris : -4, presto : -4
-- SELECT round(DECIMAL '3.99'); # differ: doris : 4, presto : 4
-- SELECT round(DECIMAL '-3.99'); # differ: doris : -4, presto : -4
-- SELECT round(DECIMAL '9.99'); # differ: doris : 10, presto : 10
-- SELECT round(DECIMAL '-9.99'); # differ: doris : -10, presto : -10
-- SELECT round(DECIMAL '9999.9'); # differ: doris : 10000, presto : 10000
-- SELECT round(DECIMAL '-9999.9'); # differ: doris : -10000, presto : -10000
-- SELECT round(DECIMAL '1000000000000.9999'); # differ: doris : 1000000000001, presto : 1000000000001
-- SELECT round(DECIMAL '-1000000000000.9999'); # differ: doris : -1000000000001, presto : -1000000000001
-- SELECT round(DECIMAL '10000000000000000'); # differ: doris : 10000000000000000, presto : 10000000000000000
-- SELECT round(DECIMAL '-10000000000000000'); # differ: doris : -10000000000000000, presto : -10000000000000000
-- SELECT round(DECIMAL '9999999999999999.99'); # differ: doris : 10000000000000000, presto : 10000000000000000
-- SELECT round(DECIMAL '99999999999999999.9'); # differ: doris : 100000000000000000, presto : 100000000000000000
-- SELECT round(CAST(0 AS DECIMAL(18,0))); # differ: doris : 0, presto : 0
-- SELECT round(CAST(0 AS DECIMAL(18,1))); # differ: doris : 0, presto : 0
-- SELECT round(CAST(0 AS DECIMAL(18,2))); # differ: doris : 0, presto : 0
-- SELECT round(CAST(DECIMAL '0.1' AS DECIMAL(18,1))); # differ: doris : 0, presto : 0
-- SELECT round(CAST(DECIMAL '-0.1' AS DECIMAL(18,1))); # differ: doris : 0, presto : 0
-- SELECT round(DECIMAL '3000000000000000000000'); # differ: doris : 3000000000000000000000, presto : 3000000000000000000000
-- SELECT round(DECIMAL '-3000000000000000000000'); # differ: doris : -3000000000000000000000, presto : -3000000000000000000000
-- SELECT round(DECIMAL '3000000000000000000000.0'); # differ: doris : 3000000000000000000000, presto : 3000000000000000000000
-- SELECT round(DECIMAL '-3000000000000000000000.0'); # differ: doris : -3000000000000000000000, presto : -3000000000000000000000
-- SELECT round(DECIMAL '3000000000000000000000.49'); # differ: doris : 3000000000000000000000, presto : 3000000000000000000000
-- SELECT round(DECIMAL '-3000000000000000000000.49'); # differ: doris : -3000000000000000000000, presto : -3000000000000000000000
-- SELECT round(DECIMAL '3000000000000000000000.50'); # differ: doris : 3000000000000000000001, presto : 3000000000000000000001
-- SELECT round(DECIMAL '-3000000000000000000000.50'); # differ: doris : -3000000000000000000001, presto : -3000000000000000000001
-- SELECT round(DECIMAL '3000000000000000000000.99'); # differ: doris : 3000000000000000000001, presto : 3000000000000000000001
-- SELECT round(DECIMAL '-3000000000000000000000.99'); # differ: doris : -3000000000000000000001, presto : -3000000000000000000001
-- SELECT round(DECIMAL '9999999999999999999999.99'); # differ: doris : 10000000000000000000000, presto : 10000000000000000000000
-- SELECT round(DECIMAL '-9999999999999999999999.99'); # differ: doris : -10000000000000000000000, presto : -10000000000000000000000
-- SELECT round(DECIMAL '1000000000000000000000000000000000.9999'); # differ: doris : 100000000000000000000000000000, presto : 1000000000000000000000000000000001
-- SELECT round(DECIMAL '-1000000000000000000000000000000000.9999'); # differ: doris : -100000000000000000000000000000, presto : -1000000000000000000000000000000001
-- SELECT round(DECIMAL '10000000000000000000000000000000000000'); # differ: doris : 100000000000000000000000000000, presto : 10000000000000000000000000000000000000
-- SELECT round(DECIMAL '-10000000000000000000000000000000000000'); # differ: doris : -100000000000000000000000000000, presto : -10000000000000000000000000000000000000
-- SELECT round(DECIMAL '3000000000000000.000000'); # differ: doris : 3000000000000000, presto : 3000000000000000
-- SELECT round(DECIMAL '-3000000000000000.000000'); # differ: doris : -3000000000000000, presto : -3000000000000000
-- SELECT round(DECIMAL '3000000000000000.499999'); # differ: doris : 3000000000000000, presto : 3000000000000000
-- SELECT round(DECIMAL '-3000000000000000.499999'); # differ: doris : -3000000000000000, presto : -3000000000000000
-- SELECT round(DECIMAL '3000000000000000.500000'); # differ: doris : 3000000000000001, presto : 3000000000000001
-- SELECT round(DECIMAL '-3000000000000000.500000'); # differ: doris : -3000000000000001, presto : -3000000000000001
-- SELECT round(DECIMAL '3000000000000000.999999'); # differ: doris : 3000000000000001, presto : 3000000000000001
-- SELECT round(DECIMAL '-3000000000000000.999999'); # differ: doris : -3000000000000001, presto : -3000000000000001
-- SELECT round(DECIMAL '9999999999999999.999999'); # differ: doris : 10000000000000000, presto : 10000000000000000
-- SELECT round(DECIMAL '-9999999999999999.999999'); # differ: doris : -10000000000000000, presto : -10000000000000000
-- SELECT round(DECIMAL '3', 1); # differ: doris : 3.0, presto : 3
-- SELECT round(DECIMAL '-3', 1); # differ: doris : -3.0, presto : -3
-- SELECT round(DECIMAL '3.0', 1); # differ: doris : 3.0, presto : 3.0
-- SELECT round(DECIMAL '-3.0', 1); # differ: doris : -3.0, presto : -3.0
-- SELECT round(DECIMAL '3.449', 1); # differ: doris : 3.4, presto : 3.400
-- SELECT round(DECIMAL '-3.449', 1); # differ: doris : -3.4, presto : -3.400
-- SELECT round(DECIMAL '3.450', 1); # differ: doris : 3.5, presto : 3.500
-- SELECT round(DECIMAL '-3.450', 1); # differ: doris : -3.5, presto : -3.500
-- SELECT round(DECIMAL '3.99', 1); # differ: doris : 4.0, presto : 4.00
-- SELECT round(DECIMAL '-3.99', 1); # differ: doris : -4.0, presto : -4.00
-- SELECT round(DECIMAL '9.99', 1); # differ: doris : 10.0, presto : 10.00
-- SELECT round(DECIMAL '-9.99', 1); # differ: doris : -10.0, presto : -10.00
-- SELECT round(DECIMAL '0.3', 0); # differ: doris : 0, presto : 0.0
-- SELECT round(DECIMAL '0.7', 0); # differ: doris : 1, presto : 0.0
-- SELECT round(DECIMAL '1.7', 0); # differ: doris : 2, presto : 2.0
-- SELECT round(DECIMAL '-0.3', 0); # differ: doris : 0, presto : 0.0
-- SELECT round(DECIMAL '-0.7', 0); # differ: doris : -1, presto : 0.0
-- SELECT round(DECIMAL '-1.7', 0); # differ: doris : -2, presto : -2.0
-- SELECT round(DECIMAL '0.7', -1); # differ: doris : 0, presto : 0.0
-- SELECT round(DECIMAL '1.7', -1); # differ: doris : 0, presto : 0.0
-- SELECT round(DECIMAL '7.1', -1); # differ: doris : 10, presto : 0.0
-- SELECT round(DECIMAL '0.3', -1); # differ: doris : 0, presto : 0.0
-- SELECT round(DECIMAL '33.3', -2); # differ: doris : 0, presto : 0.0
-- SELECT round(CAST(DECIMAL '0.7' AS decimal(20, 1)), -19); # differ: doris : 0, presto : 0.0
-- SELECT round(DECIMAL '0.00', 1); # differ: doris : 0.0, presto : 0.00
-- SELECT round(DECIMAL '1234', 7); # differ: doris : 1234.0000000, presto : 1234
-- SELECT round(DECIMAL '-1234', 7); # differ: doris : -1234.0000000, presto : -1234
-- SELECT round(DECIMAL '1234', -7); # differ: doris : 0, presto : 0
-- SELECT round(DECIMAL '-1234', -7); # differ: doris : 0, presto : 0
-- SELECT round(DECIMAL '1234.5678', 7); # differ: doris : 1234.5678000, presto : 1234.5678
-- SELECT round(DECIMAL '-1234.5678', 7); # differ: doris : -1234.5678000, presto : -1234.5678
-- SELECT round(DECIMAL '1234.5678', -2); # differ: doris : 1200, presto : 1200.0000
-- SELECT round(DECIMAL '-1234.5678', -2); # differ: doris : -1200, presto : -1200.0000
-- SELECT round(DECIMAL '1254.5678', -2); # differ: doris : 1300, presto : 1300.0000
-- SELECT round(DECIMAL '-1254.5678', -2); # differ: doris : -1300, presto : -1300.0000
-- SELECT round(DECIMAL '1234.5678', -7); # differ: doris : 0, presto : 0.0000
-- SELECT round(DECIMAL '-1234.5678', -7); # differ: doris : 0, presto : 0.0000
-- SELECT round(DECIMAL '99', -1); # differ: doris : 100, presto : 100
-- SELECT round(DECIMAL '1234567890123456789', 1); # differ: doris : 1234567890123456789.0, presto : 1234567890123456789
-- SELECT round(DECIMAL '-1234567890123456789', 1); # differ: doris : -1234567890123456789.0, presto : -1234567890123456789
-- SELECT round(DECIMAL '123456789012345678.0', 1); # differ: doris : 123456789012345678.0, presto : 123456789012345678.0
-- SELECT round(DECIMAL '-123456789012345678.0', 1); # differ: doris : -123456789012345678.0, presto : -123456789012345678.0
-- SELECT round(DECIMAL '123456789012345678.449', 1); # differ: doris : 123456789012345678.4, presto : 123456789012345678.400
-- SELECT round(DECIMAL '-123456789012345678.449', 1); # differ: doris : -123456789012345678.4, presto : -123456789012345678.400
-- SELECT round(DECIMAL '123456789012345678.45', 1); # differ: doris : 123456789012345678.5, presto : 123456789012345678.50
-- SELECT round(DECIMAL '-123456789012345678.45', 1); # differ: doris : -123456789012345678.5, presto : -123456789012345678.50
-- SELECT round(DECIMAL '123456789012345678.501', 1); # differ: doris : 123456789012345678.5, presto : 123456789012345678.500
-- SELECT round(DECIMAL '-123456789012345678.501', 1); # differ: doris : -123456789012345678.5, presto : -123456789012345678.500
-- SELECT round(DECIMAL '999999999999999999.99', 1); # differ: doris : 1000000000000000000.0, presto : 1000000000000000000.00
-- SELECT round(DECIMAL '-999999999999999999.99', 1); # differ: doris : -1000000000000000000.0, presto : -1000000000000000000.00
-- SELECT round(DECIMAL '1234567890123456789', 7); # differ: doris : 1234567890123456789.0000000, presto : 1234567890123456789
-- SELECT round(DECIMAL '-1234567890123456789', 7); # differ: doris : -1234567890123456789.0000000, presto : -1234567890123456789
-- SELECT round(DECIMAL '123456789012345678.99', 7); # differ: doris : 123456789012345678.9900000, presto : 123456789012345678.99
-- SELECT round(DECIMAL '-123456789012345678.99', 7); # differ: doris : -123456789012345678.9900000, presto : -123456789012345678.99
-- SELECT round(DECIMAL '123456789012345611.99', -2); # differ: doris : 123456789012345600, presto : 123456789012345600.00
-- SELECT round(DECIMAL '-123456789012345611.99', -2); # differ: doris : -123456789012345600, presto : -123456789012345600.00
-- SELECT round(DECIMAL '123456789012345678.99', -2); # differ: doris : 123456789012345700, presto : 123456789012345700.00
-- SELECT round(DECIMAL '-123456789012345678.99', -2); # differ: doris : -123456789012345700, presto : -123456789012345700.00
-- SELECT round(DECIMAL '123456789012345678.99', -30); # differ: doris : 123456790068172987395858690, presto : 0.00
-- SELECT round(DECIMAL '-123456789012345678.99', -30); # differ: doris : -123456790068172987395858690, presto : 0.00
-- SELECT round(DECIMAL '9999999999999999999999999999999999999.9', 1); # differ: doris : 100000000000000000000000000000.0, presto : 9999999999999999999999999999999999999.9
-- SELECT round(DECIMAL  '1329123201320737513', -3); # differ: doris : 1329123201320738000, presto : 1329123201320738000
-- SELECT round(DECIMAL '-1329123201320737513', -3); # differ: doris : -1329123201320738000, presto : -1329123201320738000
-- SELECT round(DECIMAL  '1329123201320739513', -3); # differ: doris : 1329123201320740000, presto : 1329123201320740000
-- SELECT round(DECIMAL '-1329123201320739513', -3); # differ: doris : -1329123201320740000, presto : -1329123201320740000
-- SELECT round(DECIMAL  '9999999999999999999', -3); # differ: doris : 10000000000000000000, presto : 10000000000000000000
-- SELECT round(DECIMAL '-9999999999999999999', -3); # differ: doris : -10000000000000000000, presto : -10000000000000000000
-- SELECT round(DECIMAL '9999999999999999.99', 1); # differ: doris : 10000000000000000.0, presto : 10000000000000000.00
-- SELECT round(DECIMAL '-9999999999999999.99', 1); # differ: doris : -10000000000000000.0, presto : -10000000000000000.00
-- SELECT round(DECIMAL '9999999999999999.99', -1); # differ: doris : 10000000000000000, presto : 10000000000000000.00
-- SELECT round(DECIMAL '-9999999999999999.99', -1); # differ: doris : -10000000000000000, presto : -10000000000000000.00
-- SELECT round(DECIMAL '9999999999999999.99', 2); # differ: doris : 9999999999999999.99, presto : 9999999999999999.99
-- SELECT round(DECIMAL '-9999999999999999.99', 2); # differ: doris : -9999999999999999.99, presto : -9999999999999999.99
-- SELECT round(DECIMAL '329123201320737513', -3); # differ: doris : 329123201320738000, presto : 329123201320738000
-- SELECT round(DECIMAL '-329123201320737513', -3); # differ: doris : -329123201320738000, presto : -329123201320738000
-- SELECT round(DECIMAL '329123201320739513', -3); # differ: doris : 329123201320740000, presto : 329123201320740000
-- SELECT round(DECIMAL '-329123201320739513', -3); # differ: doris : -329123201320740000, presto : -329123201320740000
-- SELECT round(DECIMAL '999999999999999999', -3); # differ: doris : 1000000000000000000, presto : 1000000000000000000
-- SELECT round(DECIMAL '-999999999999999999', -3); # differ: doris : -1000000000000000000, presto : -1000000000000000000
SELECT round(CAST(NULL as DOUBLE), CAST(NULL as INTEGER));
SELECT round(-3.0E0, CAST(NULL as INTEGER));
SELECT round(CAST(NULL as DOUBLE), 1);
SELECT round(CAST(NULL as DECIMAL(1,0)), CAST(NULL as INTEGER));
SELECT round(DECIMAL '-3.0', CAST(NULL as INTEGER));
SELECT round(CAST(NULL as DECIMAL(1,0)), 1);
SELECT round(CAST(NULL as DECIMAL(17,2)), 1);
SELECT round(CAST(NULL as DECIMAL(20,2)), 1);
-- SELECT round(nan(), 2); # error: errCode = 2, detailMessage = Can not found function 'nan'
-- SELECT round(1.0E0 / 0, 2); # differ: doris : None, presto : Infinity
-- SELECT round(-1.0E0 / 0, 2); # differ: doris : None, presto : -Infinity
SELECT sign(CAST(NULL as TINYINT));
SELECT sign(CAST(NULL as SMALLINT));
SELECT sign(CAST(NULL as INTEGER));
SELECT sign(CAST(NULL as BIGINT));
SELECT sign(CAST(NULL as DOUBLE));
SELECT sign(CAST(NULL as DECIMAL(2,1)));
SELECT sign(CAST(NULL as DECIMAL(38,0)));
-- SELECT sign(DOUBLE '+Infinity'); # differ: doris : None, presto : 1.0
-- SELECT sign(DOUBLE '-Infinity'); # differ: doris : None, presto : -1.0
-- SELECT sign(DECIMAL '0'); # differ: doris : 0, presto : 0
-- SELECT sign(DECIMAL '123'); # differ: doris : 1, presto : 1
-- SELECT sign(DECIMAL '-123'); # differ: doris : -1, presto : -1
-- SELECT sign(DECIMAL '123.000000000000000'); # differ: doris : 1, presto : 1
-- SELECT sign(DECIMAL '-123.000000000000000'); # differ: doris : -1, presto : -1
-- SELECT sign(DECIMAL '0.000000000000000000'); # differ: doris : 0, presto : 0
-- SELECT sign(DECIMAL '1230.000000000000000'); # differ: doris : 1, presto : 1
-- SELECT sign(DECIMAL '-1230.000000000000000'); # differ: doris : -1, presto : -1
SELECT greatest(TINYINT '1', TINYINT '2');
SELECT greatest(TINYINT '-1', TINYINT '-2');
SELECT greatest(TINYINT '5', TINYINT '4', TINYINT '3', TINYINT '2', TINYINT '1', TINYINT '2', TINYINT '3', TINYINT '4', TINYINT '1', TINYINT '5');
SELECT greatest(TINYINT '-1');
SELECT greatest(TINYINT '5', TINYINT '4', CAST(NULL AS TINYINT), TINYINT '3');
SELECT greatest(SMALLINT '1', SMALLINT '2');
SELECT greatest(SMALLINT '-1', SMALLINT '-2');
SELECT greatest(SMALLINT '5', SMALLINT '4', SMALLINT '3', SMALLINT '2', SMALLINT '1', SMALLINT '2', SMALLINT '3', SMALLINT '4', SMALLINT '1', SMALLINT '5');
SELECT greatest(SMALLINT '-1');
SELECT greatest(SMALLINT '5', SMALLINT '4', CAST(NULL AS SMALLINT), SMALLINT '3');
SELECT greatest(1, 2);
SELECT greatest(-1, -2);
SELECT greatest(5, 4, 3, 2, 1, 2, 3, 4, 1, 5);
SELECT greatest(-1);
SELECT greatest(5, 4, CAST(NULL AS INTEGER), 3);
SELECT greatest(10000000000, 20000000000);
SELECT greatest(-10000000000, -20000000000);
SELECT greatest(5000000000, 4, 3, 2, 1000000000, 2, 3, 4, 1, 5000000000);
SELECT greatest(-10000000000);
SELECT greatest(5000000000, 4000000000, CAST(NULL as BIGINT), 3000000000);
-- SELECT greatest(1.5E0, 2.3E0); # differ: doris : 2.3, presto : 2.3
SELECT greatest(-1.5E0, -2.3E0);
-- SELECT greatest(-1.5E0, -2.3E0, -5/3); # differ: doris : -1.500000000000000, presto : -1.0
-- SELECT greatest(1.5E0, -infinity(), infinity()); # error: errCode = 2, detailMessage = Can not found function 'infinity'
SELECT greatest(5, 4, CAST(NULL as DOUBLE), 3);
-- SELECT greatest(NaN(), 5, 4, 3); # error: errCode = 2, detailMessage = Can not found function 'NaN'
-- SELECT greatest(5, 4, NaN(), 3); # error: errCode = 2, detailMessage = Can not found function 'NaN'
-- SELECT greatest(5, 4, 3, NaN()); # error: errCode = 2, detailMessage = Can not found function 'NaN'
-- SELECT greatest(NaN()); # error: errCode = 2, detailMessage = Can not found function 'NaN'
-- SELECT greatest(NaN(), NaN(), NaN()); # error: errCode = 2, detailMessage = Can not found function 'NaN'
SELECT greatest(REAL '1.5', REAL '2.3');
SELECT greatest(REAL '-1.5', REAL '-2.3');
-- SELECT greatest(REAL '-1.5', REAL '-2.3', CAST(-5/3 AS REAL)); # differ: doris : -1.5, presto : -1.0
-- SELECT greatest(REAL '1.5', CAST(infinity() AS REAL)); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT greatest(REAL '1.5', CAST(infinity() AS REAL));	                ^	Encountered: REAL	Expected: REAL is keyword, maybe `REAL`	
SELECT greatest(REAL '5', REAL '4', CAST(NULL as REAL), REAL '3');
-- SELECT greatest(CAST(NaN() as REAL), REAL '5', REAL '4', REAL '3'); # error: errCode = 2, detailMessage = Syntax error in line 1:	...test(CAST(NaN() as REAL), REAL '5', REAL '4', REAL '3');	                             ^	Encountered: REAL	Expected: REAL is keyword, maybe `REAL`	
-- SELECT greatest(REAL '5', REAL '4', CAST(NaN() as REAL), REAL '3'); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT greatest(REAL '5', REAL '4', CAST(NaN...	                ^	Encountered: REAL	Expected: REAL is keyword, maybe `REAL`	
-- SELECT greatest(REAL '5', REAL '4', REAL '3', CAST(NaN() as REAL)); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT greatest(REAL '5', REAL '4', REAL '3'...	                ^	Encountered: REAL	Expected: REAL is keyword, maybe `REAL`	
-- SELECT greatest(CAST(NaN() as REAL)); # error: errCode = 2, detailMessage = 	no viable alternative at input 'REAL'(line 1, pos 30)	
-- SELECT greatest(CAST(NaN() as REAL), CAST(NaN() as REAL), CAST(NaN() as REAL)); # error: errCode = 2, detailMessage = 	no viable alternative at input 'REAL'(line 1, pos 30)	
-- SELECT greatest(1.0, 2.0); # differ: doris : 2.0, presto : 2.0
-- SELECT greatest(1.0, -2.0); # differ: doris : 1.0, presto : 1.0
-- SELECT greatest(1.0, 1.1, 1.2, 1.3); # differ: doris : 1.3, presto : 1.3
SELECT greatest(1, 20000000000);
SELECT greatest(1, BIGINT '2');
SELECT greatest(1.0E0, 2);
SELECT greatest(1, 2.0E0);
SELECT greatest(1.0E0, 2);
SELECT greatest(5.0E0, 4, CAST(NULL as DOUBLE), 3);
SELECT greatest(5.0E0, 4, CAST(NULL as BIGINT), 3);
SELECT greatest(1.0, 2.0E0);
-- SELECT greatest(5, 4, 3.0, 2); # differ: doris : 5.0, presto : 5.0
SELECT greatest(1E0);
-- SELECT greatest(rand()); # differ: doris : 0.6645962576820839, presto : 0.629298214802383
-- SELECT greatest(ROW(1.5E0), ROW(2.3E0)); # error: errCode = 2, detailMessage = can not cast from origin type STRUCT<StructField ( name=col1, dataType=DECIMALV3(2, 1), nullable=true )> to target type=VARCHAR(65533)
-- SELECT greatest(ROW(-1.5E0), ROW(-2.3E0)); # error: errCode = 2, detailMessage = can not cast from origin type STRUCT<StructField ( name=col1, dataType=DECIMALV3(2, 1), nullable=true )> to target type=VARCHAR(65533)
-- SELECT greatest(ROW(-1.5E0), ROW(-2.3E0), ROW(-5/3)); # error: errCode = 2, detailMessage = can not cast from origin type STRUCT<StructField ( name=col1, dataType=DECIMALV3(2, 1), nullable=true )> to target type=VARCHAR(65533)
-- SELECT greatest(ROW(1.5E0), ROW(-infinity()), ROW(infinity())); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT greatest(ROW(1.5E0), ROW(-infinity())...	                ^	Encountered: ROW	Expected: ROW is keyword, maybe `ROW`	
-- SELECT greatest(ROW(5), ROW(4), CAST(NULL as ROW(DOUBLE)), ROW(3)); # error: errCode = 2, detailMessage = Syntax error in line 1:	...ST(NULL AS STRUCT<`DOUBLE`>), STRUCT(3))	                             ^	Encountered: >	Expected	
-- SELECT greatest(ROW(NaN()), ROW(5), ROW(4), ROW(3)); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT greatest(ROW(NaN()), ROW(5), ROW(4), ROW(3));	                ^	Encountered: ROW	Expected: ROW is keyword, maybe `ROW`	
-- SELECT greatest(ROW(5), ROW(4), ROW(NaN()), ROW(3)); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT greatest(ROW(5), ROW(4), ROW(NaN()), ROW(3));	                ^	Encountered: ROW	Expected: ROW is keyword, maybe `ROW`	
-- SELECT greatest(ROW(5), ROW(4), ROW(3), ROW(NaN())); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT greatest(ROW(5), ROW(4), ROW(3), ROW(NaN()));	                ^	Encountered: ROW	Expected: ROW is keyword, maybe `ROW`	
-- SELECT greatest(ROW(NaN())); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT greatest(ROW(NaN()));	                ^	Encountered: ROW	Expected: ROW is keyword, maybe `ROW`	
-- SELECT greatest(ROW(NaN()), ROW(NaN()), ROW(NaN())); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT greatest(ROW(NaN()), ROW(NaN()), ROW(NaN()));	                ^	Encountered: ROW	Expected: ROW is keyword, maybe `ROW`	
-- SELECT greatest(ROW(REAL '1.5'), ROW(REAL '2.3')); # error: errCode = 2, detailMessage = can not cast from origin type STRUCT<StructField ( name=col1, dataType=FLOAT, nullable=true )> to target type=VARCHAR(65533)
-- SELECT greatest(ROW(REAL '-1.5'), ROW(REAL '-2.3')); # error: errCode = 2, detailMessage = can not cast from origin type STRUCT<StructField ( name=col1, dataType=FLOAT, nullable=true )> to target type=VARCHAR(65533)
-- SELECT greatest(ROW(REAL '-1.5'), ROW(REAL '-2.3'), ROW(CAST(-5/3 AS REAL))); # error: errCode = 2, detailMessage = can not cast from origin type STRUCT<StructField ( name=col1, dataType=FLOAT, nullable=true )> to target type=VARCHAR(65533)
-- SELECT greatest(ROW(REAL '1.5'), ROW(CAST(infinity() AS REAL))); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT greatest(ROW(REAL '1.5'), ROW(CAST(in...	                ^	Encountered: ROW	Expected: ROW is keyword, maybe `ROW`	
-- SELECT greatest(ROW(REAL '5'), ROW(REAL '4'), CAST(NULL as ROW(REAL)), ROW(REAL '3')); # error: errCode = 2, detailMessage = Syntax error in line 1:	...CAST(NULL AS STRUCT<`REAL`>), STRUCT(CAST('3' AS FLOAT)))	                             ^	Encountered: >	Expected	
-- SELECT greatest(ROW(CAST(NaN() as REAL)), ROW(REAL '5'), ROW(REAL '4'), ROW(REAL '3')); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT greatest(ROW(CAST(NaN() as REAL)), RO...	                ^	Encountered: ROW	Expected: ROW is keyword, maybe `ROW`	
-- SELECT greatest(ROW(REAL '5'), ROW(REAL '4'), ROW(CAST(NaN() as REAL)), ROW(REAL '3')); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT greatest(ROW(REAL '5'), ROW(REAL '4')...	                ^	Encountered: ROW	Expected: ROW is keyword, maybe `ROW`	
-- SELECT greatest(ROW(REAL '5'), ROW(REAL '4'), ROW(REAL '3'), ROW(CAST(NaN() as REAL))); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT greatest(ROW(REAL '5'), ROW(REAL '4')...	                ^	Encountered: ROW	Expected: ROW is keyword, maybe `ROW`	
-- SELECT greatest(ROW(CAST(NaN() as REAL))); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT greatest(ROW(CAST(NaN() as REAL)));	                ^	Encountered: ROW	Expected: ROW is keyword, maybe `ROW`	
-- SELECT greatest(ROW(CAST(NaN() as REAL)), ROW(CAST(NaN() as REAL)), ROW(CAST(NaN() as REAL))); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT greatest(ROW(CAST(NaN() as REAL)), RO...	                ^	Encountered: ROW	Expected: ROW is keyword, maybe `ROW`	
-- SELECT greatest(ARRAY[1.5E0], ARRAY[2.3E0]); # error: errCode = 2, detailMessage = can not cast from origin type ARRAY<DECIMALV3(2, 1)> to target type=VARCHAR(65533)
-- SELECT greatest(ARRAY[-1.5E0], ARRAY[-2.3E0]); # error: errCode = 2, detailMessage = can not cast from origin type ARRAY<DECIMALV3(2, 1)> to target type=VARCHAR(65533)
-- SELECT greatest(ARRAY[-1.5E0], ARRAY[-2.3E0], ARRAY[-5/3]); # error: errCode = 2, detailMessage = can not cast from origin type ARRAY<DECIMALV3(2, 1)> to target type=VARCHAR(65533)
-- SELECT greatest(ARRAY[1.5E0], ARRAY[-infinity()], ARRAY[infinity()]); # error: errCode = 2, detailMessage = Unknown column 'ARRAY' in 'table list' in PROJECT clause
-- SELECT greatest(ARRAY[5], ARRAY[4], CAST(NULL as ARRAY(DOUBLE)), ARRAY[3]); # error: errCode = 2, detailMessage = can not cast from origin type ARRAY<TINYINT> to target type=VARCHAR(65533)
-- SELECT greatest(ARRAY[NaN()], ARRAY[5], ARRAY[4], ARRAY[3]); # error: errCode = 2, detailMessage = Unknown column 'ARRAY' in 'table list' in PROJECT clause
-- SELECT greatest(ARRAY[5], ARRAY[4], ARRAY[NaN()], ARRAY[3]); # error: errCode = 2, detailMessage = Unknown column 'ARRAY' in 'table list' in PROJECT clause
-- SELECT greatest(ARRAY[5], ARRAY[4], ARRAY[3], ARRAY[NaN()]); # error: errCode = 2, detailMessage = Unknown column 'ARRAY' in 'table list' in PROJECT clause
-- SELECT greatest(ARRAY[NaN()]); # error: errCode = 2, detailMessage = Unknown column 'ARRAY' in 'table list' in PROJECT clause
-- SELECT greatest(ARRAY[NaN()], ARRAY[NaN()], ARRAY[NaN()]); # error: errCode = 2, detailMessage = Unknown column 'ARRAY' in 'table list' in PROJECT clause
-- SELECT greatest(ARRAY[REAL '1.5'], ARRAY[REAL '2.3']); # error: errCode = 2, detailMessage = can not cast from origin type ARRAY<FLOAT> to target type=VARCHAR(65533)
-- SELECT greatest(ARRAY[REAL '-1.5'], ARRAY[REAL '-2.3']); # error: errCode = 2, detailMessage = can not cast from origin type ARRAY<FLOAT> to target type=VARCHAR(65533)
-- SELECT greatest(ARRAY[REAL '-1.5'], ARRAY[REAL '-2.3'], ARRAY[CAST(-5/3 AS REAL)]); # error: errCode = 2, detailMessage = can not cast from origin type ARRAY<FLOAT> to target type=VARCHAR(65533)
-- SELECT greatest(ARRAY[REAL '1.5'], ARRAY[CAST(infinity() AS REAL)]); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT greatest(ARRAY[REAL '1.5'], ARRAY[CAST(infi...	                      ^	Encountered: REAL	Expected: REAL is keyword, maybe `REAL`	
-- SELECT greatest(ARRAY[REAL '5'], ARRAY[REAL '4'], CAST(NULL as ARRAY(REAL)), ARRAY[REAL '3']); # error: errCode = 2, detailMessage = can not cast from origin type ARRAY<FLOAT> to target type=VARCHAR(65533)
-- SELECT greatest(ARRAY[CAST(NaN() as REAL)], ARRAY[REAL '5'], ARRAY[REAL '4'], ARRAY[REAL '3']); # error: errCode = 2, detailMessage = Syntax error in line 1:	...ST(NaN() as REAL)], ARRAY[REAL '5'], ARRAY[REAL '4'], ...	                             ^	Encountered: REAL	Expected: REAL is keyword, maybe `REAL`	
-- SELECT greatest(ARRAY[REAL '5'], ARRAY[REAL '4'], ARRAY[CAST(NaN() as REAL)], ARRAY[REAL '3']); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT greatest(ARRAY[REAL '5'], ARRAY[REAL '4'], ...	                      ^	Encountered: REAL	Expected: REAL is keyword, maybe `REAL`	
-- SELECT greatest(ARRAY[REAL '5'], ARRAY[REAL '4'], ARRAY[REAL '3'], ARRAY[CAST(NaN() as REAL)]); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT greatest(ARRAY[REAL '5'], ARRAY[REAL '4'], ...	                      ^	Encountered: REAL	Expected: REAL is keyword, maybe `REAL`	
-- SELECT greatest(ARRAY[CAST(NaN() as REAL)]); # error: errCode = 2, detailMessage = 	no viable alternative at input '[CAST(NaN() as REAL'(line 1, pos 36)	
-- SELECT greatest(ARRAY[CAST(NaN() as REAL)], ARRAY[CAST(NaN() as REAL)], ARRAY[CAST(NaN() as REAL)]); # error: errCode = 2, detailMessage = 	no viable alternative at input '[CAST(NaN() as REAL'(line 1, pos 36)	
SELECT least(TINYINT '1', TINYINT '2');
SELECT least(TINYINT '-1', TINYINT '-2');
SELECT least(TINYINT '5', TINYINT '4', TINYINT '3', TINYINT '2', TINYINT '1', TINYINT '2', TINYINT '3', TINYINT '4', TINYINT '1', TINYINT '5');
SELECT least(TINYINT '-1');
SELECT least(TINYINT '5', TINYINT '4', CAST(NULL AS TINYINT), TINYINT '3');
SELECT least(SMALLINT '1', SMALLINT '2');
SELECT least(SMALLINT '-1', SMALLINT '-2');
SELECT least(SMALLINT '5', SMALLINT '4', SMALLINT '3', SMALLINT '2', SMALLINT '1', SMALLINT '2', SMALLINT '3', SMALLINT '4', SMALLINT '1', SMALLINT '5');
SELECT least(SMALLINT '-1');
SELECT least(SMALLINT '5', SMALLINT '4', CAST(NULL AS SMALLINT), SMALLINT '3');
SELECT least(1, 2);
SELECT least(-1, -2);
SELECT least(5, 4, 3, 2, 1, 2, 3, 4, 1, 5);
SELECT least(-1);
SELECT least(5, 4, CAST(NULL AS INTEGER), 3);
SELECT least(10000000000, 20000000000);
SELECT least(-10000000000, -20000000000);
SELECT least(50000000000, 40000000000, 30000000000, 20000000000, 50000000000);
SELECT least(-10000000000);
SELECT least(500000000, 400000000, CAST(NULL as BIGINT), 300000000);
SELECT least(1.5E0, 2.3E0);
-- SELECT least(-1.5E0, -2.3E0); # differ: doris : -2.3, presto : -2.3
-- SELECT least(-1.5E0, -2.3E0, -5/3); # differ: doris : -2.300000000000000, presto : -2.3
-- SELECT least(1.5E0, -infinity(), infinity()); # error: errCode = 2, detailMessage = Can not found function 'infinity'
SELECT least(5, 4, CAST(NULL as DOUBLE), 3);
-- SELECT least(NaN(), 5, 4, 3); # error: errCode = 2, detailMessage = Can not found function 'NaN'
-- SELECT least(5, 4, NaN(), 3); # error: errCode = 2, detailMessage = Can not found function 'NaN'
-- SELECT least(5, 4, 3, NaN()); # error: errCode = 2, detailMessage = Can not found function 'NaN'
-- SELECT least(NaN()); # error: errCode = 2, detailMessage = Can not found function 'NaN'
-- SELECT least(NaN(), NaN(), NaN()); # error: errCode = 2, detailMessage = Can not found function 'NaN'
SELECT least(REAL '1.5', REAL '2.3');
SELECT least(REAL '-1.5', REAL '-2.3');
SELECT least(REAL '-1.5', REAL '-2.3', CAST(-5/3 AS REAL));
-- SELECT least(REAL '1.5', CAST(-infinity() AS REAL)); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT least(REAL '1.5', CAST(-infinity() AS REAL));	             ^	Encountered: REAL	Expected: REAL is keyword, maybe `REAL`	
SELECT least(REAL '5', REAL '4', CAST(NULL as REAL), REAL '3');
-- SELECT least(CAST(NaN() as REAL), REAL '5', REAL '4', REAL '3'); # error: errCode = 2, detailMessage = Syntax error in line 1:	...east(CAST(NaN() as REAL), REAL '5', REAL '4', REAL '3');	                             ^	Encountered: REAL	Expected: REAL is keyword, maybe `REAL`	
-- SELECT least(REAL '5', REAL '4', CAST(NaN() as REAL), REAL '3'); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT least(REAL '5', REAL '4', CAST(NaN...	             ^	Encountered: REAL	Expected: REAL is keyword, maybe `REAL`	
-- SELECT least(REAL '5', REAL '4', REAL '3', CAST(NaN() as REAL)); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT least(REAL '5', REAL '4', REAL '3'...	             ^	Encountered: REAL	Expected: REAL is keyword, maybe `REAL`	
-- SELECT least(CAST(NaN() as REAL)); # error: errCode = 2, detailMessage = 	no viable alternative at input 'REAL'(line 1, pos 27)	
-- SELECT least(CAST(NaN() as REAL), CAST(NaN() as REAL), CAST(NaN() as REAL)); # error: errCode = 2, detailMessage = 	no viable alternative at input 'REAL'(line 1, pos 27)	
-- SELECT least(1.0, 2.0); # differ: doris : 1.0, presto : 1.0
-- SELECT least(1.0, -2.0); # differ: doris : -2.0, presto : -2.0
-- SELECT least(1.0, 1.1, 1.2, 1.3); # differ: doris : 1.0, presto : 1.0
SELECT least(1, 20000000000);
SELECT least(1, BIGINT '2');
SELECT least(1.0E0, 2);
SELECT least(1, 2.0E0);
SELECT least(1.0E0, 2);
SELECT least(5.0E0, 4, CAST(NULL as DOUBLE), 3);
SELECT least(5.0E0, 4, CAST(NULL as BIGINT), 3);
SELECT least(1.0, 2.0E0);
-- SELECT least(5, 4, 3.0, 2); # differ: doris : 2.0, presto : 2.0
-- SELECT least(ROW(1.5E0), ROW(2.3E0)); # error: errCode = 2, detailMessage = can not cast from origin type STRUCT<StructField ( name=col1, dataType=DECIMALV3(2, 1), nullable=true )> to target type=VARCHAR(65533)
-- SELECT least(ROW(-1.5E0), ROW(-2.3E0)); # error: errCode = 2, detailMessage = can not cast from origin type STRUCT<StructField ( name=col1, dataType=DECIMALV3(2, 1), nullable=true )> to target type=VARCHAR(65533)
-- SELECT least(ROW(-1.5E0), ROW(-2.3E0), ROW(-5/3)); # error: errCode = 2, detailMessage = can not cast from origin type STRUCT<StructField ( name=col1, dataType=DECIMALV3(2, 1), nullable=true )> to target type=VARCHAR(65533)
-- SELECT least(ROW(1.5E0), ROW(-infinity()), ROW(infinity())); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT least(ROW(1.5E0), ROW(-infinity()), ROW(infinity()));	             ^	Encountered: ROW	Expected: ROW is keyword, maybe `ROW`	
-- SELECT least(ROW(5), ROW(4), CAST(NULL as ROW(DOUBLE)), ROW(3)); # error: errCode = 2, detailMessage = Syntax error in line 1:	...ST(NULL AS STRUCT<`DOUBLE`>), STRUCT(3))	                             ^	Encountered: >	Expected	
-- SELECT least(ROW(NaN()), ROW(5), ROW(4), ROW(3)); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT least(ROW(NaN()), ROW(5), ROW(4), ROW(3));	             ^	Encountered: ROW	Expected: ROW is keyword, maybe `ROW`	
-- SELECT least(ROW(5), ROW(4), ROW(NaN()), ROW(3)); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT least(ROW(5), ROW(4), ROW(NaN()), ROW(3));	             ^	Encountered: ROW	Expected: ROW is keyword, maybe `ROW`	
-- SELECT least(ROW(5), ROW(4), ROW(3), ROW(NaN())); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT least(ROW(5), ROW(4), ROW(3), ROW(NaN()));	             ^	Encountered: ROW	Expected: ROW is keyword, maybe `ROW`	
-- SELECT least(ROW(NaN())); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT least(ROW(NaN()));	             ^	Encountered: ROW	Expected: ROW is keyword, maybe `ROW`	
-- SELECT least(ROW(NaN()), ROW(NaN()), ROW(NaN())); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT least(ROW(NaN()), ROW(NaN()), ROW(NaN()));	             ^	Encountered: ROW	Expected: ROW is keyword, maybe `ROW`	
-- SELECT least(ROW(REAL '1.5'), ROW(REAL '2.3')); # error: errCode = 2, detailMessage = can not cast from origin type STRUCT<StructField ( name=col1, dataType=FLOAT, nullable=true )> to target type=VARCHAR(65533)
-- SELECT least(ROW(REAL '-1.5'), ROW(REAL '-2.3')); # error: errCode = 2, detailMessage = can not cast from origin type STRUCT<StructField ( name=col1, dataType=FLOAT, nullable=true )> to target type=VARCHAR(65533)
-- SELECT least(ROW(REAL '-1.5'), ROW(REAL '-2.3'), ROW(CAST(-5/3 AS REAL))); # error: errCode = 2, detailMessage = can not cast from origin type STRUCT<StructField ( name=col1, dataType=FLOAT, nullable=true )> to target type=VARCHAR(65533)
-- SELECT least(ROW(REAL '1.5'), ROW(CAST(-infinity() AS REAL)), ROW(CAST(infinity() AS REAL))); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT least(ROW(REAL '1.5'), ROW(CAST(-i...	             ^	Encountered: ROW	Expected: ROW is keyword, maybe `ROW`	
-- SELECT least(ROW(REAL '5'), ROW(REAL '4'), CAST(NULL as ROW(REAL)), ROW(REAL '3')); # error: errCode = 2, detailMessage = Syntax error in line 1:	...CAST(NULL AS STRUCT<`REAL`>), STRUCT(CAST('3' AS FLOAT)))	                             ^	Encountered: >	Expected	
-- SELECT least(ROW(CAST(NaN() as REAL)), ROW(REAL '5'), ROW(REAL '4'), ROW(REAL '3')); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT least(ROW(CAST(NaN() as REAL)), RO...	             ^	Encountered: ROW	Expected: ROW is keyword, maybe `ROW`	
-- SELECT least(ROW(REAL '5'), ROW(REAL '4'), ROW(CAST(NaN() as REAL)), ROW(REAL '3')); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT least(ROW(REAL '5'), ROW(REAL '4')...	             ^	Encountered: ROW	Expected: ROW is keyword, maybe `ROW`	
-- SELECT least(ROW(REAL '5'), ROW(REAL '4'), ROW(REAL '3'), ROW(CAST(NaN() as REAL))); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT least(ROW(REAL '5'), ROW(REAL '4')...	             ^	Encountered: ROW	Expected: ROW is keyword, maybe `ROW`	
-- SELECT least(ROW(CAST(NaN() as REAL))); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT least(ROW(CAST(NaN() as REAL)));	             ^	Encountered: ROW	Expected: ROW is keyword, maybe `ROW`	
-- SELECT least(ROW(CAST(NaN() as REAL)), ROW(CAST(NaN() as REAL)), ROW(CAST(NaN() as REAL))); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT least(ROW(CAST(NaN() as REAL)), RO...	             ^	Encountered: ROW	Expected: ROW is keyword, maybe `ROW`	
-- SELECT least(ARRAY[1.5E0], ARRAY[2.3E0]); # error: errCode = 2, detailMessage = can not cast from origin type ARRAY<DECIMALV3(2, 1)> to target type=VARCHAR(65533)
-- SELECT least(ARRAY[-1.5E0], ARRAY[-2.3E0]); # error: errCode = 2, detailMessage = can not cast from origin type ARRAY<DECIMALV3(2, 1)> to target type=VARCHAR(65533)
-- SELECT least(ARRAY[-1.5E0], ARRAY[-2.3E0], ARRAY[-5/3]); # error: errCode = 2, detailMessage = can not cast from origin type ARRAY<DECIMALV3(2, 1)> to target type=VARCHAR(65533)
-- SELECT least(ARRAY[1.5E0], ARRAY[-infinity()], ARRAY[infinity()]); # error: errCode = 2, detailMessage = Unknown column 'ARRAY' in 'table list' in PROJECT clause
-- SELECT least(ARRAY[5], ARRAY[4], CAST(NULL as ARRAY(DOUBLE)), ARRAY[3]); # error: errCode = 2, detailMessage = can not cast from origin type ARRAY<TINYINT> to target type=VARCHAR(65533)
-- SELECT least(ARRAY[NaN()], ARRAY[5], ARRAY[4], ARRAY[3]); # error: errCode = 2, detailMessage = Unknown column 'ARRAY' in 'table list' in PROJECT clause
-- SELECT least(ARRAY[5], ARRAY[4], ARRAY[NaN()], ARRAY[3]); # error: errCode = 2, detailMessage = Unknown column 'ARRAY' in 'table list' in PROJECT clause
-- SELECT least(ARRAY[5], ARRAY[4], ARRAY[3], ARRAY[NaN()]); # error: errCode = 2, detailMessage = Unknown column 'ARRAY' in 'table list' in PROJECT clause
-- SELECT least(ARRAY[NaN()]); # error: errCode = 2, detailMessage = Unknown column 'ARRAY' in 'table list' in PROJECT clause
-- SELECT least(ARRAY[NaN()], ARRAY[NaN()], ARRAY[NaN()]); # error: errCode = 2, detailMessage = Unknown column 'ARRAY' in 'table list' in PROJECT clause
-- SELECT least(ARRAY[REAL '1.5'], ARRAY[REAL '2.3']); # error: errCode = 2, detailMessage = can not cast from origin type ARRAY<FLOAT> to target type=VARCHAR(65533)
-- SELECT least(ARRAY[REAL '-1.5'], ARRAY[REAL '-2.3']); # error: errCode = 2, detailMessage = can not cast from origin type ARRAY<FLOAT> to target type=VARCHAR(65533)
-- SELECT least(ARRAY[REAL '-1.5'], ARRAY[REAL '-2.3'], ARRAY[CAST(-5/3 AS REAL)]); # error: errCode = 2, detailMessage = can not cast from origin type ARRAY<FLOAT> to target type=VARCHAR(65533)
-- SELECT least(ARRAY[REAL '1.5'], ARRAY[CAST(-infinity() AS REAL)], ARRAY[CAST(infinity() AS REAL)]); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT least(ARRAY[REAL '1.5'], ARRAY[CAST(-inf...	                   ^	Encountered: REAL	Expected: REAL is keyword, maybe `REAL`	
-- SELECT least(ARRAY[REAL '5'], ARRAY[REAL '4'], CAST(NULL as ARRAY(REAL)), ARRAY[REAL '3']); # error: errCode = 2, detailMessage = can not cast from origin type ARRAY<FLOAT> to target type=VARCHAR(65533)
-- SELECT least(ARRAY[CAST(NaN() as REAL)], ARRAY[REAL '5'], ARRAY[REAL '4'], ARRAY[REAL '3']); # error: errCode = 2, detailMessage = Syntax error in line 1:	...ST(NaN() as REAL)], ARRAY[REAL '5'], ARRAY[REAL '4'], ...	                             ^	Encountered: REAL	Expected: REAL is keyword, maybe `REAL`	
-- SELECT least(ARRAY[REAL '5'], ARRAY[REAL '4'], ARRAY[CAST(NaN() as REAL)], ARRAY[REAL '3']); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT least(ARRAY[REAL '5'], ARRAY[REAL '4'], ...	                   ^	Encountered: REAL	Expected: REAL is keyword, maybe `REAL`	
-- SELECT least(ARRAY[REAL '5'], ARRAY[REAL '4'], ARRAY[REAL '3'], ARRAY[CAST(NaN() as REAL)]); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT least(ARRAY[REAL '5'], ARRAY[REAL '4'], ...	                   ^	Encountered: REAL	Expected: REAL is keyword, maybe `REAL`	
-- SELECT least(ARRAY[CAST(NaN() as REAL)]); # error: errCode = 2, detailMessage = 	no viable alternative at input '[CAST(NaN() as REAL'(line 1, pos 33)	
-- SELECT least(ARRAY[CAST(NaN() as REAL)], ARRAY[CAST(NaN() as REAL)], ARRAY[CAST(NaN() as REAL)]); # error: errCode = 2, detailMessage = 	no viable alternative at input '[CAST(NaN() as REAL'(line 1, pos 33)	
-- SELECT to_base(2147483648, 16); # error: errCode = 2, detailMessage = Can not found function 'to_base'
-- SELECT to_base(255, 2); # error: errCode = 2, detailMessage = Can not found function 'to_base'
-- SELECT to_base(-2147483647, 16); # error: errCode = 2, detailMessage = Can not found function 'to_base'
-- SELECT to_base(NULL, 16); # error: errCode = 2, detailMessage = Can not found function 'to_base'
-- SELECT to_base(-2147483647, NULL); # error: errCode = 2, detailMessage = Can not found function 'to_base'
-- SELECT to_base(NULL, NULL); # error: errCode = 2, detailMessage = Can not found function 'to_base'
-- SELECT from_base('80000000', 16); # error: errCode = 2, detailMessage = Can not found function 'FROM_BASE'
-- SELECT from_base('11111111', 2); # error: errCode = 2, detailMessage = Can not found function 'FROM_BASE'
-- SELECT from_base('-7fffffff', 16); # error: errCode = 2, detailMessage = Can not found function 'FROM_BASE'
-- SELECT from_base('9223372036854775807', 10); # error: errCode = 2, detailMessage = Can not found function 'FROM_BASE'
-- SELECT from_base('-9223372036854775808', 10); # error: errCode = 2, detailMessage = Can not found function 'FROM_BASE'
-- SELECT from_base(NULL, 10); # error: errCode = 2, detailMessage = Can not found function 'FROM_BASE'
-- SELECT from_base('-9223372036854775808', NULL); # error: errCode = 2, detailMessage = Can not found function 'FROM_BASE'
-- SELECT from_base(NULL, NULL); # error: errCode = 2, detailMessage = Can not found function 'FROM_BASE'
SELECT width_bucket(3.14E0, 0, 4, 3);
SELECT width_bucket(2, 0, 4, 3);
-- SELECT width_bucket(infinity(), 0, 4, 3); # error: errCode = 2, detailMessage = Can not found function 'infinity'
SELECT width_bucket(-1, 0, 3.2E0, 4);
-- SELECT width_bucket(3.14E0, 4, 0, 3); # differ: doris : 0, presto : 1
-- SELECT width_bucket(2, 4, 0, 3); # differ: doris : 0, presto : 2
-- SELECT width_bucket(infinity(), 4, 0, 3); # error: errCode = 2, detailMessage = Can not found function 'infinity'
-- SELECT width_bucket(-1, 3.2E0, 0, 4); # differ: doris : 0, presto : 5
-- SELECT width_bucket(3.14E0, array[0.0E0, 2.0E0, 4.0E0]); # error: errCode = 2, detailMessage = Can not found function 'WIDTH_BUCKET' which has 2 arity. Candidate functions are: [WIDTH_BUCKETorg.apache.doris.nereids.trees.expressions.functions.BuiltinFunctionBuilder@3fa5c17d]
-- SELECT width_bucket(infinity(), array[0.0E0, 2.0E0, 4.0E0]); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT width_bucket(infinity(), array[0.0E0, 2.0E0, 4.0E0]);	                                           ^	Encountered: COMMA	Expected: ||	
-- SELECT width_bucket(-1, array[0.0E0, 1.2E0, 3.3E0, 4.5E0]); # error: errCode = 2, detailMessage = Can not found function 'WIDTH_BUCKET' which has 2 arity. Candidate functions are: [WIDTH_BUCKETorg.apache.doris.nereids.trees.expressions.functions.BuiltinFunctionBuilder@3fa5c17d]
-- SELECT width_bucket(3.145E0, array[0.0E0]); # error: errCode = 2, detailMessage = Can not found function 'WIDTH_BUCKET' which has 2 arity. Candidate functions are: [WIDTH_BUCKETorg.apache.doris.nereids.trees.expressions.functions.BuiltinFunctionBuilder@3fa5c17d]
-- SELECT width_bucket(-3.145E0, array[0.0E0]); # error: errCode = 2, detailMessage = Can not found function 'WIDTH_BUCKET' which has 2 arity. Candidate functions are: [WIDTH_BUCKETorg.apache.doris.nereids.trees.expressions.functions.BuiltinFunctionBuilder@3fa5c17d]
-- SELECT width_bucket(1.5E0, array[1.0E0, 2.3E0, 2.0E0]); # error: errCode = 2, detailMessage = Can not found function 'WIDTH_BUCKET' which has 2 arity. Candidate functions are: [WIDTH_BUCKETorg.apache.doris.nereids.trees.expressions.functions.BuiltinFunctionBuilder@3fa5c17d]
-- SELECT cosine_similarity(map(ARRAY['a', 'b'], ARRAY[1.0E0, 2.0E0]), map(ARRAY['c', 'b'], ARRAY[1.0E0, 3.0E0])); # error: errCode = 2, detailMessage = Syntax error in line 1:	...e_similarity(map(ARRAY['a', 'b'], ARRAY[1.0E0, 2.0E0])...	                             ^	Encountered: COMMA	Expected: ||	
-- SELECT cosine_similarity(map(ARRAY['a', 'b', 'c'], ARRAY[1.0E0, 2.0E0, -1.0E0]), map(ARRAY['c', 'b'], ARRAY[1.0E0, 3.0E0])); # error: errCode = 2, detailMessage = Syntax error in line 1:	...e_similarity(map(ARRAY['a', 'b', 'c'], ARRAY[1.0E0, 2....	                             ^	Encountered: COMMA	Expected: ||	
-- SELECT cosine_similarity(map(ARRAY['a', 'b', 'c'], ARRAY[1.0E0, 2.0E0, -1.0E0]), map(ARRAY['d', 'e'], ARRAY[1.0E0, 3.0E0])); # error: errCode = 2, detailMessage = Syntax error in line 1:	...e_similarity(map(ARRAY['a', 'b', 'c'], ARRAY[1.0E0, 2....	                             ^	Encountered: COMMA	Expected: ||	
-- SELECT cosine_similarity(null, map(ARRAY['c', 'b'], ARRAY[1.0E0, 3.0E0])); # error: errCode = 2, detailMessage = Syntax error in line 1:	...larity(null, map(ARRAY['c', 'b'], ARRAY[1.0E0, 3.0E0]));	                             ^	Encountered: COMMA	Expected: ||	
-- SELECT cosine_similarity(map(ARRAY['a', 'b'], ARRAY[1.0E0, null]), map(ARRAY['c', 'b'], ARRAY[1.0E0, 3.0E0])); # error: errCode = 2, detailMessage = Syntax error in line 1:	...e_similarity(map(ARRAY['a', 'b'], ARRAY[1.0E0, null]),...	                             ^	Encountered: COMMA	Expected: ||	
-- SELECT inverse_normal_cdf(0, 1, 0.3); # error: errCode = 2, detailMessage = Can not found function 'inverse_normal_cdf'
-- SELECT inverse_normal_cdf(10, 9, 0.9); # error: errCode = 2, detailMessage = Can not found function 'inverse_normal_cdf'
-- SELECT inverse_normal_cdf(0.5, 0.25, 0.65); # error: errCode = 2, detailMessage = Can not found function 'inverse_normal_cdf'
-- SELECT normal_cdf(0, 1, 1.96); # error: errCode = 2, detailMessage = Can not found function 'normal_cdf'
-- SELECT normal_cdf(10, 9, 10); # error: errCode = 2, detailMessage = Can not found function 'normal_cdf'
-- SELECT normal_cdf(-1.5, 2.1, -7.8); # error: errCode = 2, detailMessage = Can not found function 'normal_cdf'
-- SELECT normal_cdf(0, 1, infinity()); # error: errCode = 2, detailMessage = Can not found function 'infinity'
-- SELECT normal_cdf(0, 1, -infinity()); # error: errCode = 2, detailMessage = Can not found function 'infinity'
-- SELECT normal_cdf(infinity(), 1, 0); # error: errCode = 2, detailMessage = Can not found function 'infinity'
-- SELECT normal_cdf(-infinity(), 1, 0); # error: errCode = 2, detailMessage = Can not found function 'infinity'
-- SELECT normal_cdf(0, infinity(), 0); # error: errCode = 2, detailMessage = Can not found function 'infinity'
-- SELECT normal_cdf(nan(), 1, 0); # error: errCode = 2, detailMessage = Can not found function 'nan'
-- SELECT normal_cdf(0, 1, nan()); # error: errCode = 2, detailMessage = Can not found function 'nan'
-- SELECT inverse_beta_cdf(3, 3.6, 0.0); # error: errCode = 2, detailMessage = Can not found function 'inverse_beta_cdf'
-- SELECT inverse_beta_cdf(3, 3.6, 1.0); # error: errCode = 2, detailMessage = Can not found function 'inverse_beta_cdf'
-- SELECT inverse_beta_cdf(3, 3.6, 0.3); # error: errCode = 2, detailMessage = Can not found function 'inverse_beta_cdf'
-- SELECT inverse_beta_cdf(3, 3.6, 0.95); # error: errCode = 2, detailMessage = Can not found function 'inverse_beta_cdf'
-- SELECT beta_cdf(3, 3.6, 0.0); # error: errCode = 2, detailMessage = Can not found function 'beta_cdf'
-- SELECT beta_cdf(3, 3.6, 1.0); # error: errCode = 2, detailMessage = Can not found function 'beta_cdf'
-- SELECT beta_cdf(3, 3.6, 0.3); # error: errCode = 2, detailMessage = Can not found function 'beta_cdf'
-- SELECT beta_cdf(3, 3.6, 0.9); # error: errCode = 2, detailMessage = Can not found function 'beta_cdf'
-- SELECT wilson_interval_lower(1250, 1310, 1.96e0); # error: errCode = 2, detailMessage = Can not found function 'wilson_interval_lower'
-- SELECT wilson_interval_upper(1250, 1310, 1.96e0) # error: errCode = 2, detailMessage = Can not found function 'wilson_interval_upper'
