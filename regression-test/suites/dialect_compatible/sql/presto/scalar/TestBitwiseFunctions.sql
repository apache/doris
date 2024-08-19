set sql_dialect='presto';
set enable_fallback_to_original_planner=false;
set debug_skip_fold_constant=false;
SELECT bit_count(0, 64);
SELECT bit_count(7, 64);
SELECT bit_count(24, 64);
-- SELECT bit_count(-8, 64); # error: errCode = 2, detailMessage = Can not found function 'bit_count' which has 2 arity. Candidate functions are: [bit_countorg.apache.doris.nereids.trees.expressions.functions.BuiltinFunctionBuilder@10c6d615]
SELECT bit_count(0, 32);
SELECT bit_count(7, 32);
SELECT bit_count(24, 32);
-- SELECT bit_count(-8, 32); # error: errCode = 2, detailMessage = Can not found function 'bit_count' which has 2 arity. Candidate functions are: [bit_countorg.apache.doris.nereids.trees.expressions.functions.BuiltinFunctionBuilder@10c6d615]
SELECT bit_count(1152921504598458367, 62);
-- SELECT bit_count(-1, 62); # error: errCode = 2, detailMessage = Can not found function 'bit_count' which has 2 arity. Candidate functions are: [bit_countorg.apache.doris.nereids.trees.expressions.functions.BuiltinFunctionBuilder@10c6d615]
SELECT bit_count(33554132, 26);
-- SELECT bit_count(-1, 26); # error: errCode = 2, detailMessage = Can not found function 'bit_count' which has 2 arity. Candidate functions are: [bit_countorg.apache.doris.nereids.trees.expressions.functions.BuiltinFunctionBuilder@10c6d615]
SELECT bitwise_not(0);
SELECT bitwise_not(-1);
SELECT bitwise_not(8);
SELECT bitwise_not(-8);
-- SELECT bitwise_not(bitwise_not, bitwise_and, 0, -1); # error: errCode = 2, detailMessage = Unknown column 'bitwise_not' in 'table list' in PROJECT clause
SELECT bitwise_and(3, 8);
SELECT bitwise_and(-4, 12);
SELECT bitwise_and(60, 21);
SELECT bitwise_or(0, -1);
SELECT bitwise_or(3, 8);
SELECT bitwise_or(-4, 12);
SELECT bitwise_or(60, 21);
SELECT bitwise_xor(0, -1);
SELECT bitwise_xor(3, 8);
SELECT bitwise_xor(-4, 12);
SELECT bitwise_xor(60, 21);
SELECT bitwise_left_shift(TINYINT'7', 2);
SELECT bitwise_left_shift(TINYINT '-7', 2);
-- SELECT bitwise_left_shift(TINYINT '1', 7); # differ: doris : 128, presto : -128
-- SELECT bitwise_left_shift(TINYINT '-128', 1); # differ: doris : -256, presto : 0
-- SELECT bitwise_left_shift(TINYINT '-65', 1); # differ: doris : -130, presto : 126
SELECT bitwise_left_shift(TINYINT '-7', 64);
SELECT bitwise_left_shift(TINYINT '-128', 0);
SELECT bitwise_left_shift(SMALLINT '7', 2);
SELECT bitwise_left_shift(SMALLINT '-7', 2);
SELECT bitwise_left_shift(SMALLINT '1', 7);
-- SELECT bitwise_left_shift(SMALLINT '-32768', 1); # differ: doris : -65536, presto : 0
SELECT bitwise_left_shift(SMALLINT '-65', 1);
SELECT bitwise_left_shift(SMALLINT '-7', 64);
SELECT bitwise_left_shift(SMALLINT '-32768', 0);
SELECT bitwise_left_shift(INTEGER '7', 2);
SELECT bitwise_left_shift(INTEGER '-7', 2);
SELECT bitwise_left_shift(INTEGER '1', 7);
-- SELECT bitwise_left_shift(INTEGER '-2147483648', 1); # differ: doris : -4294967296, presto : 0
SELECT bitwise_left_shift(INTEGER '-65', 1);
SELECT bitwise_left_shift(INTEGER '-7', 64);
SELECT bitwise_left_shift(INTEGER '-2147483648', 0);
SELECT bitwise_left_shift(BIGINT '7', 2);
SELECT bitwise_left_shift(BIGINT '-7', 2);
SELECT bitwise_left_shift(BIGINT '-7', 64);
SELECT bitwise_right_shift(TINYINT '7', 2);
-- SELECT bitwise_right_shift(TINYINT '-7', 2); # differ: doris : 4611686018427387902, presto : 62
SELECT bitwise_right_shift(TINYINT '-7', 64);
SELECT bitwise_right_shift(TINYINT '-128', 0);
SELECT bitwise_right_shift(SMALLINT '7', 2);
-- SELECT bitwise_right_shift(SMALLINT '-7', 2); # differ: doris : 4611686018427387902, presto : 16382
SELECT bitwise_right_shift(SMALLINT '-7', 64);
SELECT bitwise_right_shift(SMALLINT '-32768', 0);
SELECT bitwise_right_shift(INTEGER '7', 2);
-- SELECT bitwise_right_shift(INTEGER '-7', 2); # differ: doris : 4611686018427387902, presto : 1073741822
SELECT bitwise_right_shift(INTEGER '-7', 64);
SELECT bitwise_right_shift(INTEGER '-2147483648', 0);
SELECT bitwise_right_shift(BIGINT '7', 2);
SELECT bitwise_right_shift(BIGINT '-7', 2);
SELECT bitwise_right_shift(BIGINT '-7', 64);
-- SELECT bitwise_right_shift_arithmetic(TINYINT '7', 2); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT bitwise_right_shift_arithmetic(TINYINT '7', 2);	                                      ^	Encountered: TINYINT	Expected: TINYINT is keyword, maybe `TINYINT`	
-- SELECT bitwise_right_shift_arithmetic(TINYINT '-7', 2); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT bitwise_right_shift_arithmetic(TINYINT '-7', 2);	                                      ^	Encountered: TINYINT	Expected: TINYINT is keyword, maybe `TINYINT`	
-- SELECT bitwise_right_shift_arithmetic(TINYINT '7', 64); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT bitwise_right_shift_arithmetic(TINYINT '7', 64);	                                      ^	Encountered: TINYINT	Expected: TINYINT is keyword, maybe `TINYINT`	
-- SELECT bitwise_right_shift_arithmetic(TINYINT '-7', 64); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT bitwise_right_shift_arithmetic(TINYINT '-7', 64);	                                      ^	Encountered: TINYINT	Expected: TINYINT is keyword, maybe `TINYINT`	
-- SELECT bitwise_right_shift_arithmetic(TINYINT '-128', 0); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT bitwise_right_shift_arithmetic(TINYINT '-128', 0);	                                      ^	Encountered: TINYINT	Expected: TINYINT is keyword, maybe `TINYINT`	
-- SELECT bitwise_right_shift_arithmetic(SMALLINT '7', 2); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT bitwise_right_shift_arithmetic(SMALLINT '7', 2);	                                      ^	Encountered: SMALLINT	Expected: SMALLINT is keyword, maybe `SMALLINT`	
-- SELECT bitwise_right_shift_arithmetic(SMALLINT '-7', 2); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT bitwise_right_shift_arithmetic(SMALLINT '-7', 2);	                                      ^	Encountered: SMALLINT	Expected: SMALLINT is keyword, maybe `SMALLINT`	
-- SELECT bitwise_right_shift_arithmetic(SMALLINT '7', 64); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT bitwise_right_shift_arithmetic(SMALLINT '7', 64);	                                      ^	Encountered: SMALLINT	Expected: SMALLINT is keyword, maybe `SMALLINT`	
-- SELECT bitwise_right_shift_arithmetic(SMALLINT '-7', 64); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT bitwise_right_shift_arithmetic(SMALLINT '-7', 64);	                                      ^	Encountered: SMALLINT	Expected: SMALLINT is keyword, maybe `SMALLINT`	
-- SELECT bitwise_right_shift_arithmetic(SMALLINT '-32768', 0); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT bitwise_right_shift_arithmetic(SMALLINT '-32768', 0);	                                      ^	Encountered: SMALLINT	Expected: SMALLINT is keyword, maybe `SMALLINT`	
-- SELECT bitwise_right_shift_arithmetic(INTEGER '7', 2); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT bitwise_right_shift_arithmetic(INTEGER '7', 2);	                                      ^	Encountered: INTEGER	Expected: INTEGER is keyword, maybe `INTEGER`	
-- SELECT bitwise_right_shift_arithmetic(INTEGER '-7', 2); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT bitwise_right_shift_arithmetic(INTEGER '-7', 2);	                                      ^	Encountered: INTEGER	Expected: INTEGER is keyword, maybe `INTEGER`	
-- SELECT bitwise_right_shift_arithmetic(INTEGER '7', 64); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT bitwise_right_shift_arithmetic(INTEGER '7', 64);	                                      ^	Encountered: INTEGER	Expected: INTEGER is keyword, maybe `INTEGER`	
-- SELECT bitwise_right_shift_arithmetic(INTEGER '-7', 64); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT bitwise_right_shift_arithmetic(INTEGER '-7', 64);	                                      ^	Encountered: INTEGER	Expected: INTEGER is keyword, maybe `INTEGER`	
-- SELECT bitwise_right_shift_arithmetic(INTEGER '-2147483648', 0); # error: errCode = 2, detailMessage = Syntax error in line 1:	...se_right_shift_arithmetic(INTEGER '-2147483648', 0);	                             ^	Encountered: INTEGER	Expected: INTEGER is keyword, maybe `INTEGER`	
-- SELECT bitwise_right_shift_arithmetic(BIGINT '7', 2); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT bitwise_right_shift_arithmetic(BIGINT '7', 2);	                                      ^	Encountered: BIGINT	Expected: BIGINT is keyword, maybe `BIGINT`	
-- SELECT bitwise_right_shift_arithmetic(BIGINT '-7', 2); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT bitwise_right_shift_arithmetic(BIGINT '-7', 2);	                                      ^	Encountered: BIGINT	Expected: BIGINT is keyword, maybe `BIGINT`	
-- SELECT bitwise_right_shift_arithmetic(BIGINT '7', 64); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT bitwise_right_shift_arithmetic(BIGINT '7', 64);	                                      ^	Encountered: BIGINT	Expected: BIGINT is keyword, maybe `BIGINT`	
-- SELECT bitwise_right_shift_arithmetic(BIGINT '-7', 64); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT bitwise_right_shift_arithmetic(BIGINT '-7', 64);	                                      ^	Encountered: BIGINT	Expected: BIGINT is keyword, maybe `BIGINT`	
set debug_skip_fold_constant=true;
SELECT bit_count(0, 64);
SELECT bit_count(7, 64);
SELECT bit_count(24, 64);
-- SELECT bit_count(-8, 64); # error: errCode = 2, detailMessage = Can not found function 'bit_count' which has 2 arity. Candidate functions are: [bit_countorg.apache.doris.nereids.trees.expressions.functions.BuiltinFunctionBuilder@10c6d615]
SELECT bit_count(0, 32);
SELECT bit_count(7, 32);
SELECT bit_count(24, 32);
-- SELECT bit_count(-8, 32); # error: errCode = 2, detailMessage = Can not found function 'bit_count' which has 2 arity. Candidate functions are: [bit_countorg.apache.doris.nereids.trees.expressions.functions.BuiltinFunctionBuilder@10c6d615]
SELECT bit_count(1152921504598458367, 62);
-- SELECT bit_count(-1, 62); # error: errCode = 2, detailMessage = Can not found function 'bit_count' which has 2 arity. Candidate functions are: [bit_countorg.apache.doris.nereids.trees.expressions.functions.BuiltinFunctionBuilder@10c6d615]
SELECT bit_count(33554132, 26);
-- SELECT bit_count(-1, 26); # error: errCode = 2, detailMessage = Can not found function 'bit_count' which has 2 arity. Candidate functions are: [bit_countorg.apache.doris.nereids.trees.expressions.functions.BuiltinFunctionBuilder@10c6d615]
SELECT bitwise_not(0);
SELECT bitwise_not(-1);
SELECT bitwise_not(8);
SELECT bitwise_not(-8);
-- SELECT bitwise_not(bitwise_not, bitwise_and, 0, -1); # error: errCode = 2, detailMessage = Unknown column 'bitwise_not' in 'table list' in PROJECT clause
SELECT bitwise_and(3, 8);
SELECT bitwise_and(-4, 12);
SELECT bitwise_and(60, 21);
SELECT bitwise_or(0, -1);
SELECT bitwise_or(3, 8);
SELECT bitwise_or(-4, 12);
SELECT bitwise_or(60, 21);
SELECT bitwise_xor(0, -1);
SELECT bitwise_xor(3, 8);
SELECT bitwise_xor(-4, 12);
SELECT bitwise_xor(60, 21);
SELECT bitwise_left_shift(TINYINT'7', 2);
SELECT bitwise_left_shift(TINYINT '-7', 2);
-- SELECT bitwise_left_shift(TINYINT '1', 7); # differ: doris : 128, presto : -128
-- SELECT bitwise_left_shift(TINYINT '-128', 1); # differ: doris : -256, presto : 0
-- SELECT bitwise_left_shift(TINYINT '-65', 1); # differ: doris : -130, presto : 126
SELECT bitwise_left_shift(TINYINT '-7', 64);
SELECT bitwise_left_shift(TINYINT '-128', 0);
SELECT bitwise_left_shift(SMALLINT '7', 2);
SELECT bitwise_left_shift(SMALLINT '-7', 2);
SELECT bitwise_left_shift(SMALLINT '1', 7);
-- SELECT bitwise_left_shift(SMALLINT '-32768', 1); # differ: doris : -65536, presto : 0
SELECT bitwise_left_shift(SMALLINT '-65', 1);
SELECT bitwise_left_shift(SMALLINT '-7', 64);
SELECT bitwise_left_shift(SMALLINT '-32768', 0);
SELECT bitwise_left_shift(INTEGER '7', 2);
SELECT bitwise_left_shift(INTEGER '-7', 2);
SELECT bitwise_left_shift(INTEGER '1', 7);
-- SELECT bitwise_left_shift(INTEGER '-2147483648', 1); # differ: doris : -4294967296, presto : 0
SELECT bitwise_left_shift(INTEGER '-65', 1);
SELECT bitwise_left_shift(INTEGER '-7', 64);
SELECT bitwise_left_shift(INTEGER '-2147483648', 0);
SELECT bitwise_left_shift(BIGINT '7', 2);
SELECT bitwise_left_shift(BIGINT '-7', 2);
SELECT bitwise_left_shift(BIGINT '-7', 64);
SELECT bitwise_right_shift(TINYINT '7', 2);
-- SELECT bitwise_right_shift(TINYINT '-7', 2); # differ: doris : 4611686018427387902, presto : 62
SELECT bitwise_right_shift(TINYINT '-7', 64);
SELECT bitwise_right_shift(TINYINT '-128', 0);
SELECT bitwise_right_shift(SMALLINT '7', 2);
-- SELECT bitwise_right_shift(SMALLINT '-7', 2); # differ: doris : 4611686018427387902, presto : 16382
SELECT bitwise_right_shift(SMALLINT '-7', 64);
SELECT bitwise_right_shift(SMALLINT '-32768', 0);
SELECT bitwise_right_shift(INTEGER '7', 2);
-- SELECT bitwise_right_shift(INTEGER '-7', 2); # differ: doris : 4611686018427387902, presto : 1073741822
SELECT bitwise_right_shift(INTEGER '-7', 64);
SELECT bitwise_right_shift(INTEGER '-2147483648', 0);
SELECT bitwise_right_shift(BIGINT '7', 2);
SELECT bitwise_right_shift(BIGINT '-7', 2);
SELECT bitwise_right_shift(BIGINT '-7', 64);
-- SELECT bitwise_right_shift_arithmetic(TINYINT '7', 2); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT bitwise_right_shift_arithmetic(TINYINT '7', 2);	                                      ^	Encountered: TINYINT	Expected: TINYINT is keyword, maybe `TINYINT`	
-- SELECT bitwise_right_shift_arithmetic(TINYINT '-7', 2); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT bitwise_right_shift_arithmetic(TINYINT '-7', 2);	                                      ^	Encountered: TINYINT	Expected: TINYINT is keyword, maybe `TINYINT`	
-- SELECT bitwise_right_shift_arithmetic(TINYINT '7', 64); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT bitwise_right_shift_arithmetic(TINYINT '7', 64);	                                      ^	Encountered: TINYINT	Expected: TINYINT is keyword, maybe `TINYINT`	
-- SELECT bitwise_right_shift_arithmetic(TINYINT '-7', 64); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT bitwise_right_shift_arithmetic(TINYINT '-7', 64);	                                      ^	Encountered: TINYINT	Expected: TINYINT is keyword, maybe `TINYINT`	
-- SELECT bitwise_right_shift_arithmetic(TINYINT '-128', 0); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT bitwise_right_shift_arithmetic(TINYINT '-128', 0);	                                      ^	Encountered: TINYINT	Expected: TINYINT is keyword, maybe `TINYINT`	
-- SELECT bitwise_right_shift_arithmetic(SMALLINT '7', 2); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT bitwise_right_shift_arithmetic(SMALLINT '7', 2);	                                      ^	Encountered: SMALLINT	Expected: SMALLINT is keyword, maybe `SMALLINT`	
-- SELECT bitwise_right_shift_arithmetic(SMALLINT '-7', 2); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT bitwise_right_shift_arithmetic(SMALLINT '-7', 2);	                                      ^	Encountered: SMALLINT	Expected: SMALLINT is keyword, maybe `SMALLINT`	
-- SELECT bitwise_right_shift_arithmetic(SMALLINT '7', 64); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT bitwise_right_shift_arithmetic(SMALLINT '7', 64);	                                      ^	Encountered: SMALLINT	Expected: SMALLINT is keyword, maybe `SMALLINT`	
-- SELECT bitwise_right_shift_arithmetic(SMALLINT '-7', 64); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT bitwise_right_shift_arithmetic(SMALLINT '-7', 64);	                                      ^	Encountered: SMALLINT	Expected: SMALLINT is keyword, maybe `SMALLINT`	
-- SELECT bitwise_right_shift_arithmetic(SMALLINT '-32768', 0); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT bitwise_right_shift_arithmetic(SMALLINT '-32768', 0);	                                      ^	Encountered: SMALLINT	Expected: SMALLINT is keyword, maybe `SMALLINT`	
-- SELECT bitwise_right_shift_arithmetic(INTEGER '7', 2); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT bitwise_right_shift_arithmetic(INTEGER '7', 2);	                                      ^	Encountered: INTEGER	Expected: INTEGER is keyword, maybe `INTEGER`	
-- SELECT bitwise_right_shift_arithmetic(INTEGER '-7', 2); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT bitwise_right_shift_arithmetic(INTEGER '-7', 2);	                                      ^	Encountered: INTEGER	Expected: INTEGER is keyword, maybe `INTEGER`	
-- SELECT bitwise_right_shift_arithmetic(INTEGER '7', 64); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT bitwise_right_shift_arithmetic(INTEGER '7', 64);	                                      ^	Encountered: INTEGER	Expected: INTEGER is keyword, maybe `INTEGER`	
-- SELECT bitwise_right_shift_arithmetic(INTEGER '-7', 64); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT bitwise_right_shift_arithmetic(INTEGER '-7', 64);	                                      ^	Encountered: INTEGER	Expected: INTEGER is keyword, maybe `INTEGER`	
-- SELECT bitwise_right_shift_arithmetic(INTEGER '-2147483648', 0); # error: errCode = 2, detailMessage = Syntax error in line 1:	...se_right_shift_arithmetic(INTEGER '-2147483648', 0);	                             ^	Encountered: INTEGER	Expected: INTEGER is keyword, maybe `INTEGER`	
-- SELECT bitwise_right_shift_arithmetic(BIGINT '7', 2); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT bitwise_right_shift_arithmetic(BIGINT '7', 2);	                                      ^	Encountered: BIGINT	Expected: BIGINT is keyword, maybe `BIGINT`	
-- SELECT bitwise_right_shift_arithmetic(BIGINT '-7', 2); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT bitwise_right_shift_arithmetic(BIGINT '-7', 2);	                                      ^	Encountered: BIGINT	Expected: BIGINT is keyword, maybe `BIGINT`	
-- SELECT bitwise_right_shift_arithmetic(BIGINT '7', 64); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT bitwise_right_shift_arithmetic(BIGINT '7', 64);	                                      ^	Encountered: BIGINT	Expected: BIGINT is keyword, maybe `BIGINT`	
-- SELECT bitwise_right_shift_arithmetic(BIGINT '-7', 64) # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT bitwise_right_shift_arithmetic(BIGINT '-7', 64);	                                      ^	Encountered: BIGINT	Expected: BIGINT is keyword, maybe `BIGINT`	
