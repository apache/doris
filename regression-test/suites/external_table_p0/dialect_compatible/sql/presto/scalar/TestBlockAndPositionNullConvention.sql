set sql_dialect='presto';
set enable_fallback_to_original_planner=false;
set debug_skip_fold_constant=false;
-- SELECT test_block_position(BIGINT '1234'); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT test_block_position(BIGINT '1234');	                           ^	Encountered: BIGINT	Expected: BIGINT is keyword, maybe `BIGINT`	
-- SELECT test_block_position(12.34e0); # error: errCode = 2, detailMessage = Can not found function 'test_block_position'
-- SELECT test_block_position('hello'); # error: errCode = 2, detailMessage = Can not found function 'test_block_position'
-- SELECT test_block_position(true); # error: errCode = 2, detailMessage = Can not found function 'test_block_position'
-- SELECT test_block_position(ROW(1234)); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT test_block_position(ROW(1234));	                           ^	Encountered: ROW	Expected: ROW is keyword, maybe `ROW`	
-- SELECT test_value_block_position(BIGINT '1234'); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT test_value_block_position(BIGINT '1234');	                                 ^	Encountered: BIGINT	Expected: BIGINT is keyword, maybe `BIGINT`	
-- SELECT test_value_block_position(12.34e0); # error: errCode = 2, detailMessage = Can not found function 'test_value_block_position'
-- SELECT test_value_block_position('hello'); # error: errCode = 2, detailMessage = Can not found function 'test_value_block_position'
-- SELECT test_value_block_position(true); # error: errCode = 2, detailMessage = Can not found function 'test_value_block_position'
set debug_skip_fold_constant=true;
-- SELECT test_block_position(BIGINT '1234'); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT test_block_position(BIGINT '1234');	                           ^	Encountered: BIGINT	Expected: BIGINT is keyword, maybe `BIGINT`	
-- SELECT test_block_position(12.34e0); # error: errCode = 2, detailMessage = Can not found function 'test_block_position'
-- SELECT test_block_position('hello'); # error: errCode = 2, detailMessage = Can not found function 'test_block_position'
-- SELECT test_block_position(true); # error: errCode = 2, detailMessage = Can not found function 'test_block_position'
-- SELECT test_block_position(ROW(1234)); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT test_block_position(ROW(1234));	                           ^	Encountered: ROW	Expected: ROW is keyword, maybe `ROW`	
-- SELECT test_value_block_position(BIGINT '1234'); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT test_value_block_position(BIGINT '1234');	                                 ^	Encountered: BIGINT	Expected: BIGINT is keyword, maybe `BIGINT`	
-- SELECT test_value_block_position(12.34e0); # error: errCode = 2, detailMessage = Can not found function 'test_value_block_position'
-- SELECT test_value_block_position('hello'); # error: errCode = 2, detailMessage = Can not found function 'test_value_block_position'
-- SELECT test_value_block_position(true) # error: errCode = 2, detailMessage = Can not found function 'test_value_block_position'
