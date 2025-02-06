set sql_dialect='presto';
set enable_fallback_to_original_planner=false;
set debug_skip_fold_constant=false;
-- SELECT generic_incomplete_specialization_nullable(9876543210); # error: errCode = 2, detailMessage = Can not found function 'generic_incomplete_specialization_nullable'
-- SELECT generic_incomplete_specialization_nullable(1.234E0); # error: errCode = 2, detailMessage = Can not found function 'generic_incomplete_specialization_nullable'
-- SELECT generic_incomplete_specialization_nullable('abcd'); # error: errCode = 2, detailMessage = Can not found function 'generic_incomplete_specialization_nullable'
-- SELECT generic_incomplete_specialization_nullable(true); # error: errCode = 2, detailMessage = Can not found function 'generic_incomplete_specialization_nullable'
-- SELECT generic_incomplete_specialization_nullable(array[1, 2]); # error: errCode = 2, detailMessage = Syntax error in line 1:	...alization_nullable(array[1, 2]);	                             ^	Encountered: COMMA	Expected: ||	
-- SELECT generic_incomplete_specialization_not_nullable(9876543210); # error: errCode = 2, detailMessage = Can not found function 'generic_incomplete_specialization_not_nullable'
-- SELECT generic_incomplete_specialization_not_nullable(1.234E0); # error: errCode = 2, detailMessage = Can not found function 'generic_incomplete_specialization_not_nullable'
-- SELECT generic_incomplete_specialization_not_nullable('abcd'); # error: errCode = 2, detailMessage = Can not found function 'generic_incomplete_specialization_not_nullable'
-- SELECT generic_incomplete_specialization_not_nullable(true); # error: errCode = 2, detailMessage = Can not found function 'generic_incomplete_specialization_not_nullable'
-- SELECT generic_incomplete_specialization_not_nullable(array[1, 2]); # error: errCode = 2, detailMessage = Syntax error in line 1:	...ation_not_nullable(array[1, 2]);	                             ^	Encountered: COMMA	Expected: ||	
set debug_skip_fold_constant=true;
-- SELECT generic_incomplete_specialization_nullable(9876543210); # error: errCode = 2, detailMessage = Can not found function 'generic_incomplete_specialization_nullable'
-- SELECT generic_incomplete_specialization_nullable(1.234E0); # error: errCode = 2, detailMessage = Can not found function 'generic_incomplete_specialization_nullable'
-- SELECT generic_incomplete_specialization_nullable('abcd'); # error: errCode = 2, detailMessage = Can not found function 'generic_incomplete_specialization_nullable'
-- SELECT generic_incomplete_specialization_nullable(true); # error: errCode = 2, detailMessage = Can not found function 'generic_incomplete_specialization_nullable'
-- SELECT generic_incomplete_specialization_nullable(array[1, 2]); # error: errCode = 2, detailMessage = Syntax error in line 1:	...alization_nullable(array[1, 2]);	                             ^	Encountered: COMMA	Expected: ||	
-- SELECT generic_incomplete_specialization_not_nullable(9876543210); # error: errCode = 2, detailMessage = Can not found function 'generic_incomplete_specialization_not_nullable'
-- SELECT generic_incomplete_specialization_not_nullable(1.234E0); # error: errCode = 2, detailMessage = Can not found function 'generic_incomplete_specialization_not_nullable'
-- SELECT generic_incomplete_specialization_not_nullable('abcd'); # error: errCode = 2, detailMessage = Can not found function 'generic_incomplete_specialization_not_nullable'
-- SELECT generic_incomplete_specialization_not_nullable(true); # error: errCode = 2, detailMessage = Can not found function 'generic_incomplete_specialization_not_nullable'
-- SELECT generic_incomplete_specialization_not_nullable(array[1, 2]) # error: errCode = 2, detailMessage = Syntax error in line 1:	...ation_not_nullable(array[1, 2]);	                             ^	Encountered: COMMA	Expected: ||	
