set sql_dialect='presto';
set enable_fallback_to_original_planner=false;
set debug_skip_fold_constant=false;
-- SELECT luhn_check('4242424242424242'); # error: errCode = 2, detailMessage = Can not found function 'luhn_check'
-- SELECT luhn_check('1234567891234567'); # error: errCode = 2, detailMessage = Can not found function 'luhn_check'
-- SELECT luhn_check(''); # error: errCode = 2, detailMessage = Can not found function 'luhn_check'
-- SELECT luhn_check(NULL); # error: errCode = 2, detailMessage = Can not found function 'luhn_check'
-- SELECT luhn_check('123456789'); # error: errCode = 2, detailMessage = Can not found function 'luhn_check'
set debug_skip_fold_constant=true;
-- SELECT luhn_check('4242424242424242'); # error: errCode = 2, detailMessage = Can not found function 'luhn_check'
-- SELECT luhn_check('1234567891234567'); # error: errCode = 2, detailMessage = Can not found function 'luhn_check'
-- SELECT luhn_check(''); # error: errCode = 2, detailMessage = Can not found function 'luhn_check'
-- SELECT luhn_check(NULL); # error: errCode = 2, detailMessage = Can not found function 'luhn_check'
-- SELECT luhn_check('123456789'); # error: errCode = 2, detailMessage = Can not found function 'luhn_check'
