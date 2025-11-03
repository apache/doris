set sql_dialect='presto';
set enable_fallback_to_original_planner=false;
set debug_skip_fold_constant=false;
-- SELECT custom_add(123, 456); # error: errCode = 2, detailMessage = Can not found function 'custom_add'
-- SELECT custom_is_null(CAST(NULL AS VARCHAR)); # error: errCode = 2, detailMessage = Can not found function 'custom_is_null'
-- SELECT custom_is_null('not null'); # error: errCode = 2, detailMessage = Can not found function 'custom_is_null'
-- SELECT custom_is_null(CAST(NULL AS BIGINT)); # error: errCode = 2, detailMessage = Can not found function 'custom_is_null'
-- SELECT custom_is_null(0); # error: errCode = 2, detailMessage = Can not found function 'custom_is_null'
-- SELECT identity&function(\, (123)); # error: errCode = 2, detailMessage = Please check your sql, we meet an error when parsing.
-- SELECT identity.function(\, (123)); # error: errCode = 2, detailMessage = Please check your sql, we meet an error when parsing.
set debug_skip_fold_constant=true;
-- SELECT custom_add(123, 456); # error: errCode = 2, detailMessage = Can not found function 'custom_add'
-- SELECT custom_is_null(CAST(NULL AS VARCHAR)); # error: errCode = 2, detailMessage = Can not found function 'custom_is_null'
-- SELECT custom_is_null('not null'); # error: errCode = 2, detailMessage = Can not found function 'custom_is_null'
-- SELECT custom_is_null(CAST(NULL AS BIGINT)); # error: errCode = 2, detailMessage = Can not found function 'custom_is_null'
-- SELECT custom_is_null(0); # error: errCode = 2, detailMessage = Can not found function 'custom_is_null'
-- SELECT identity&function(\, (123)); # error: errCode = 2, detailMessage = Please check your sql, we meet an error when parsing.
-- SELECT identity.function(\, (123)) # error: errCode = 2, detailMessage = Please check your sql, we meet an error when parsing.
