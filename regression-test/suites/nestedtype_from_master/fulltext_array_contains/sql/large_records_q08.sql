

SELECT/*+SET_VAR(enable_common_expr_pushdown=true,enable_common_expr_pushdown_for_inverted_index=true)*/ COUNT(*) FROM large_records_t4_dk_array;

SELECT/*+SET_VAR(enable_common_expr_pushdown=true,enable_common_expr_pushdown_for_inverted_index=true)*/ COUNT(*) FROM large_records_t4_dk_array WHERE array_contains(b, 'samerowword');

SELECT/*+SET_VAR(enable_common_expr_pushdown=true,enable_common_expr_pushdown_for_inverted_index=true)*/ COUNT(*) FROM large_records_t4_dk_array WHERE a MATCH_ANY 'samerowword' OR array_contains(b, 'samerowword');

SELECT/*+SET_VAR(enable_common_expr_pushdown=true,enable_common_expr_pushdown_for_inverted_index=true)*/ COUNT(*) FROM large_records_t4_dk_array 
    WHERE (a MATCH_ANY 'samerowword' OR array_contains(b, 'samerowword'))
    AND (a MATCH_ANY '1050' OR array_contains(b, '1050'));

SELECT/*+SET_VAR(enable_common_expr_pushdown=true,enable_common_expr_pushdown_for_inverted_index=true)*/ COUNT(*) FROM large_records_t4_dk_array 
    WHERE (a MATCH_ANY 'samerowword' OR array_contains(b, 'samerowword'))
    AND NOT (a MATCH_ANY '1050' OR array_contains(b, '1050'));

SELECT/*+SET_VAR(enable_common_expr_pushdown=true,enable_common_expr_pushdown_for_inverted_index=true)*/ COUNT(*) FROM large_records_t4_dk_array WHERE a MATCH_ANY '2001' OR array_contains(b, '2001');