

SELECT/*+SET_VAR(enable_common_expr_pushdown=true,enable_common_expr_pushdown_for_inverted_index=true)*/ COUNT(*) FROM large_records_t3_uk_array;

SELECT/*+SET_VAR(enable_common_expr_pushdown=true,enable_common_expr_pushdown_for_inverted_index=true)*/ COUNT(*) FROM large_records_t3_uk_array WHERE array_contains(b, 'samerowword');

SELECT/*+SET_VAR(enable_common_expr_pushdown=true,enable_common_expr_pushdown_for_inverted_index=true)*/ COUNT(*) FROM large_records_t3_uk_array WHERE a MATCH_ANY 'samerowword' OR array_contains(b, 'samerowword');

SELECT/*+SET_VAR(enable_common_expr_pushdown=true,enable_common_expr_pushdown_for_inverted_index=true)*/ COUNT(*) FROM large_records_t3_uk_array 
    WHERE (a MATCH_ANY 'samerowword' OR array_contains(b, 'samerowword'))
    AND (a MATCH_ANY 'samerowword' OR array_contains(b, 'samerowword'));

SELECT/*+SET_VAR(enable_common_expr_pushdown=true,enable_common_expr_pushdown_for_inverted_index=true)*/ COUNT(*) FROM large_records_t3_uk_array 
    WHERE (a MATCH_ANY 'samerowword' OR array_contains(b, 'samerowword'))
    AND NOT (a MATCH_ANY 'row45col2word49' OR array_contains(b, 'row45col2word49'));

SELECT/*+SET_VAR(enable_common_expr_pushdown=true,enable_common_expr_pushdown_for_inverted_index=true)*/ COUNT(*) FROM large_records_t3_uk_array 
    WHERE (a MATCH_ANY 'sameroww' OR array_contains(b, 'sameroww'));