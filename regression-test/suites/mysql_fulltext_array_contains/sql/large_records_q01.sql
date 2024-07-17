

SELECT/*+SET_VAR(enable_common_expr_pushdown=true,enable_common_expr_pushdown_for_inverted_index=true)*/ COUNT(*) FROM large_records_t1_uk_array;

SELECT/*+SET_VAR(enable_common_expr_pushdown=true,enable_common_expr_pushdown_for_inverted_index=true)*/ FTS_DOC_ID FROM large_records_t1_uk_array WHERE a MATCH_ANY 'row35col2word49' OR array_contains(b, 'row35col2word49') order by FTS_DOC_ID;

SELECT/*+SET_VAR(enable_common_expr_pushdown=true,enable_common_expr_pushdown_for_inverted_index=true)*/ COUNT(*) FROM large_records_t1_uk_array
        WHERE (a MATCH_ANY 'row5col2word49' OR array_contains(b, 'row5col2word49'))
        AND (a MATCH_ANY 'row5col1word49' OR array_contains(b, 'row5col1word49'));

SELECT/*+SET_VAR(enable_common_expr_pushdown=true,enable_common_expr_pushdown_for_inverted_index=true)*/ COUNT(*) FROM large_records_t1_uk_array
        WHERE a MATCH_ANY 'row5col2word49' OR  array_contains(b, 'row5col2word49');

SELECT/*+SET_VAR(enable_common_expr_pushdown=true,enable_common_expr_pushdown_for_inverted_index=true)*/ COUNT(*) FROM large_records_t1_uk_array
        WHERE (a MATCH_ANY 'row35col2word49' OR array_contains(b, 'row35col2word49'))
        AND (a MATCH_ANY 'row35col1word49 row35col2word40' OR array_contains(b, 'row35col1word49 row35col2word40'));

SELECT/*+SET_VAR(enable_common_expr_pushdown=true,enable_common_expr_pushdown_for_inverted_index=true)*/ COUNT(*) FROM large_records_t1_uk_array
        WHERE (a MATCH_ANY 'row35col2word49' OR array_contains(b, 'row35col2word49'))
        AND NOT (a MATCH_ANY 'row45col2word49' OR array_contains(b, 'row45col2word49'));

SELECT/*+SET_VAR(enable_common_expr_pushdown=true,enable_common_expr_pushdown_for_inverted_index=true)*/ COUNT(*) FROM large_records_t1_uk_array
        WHERE a MATCH_ANY 'row5col2word49' OR  array_contains(b, 'row5col2word40');

SELECT/*+SET_VAR(enable_common_expr_pushdown=true,enable_common_expr_pushdown_for_inverted_index=true)*/ COUNT(*) FROM large_records_t1_uk_array
        WHERE a MATCH_ANY 'row35col2word49' OR  array_contains(b, 'row35col2word49');

SELECT/*+SET_VAR(enable_common_expr_pushdown=true,enable_common_expr_pushdown_for_inverted_index=true)*/ COUNT(*) FROM large_records_t1_uk_array
        WHERE a MATCH_ANY 'ROW35col2WORD49' OR  array_contains(b, 'ROW35col2WORD49');

SELECT/*+SET_VAR(enable_common_expr_pushdown=true,enable_common_expr_pushdown_for_inverted_index=true)*/ a,b FROM large_records_t1_uk_array
        WHERE (a MATCH_ANY 'row5col2word49' OR array_contains(b, 'row5col2word49'))
        AND (a MATCH_ANY 'row5col1word49' OR array_contains(b, 'row5col1word49')) ORDER BY FTS_DOC_ID;

