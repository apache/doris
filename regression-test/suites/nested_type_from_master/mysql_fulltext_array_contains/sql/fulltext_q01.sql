SELECT/*+SET_VAR(enable_common_expr_pushdown=true,enable_common_expr_pushdown_for_inverted_index=true)*/ * FROM fulltext_t1_uk_array WHERE a MATCH_ANY 'collections' OR array_contains(b, 'collections') ORDER BY a;
SELECT/*+SET_VAR(enable_common_expr_pushdown=true,enable_common_expr_pushdown_for_inverted_index=true)*/ * FROM fulltext_t1_uk_array WHERE a MATCH_ANY 'indexes' OR array_contains(b, 'indexes') ORDER BY a;
SELECT/*+SET_VAR(enable_common_expr_pushdown=true,enable_common_expr_pushdown_for_inverted_index=true)*/ * FROM fulltext_t1_uk_array WHERE a MATCH_ANY 'indexes collections' OR array_contains(b, 'indexes collections') ORDER BY a;
SELECT/*+SET_VAR(enable_common_expr_pushdown=true,enable_common_expr_pushdown_for_inverted_index=true)*/ * FROM fulltext_t1_uk_array WHERE a MATCH_ANY 'only' OR array_contains(b, 'only') ORDER BY a;

SELECT/*+SET_VAR(enable_common_expr_pushdown=true,enable_common_expr_pushdown_for_inverted_index=true)*/ * FROM fulltext_t1_uk_array WHERE (a MATCH_ANY 'support' OR array_contains(b, 'support'))
                    AND NOT (a MATCH_ANY 'collections' OR array_contains(b, 'collections')) ORDER BY a;

SELECT/*+SET_VAR(enable_common_expr_pushdown=true,enable_common_expr_pushdown_for_inverted_index=true)*/ * FROM fulltext_t1_uk_array WHERE a MATCH_ANY 'support  collections' OR array_contains(b, 'support  collections') ORDER BY a;

SELECT/*+SET_VAR(enable_common_expr_pushdown=true,enable_common_expr_pushdown_for_inverted_index=true)*/ * FROM fulltext_t1_uk_array WHERE (a MATCH_ANY 'support collections' OR array_contains(b, 'support collections')) 
                    AND (a MATCH_ANY 'collections' OR array_contains(b, 'collections')) ORDER BY a;

SELECT/*+SET_VAR(enable_common_expr_pushdown=true,enable_common_expr_pushdown_for_inverted_index=true)*/ * FROM fulltext_t1_uk_array WHERE a MATCH_ALL 'support collections' OR array_contains(b, 'support collections') ORDER BY a;
SELECT/*+SET_VAR(enable_common_expr_pushdown=true,enable_common_expr_pushdown_for_inverted_index=true)*/ * FROM fulltext_t1_uk_array WHERE a MATCH_ANY 'search' OR array_contains(b, 'search') ORDER BY a;

SELECT/*+SET_VAR(enable_common_expr_pushdown=true,enable_common_expr_pushdown_for_inverted_index=true)*/ * FROM fulltext_t1_uk_array WHERE (a MATCH_ANY 'search' OR array_contains(b, 'search')) 
                    AND (a MATCH_ANY 'support vector' OR array_contains(b, 'support vector')) ORDER BY a;

SELECT/*+SET_VAR(enable_common_expr_pushdown=true,enable_common_expr_pushdown_for_inverted_index=true)*/ * FROM fulltext_t1_uk_array WHERE (a MATCH_ANY 'search' OR array_contains(b, 'search')) 
                    AND NOT (a MATCH_ANY 'support vector' OR array_contains(b, 'support vector')) ORDER BY a;

-- NOT SUPPORT:
-- SELECT/*+SET_VAR(enable_common_expr_pushdown=true,enable_common_expr_pushdown_for_inverted_index=true)*/ *, (a MATCH_ANY 'support collections' OR array_contains(b, 'support collections')) AS x FROM  fulltext_t1_uk;
-- SELECT/*+SET_VAR(enable_common_expr_pushdown=true,enable_common_expr_pushdown_for_inverted_index=true)*/ *, (a MATCH_ANY 'collections support' OR array_contains(b, 'collections support')) AS x FROM  fulltext_t1_uk;


-- match phrase, NOT SUPPORT:
-- SELECT/*+SET_VAR(enable_common_expr_pushdown=true,enable_common_expr_pushdown_for_inverted_index=true)*/ * FROM fulltext_t1_uk_array WHERE a MATCH_PHRASE 'support now' OR b MATCH_PHRASE 'support now';
-- SELECT/*+SET_VAR(enable_common_expr_pushdown=true,enable_common_expr_pushdown_for_inverted_index=true)*/ * FROM fulltext_t1_uk_array WHERE a MATCH_PHRASE 'Now sUPPort' OR b MATCH_PHRASE 'Now sUPPort';
-- SELECT/*+SET_VAR(enable_common_expr_pushdown=true,enable_common_expr_pushdown_for_inverted_index=true)*/ * FROM fulltext_t1_uk_array WHERE a MATCH_PHRASE 'now   support' OR b MATCH_PHRASE 'now   support';

-- SELECT/*+SET_VAR(enable_common_expr_pushdown=true,enable_common_expr_pushdown_for_inverted_index=true)*/ * FROM fulltext_t1_uk_array WHERE (a MATCH_PHRASE 'text search' OR b MATCH_PHRASE 'text search')
--                        OR (a MATCH_PHRASE 'now support' OR b MATCH_PHRASE 'now support');

-- SELECT/*+SET_VAR(enable_common_expr_pushdown=true,enable_common_expr_pushdown_for_inverted_index=true)*/ * FROM fulltext_t1_uk_array WHERE (a MATCH_PHRASE 'text search' OR b MATCH_PHRASE 'text search')
--                        AND NOT (a MATCH_PHRASE 'now support' OR b MATCH_PHRASE 'now support');


SELECT/*+SET_VAR(enable_common_expr_pushdown=true,enable_common_expr_pushdown_for_inverted_index=true)*/ * FROM fulltext_t1_uk_array WHERE a MATCH_ANY '"space model' OR array_contains(b, '"space model') ORDER BY a;