SELECT/*+SET_VAR(enable_common_expr_pushdown=true,enable_common_expr_pushdown_for_inverted_index=true)*/ * FROM articles_uk_array WHERE array_contains(title, 'Database') OR array_contains(body, 'Database') ORDER BY id;
SELECT/*+SET_VAR(enable_common_expr_pushdown=true,enable_common_expr_pushdown_for_inverted_index=true)*/ COUNT(*) FROM articles_uk_array WHERE array_contains(title, 'database') OR array_contains(body, 'database');
SELECT/*+SET_VAR(enable_common_expr_pushdown=true,enable_common_expr_pushdown_for_inverted_index=true)*/ * FROM articles_uk_array WHERE array_contains(title, 'Tutorial') OR array_contains(body, 'Tutorial') ORDER BY id;
SELECT/*+SET_VAR(enable_common_expr_pushdown=true,enable_common_expr_pushdown_for_inverted_index=true)*/ * FROM articles_uk_array WHERE array_contains(title, 'MySQL') OR array_contains(body, 'MySQL') ORDER BY id;

-- NOT SUPPORT:
-- SELECT/*+SET_VAR(enable_common_expr_pushdown=true,enable_common_expr_pushdown_for_inverted_index=true)*/ COUNT(IF(array_contains(title, 'database') OR array_contains(body, 'database'), 1, NULL)) as count FROM articles_uk_array;

SELECT/*+SET_VAR(enable_common_expr_pushdown=true,enable_common_expr_pushdown_for_inverted_index=true)*/ * FROM articles_uk_array WHERE (array_contains(title, 'MySQL') AND NOT array_contains(title, 'YourSQL')) 
                            OR (array_contains(body, 'MySQL') AND NOT array_contains(body, 'YourSQL')) ORDER BY id;

SELECT/*+SET_VAR(enable_common_expr_pushdown=true,enable_common_expr_pushdown_for_inverted_index=true)*/ * FROM articles_uk_array WHERE array_contains(title, 'DBMS Security') OR array_contains(body, 'DBMS Security') ORDER BY id;
SELECT/*+SET_VAR(enable_common_expr_pushdown=true,enable_common_expr_pushdown_for_inverted_index=true)*/ * FROM articles_uk_array WHERE array_contains(title, 'MySQL YourSQL') OR array_contains(body, 'MySQL YourSQL') ORDER BY id;

SELECT/*+SET_VAR(enable_common_expr_pushdown=true,enable_common_expr_pushdown_for_inverted_index=true)*/ * FROM articles_uk_array WHERE (array_contains(title, 'MySQL') OR array_contains(body, 'MySQL')) 
                            AND NOT (array_contains(title, 'Well stands') OR array_contains(body, 'Well stands')) ORDER BY id;

SELECT/*+SET_VAR(enable_common_expr_pushdown=true,enable_common_expr_pushdown_for_inverted_index=true)*/ * FROM articles_uk_array WHERE (array_contains(title, 'YourSQL') OR array_contains(body, 'YourSQL')) 
                            OR ((array_contains(title, 'MySQL') OR array_contains(body, 'MySQL')) 
                                AND NOT (array_contains(title, 'Tricks Security') OR array_contains(body, 'Tricks Security'))) ORDER BY id;

SELECT/*+SET_VAR(enable_common_expr_pushdown=true,enable_common_expr_pushdown_for_inverted_index=true)*/ * FROM articles_uk_array WHERE ((array_contains(title, 'MySQL') OR array_contains(body, 'MySQL')) 
                                AND NOT (array_contains(title, 'Tricks Security') OR array_contains(body, 'Tricks Security')))
                           AND NOT (array_contains(title, 'YourSQL') OR array_contains(body, 'YourSQL')) ORDER BY id;

-- match phrase, NOT SUPPORT:
-- SELECT/*+SET_VAR(enable_common_expr_pushdown=true,enable_common_expr_pushdown_for_inverted_index=true)*/ * FROM articles_uk_array WHERE title MATCH_PHRASE 'following database' OR body MATCH_PHRASE 'following database';