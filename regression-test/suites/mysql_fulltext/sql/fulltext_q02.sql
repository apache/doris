SELECT * FROM fulltext_t1_dk WHERE a MATCH_ANY 'collections' OR b MATCH_ANY 'collections' ORDER BY a;
SELECT * FROM fulltext_t1_dk WHERE a MATCH_ANY 'indexes' OR b MATCH_ANY 'indexes' ORDER BY a;
SELECT * FROM fulltext_t1_dk WHERE a MATCH_ANY 'indexes collections' OR b MATCH_ANY 'indexes collections' ORDER BY a;
SELECT * FROM fulltext_t1_dk WHERE a MATCH_ANY 'only' OR b MATCH_ANY 'only' ORDER BY a;

SELECT * FROM fulltext_t1_dk WHERE (a MATCH_ANY 'support' OR b MATCH_ANY 'support')
                    AND NOT (a MATCH_ANY 'collections' OR b MATCH_ANY 'collections') ORDER BY a;

SELECT * FROM fulltext_t1_dk WHERE a MATCH_ANY 'support  collections' OR b MATCH_ANY 'support  collections' ORDER BY a;

SELECT * FROM fulltext_t1_dk WHERE (a MATCH_ANY 'support collections' OR b MATCH_ANY 'support collections') 
                    AND (a MATCH_ANY 'collections' OR b MATCH_ANY 'collections') ORDER BY a;

SELECT * FROM fulltext_t1_dk WHERE a MATCH_ALL 'support collections' OR b MATCH_ALL 'support collections' ORDER BY a;
SELECT * FROM fulltext_t1_dk WHERE a MATCH_ANY 'search' OR b MATCH_ANY 'search' ORDER BY a;

SELECT * FROM fulltext_t1_dk WHERE (a MATCH_ANY 'search' OR b MATCH_ANY 'search') 
                    AND (a MATCH_ANY 'support vector' OR b MATCH_ANY 'support vector') ORDER BY a;

SELECT * FROM fulltext_t1_dk WHERE (a MATCH_ANY 'search' OR b MATCH_ANY 'search') 
                    AND NOT (a MATCH_ANY 'support vector' OR b MATCH_ANY 'support vector') ORDER BY a;

-- NOT SUPPORT:
-- SELECT *, (a MATCH_ANY 'support collections' OR b MATCH_ANY 'support collections') AS x FROM  fulltext_t1_dk;
-- SELECT *, (a MATCH_ANY 'collections support' OR b MATCH_ANY 'collections support') AS x FROM  fulltext_t1_dk;


-- match phrase, NOT SUPPORT:
-- SELECT * FROM fulltext_t1_dk WHERE a MATCH_PHRASE 'support now' OR b MATCH_PHRASE 'support now';
-- SELECT * FROM fulltext_t1_dk WHERE a MATCH_PHRASE 'Now sUPPort' OR b MATCH_PHRASE 'Now sUPPort';
-- SELECT * FROM fulltext_t1_dk WHERE a MATCH_PHRASE 'now   support' OR b MATCH_PHRASE 'now   support';

-- SELECT * FROM fulltext_t1_dk WHERE (a MATCH_PHRASE 'text search' OR b MATCH_PHRASE 'text search')
--                        OR (a MATCH_PHRASE 'now support' OR b MATCH_PHRASE 'now support');

-- SELECT * FROM fulltext_t1_dk WHERE (a MATCH_PHRASE 'text search' OR b MATCH_PHRASE 'text search')
--                        AND NOT (a MATCH_PHRASE 'now support' OR b MATCH_PHRASE 'now support');


SELECT * FROM fulltext_t1_dk WHERE a MATCH_ANY '"space model' OR b MATCH_ANY '"space model' ORDER BY a;