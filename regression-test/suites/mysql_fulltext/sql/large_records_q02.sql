

SELECT COUNT(*) FROM large_records_t1_dk;

SELECT FTS_DOC_ID FROM large_records_t1_dk WHERE a MATCH_ANY 'row35col2word49' OR b MATCH_ANY 'row35col2word49' order by FTS_DOC_ID;

SELECT COUNT(*) from large_records_t1_dk
        WHERE (a MATCH_ANY 'row5col2word49' OR b MATCH_ANY 'row5col2word49')
        AND (a MATCH_ANY 'row5col1word49' OR b MATCH_ANY 'row5col1word49');

SELECT COUNT(*) from large_records_t1_dk
        WHERE a MATCH_ANY 'row5col2word49' OR  b MATCH_ANY 'row5col2word49';

SELECT COUNT(*) from large_records_t1_dk
        WHERE (a MATCH_ANY 'row35col2word49' OR b MATCH_ANY 'row35col2word49')
        AND (a MATCH_ANY 'row35col1word49 row35col2word40' OR b MATCH_ANY 'row35col1word49 row35col2word40');

SELECT COUNT(*) from large_records_t1_dk
        WHERE (a MATCH_ANY 'row35col2word49' OR b MATCH_ANY 'row35col2word49')
        AND NOT (a MATCH_ANY 'row45col2word49' OR b MATCH_ANY 'row45col2word49');

SELECT COUNT(*) from large_records_t1_dk
        WHERE a MATCH_ANY 'row5col2word49' OR  b MATCH_ANY 'row5col2word40';

SELECT COUNT(*) from large_records_t1_dk
        WHERE a MATCH_ANY 'row35col2word49' OR  b MATCH_ANY 'row35col2word49';

SELECT COUNT(*) from large_records_t1_dk
        WHERE a MATCH_ANY 'ROW35col2WORD49' OR  b MATCH_ANY 'ROW35col2WORD49';

SELECT a,b FROM large_records_t1_dk
        WHERE (a MATCH_ANY 'row5col2word49' OR b MATCH_ANY 'row5col2word49')
        AND (a MATCH_ANY 'row5col1word49' OR b MATCH_ANY 'row5col1word49') ORDER BY a,b;

