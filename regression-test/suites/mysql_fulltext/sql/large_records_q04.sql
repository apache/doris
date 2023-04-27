

SELECT COUNT(*) FROM large_records_t2_dk;

SELECT FTS_DOC_ID FROM large_records_t2_dk WHERE b like '%row300col2word30%';

SELECT FTS_DOC_ID FROM large_records_t2_dk WHERE a MATCH_ANY 'row35col2word49' OR b MATCH_ANY 'row35col2word49';

SELECT COUNT(*) FROM large_records_t2_dk 
    WHERE (a MATCH_ANY 'row5col2word49' OR b MATCH_ANY 'row5col2word49')
    AND (a MATCH_ANY 'row5col1word49' OR b MATCH_ANY 'row5col1word49');

SELECT COUNT(*) from large_records_t2_dk 
    WHERE (a MATCH_ANY 'row5col2word49' OR b MATCH_ANY 'row5col2word49');

SELECT COUNT(*) from large_records_t2_dk 
    WHERE (a MATCH_ANY 'row35col2word49' OR b MATCH_ANY 'row35col2word49')
    AND (a MATCH_ANY 'row35col1word49 row35col2word40' OR b MATCH_ANY 'row35col1word49 row35col2word40');

SELECT COUNT(*) from large_records_t2_dk 
    WHERE (a MATCH_ANY 'row35col2word49' OR b MATCH_ANY 'row35col2word49')
    AND NOT (a MATCH_ANY 'row45col2word49' OR b MATCH_ANY 'row45col2word49');

SELECT COUNT(*) from large_records_t2_dk 
    WHERE a MATCH_ANY 'row5col2word49 row5col2word40' OR b MATCH_ANY 'row5col2word49 row5col2word40';

SELECT COUNT(*) from large_records_t2_dk 
    WHERE a MATCH_ALL 'ROW35col2WORD49' OR b MATCH_ALL 'ROW35col2WORD49';