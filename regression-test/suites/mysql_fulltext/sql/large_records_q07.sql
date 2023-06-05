

SELECT COUNT(*) FROM large_records_t4_uk;

SELECT COUNT(*) FROM large_records_t4_uk WHERE a like '%samerowword%';

SELECT COUNT(*) FROM large_records_t4_uk WHERE a MATCH_ANY 'samerowword' OR b MATCH_ANY 'samerowword';

SELECT COUNT(*) FROM large_records_t4_uk 
    WHERE (a MATCH_ANY 'samerowword' OR b MATCH_ANY 'samerowword')
    AND (a MATCH_ANY '1050' OR b MATCH_ANY '1050');

SELECT COUNT(*) FROM large_records_t4_uk 
    WHERE (a MATCH_ANY 'samerowword' OR b MATCH_ANY 'samerowword')
    AND NOT (a MATCH_ANY '1050' OR b MATCH_ANY '1050');

SELECT COUNT(*) FROM large_records_t4_uk WHERE a MATCH_ANY '2001' OR b MATCH_ANY '2001';