SET enable_nereids_planner=TRUE;
SET enable_fallback_to_original_planner=FALSE;
SELECT CounterID, count(), max(if(SearchPhrase != "", SearchPhrase, "")) FROM hits GROUP BY CounterID ORDER BY count() DESC LIMIT 20
