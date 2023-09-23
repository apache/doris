SET enable_nereids_planner=true;
SET enable_fallback_to_original_planner=false;

SELECT CounterID, count(), max(if(SearchPhrase != "", SearchPhrase, "")) FROM hits GROUP BY CounterID ORDER BY count() DESC LIMIT 20
