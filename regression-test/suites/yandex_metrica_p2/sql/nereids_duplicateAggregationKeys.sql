SET enable_nereids_planner=true;
SET enable_fallback_to_original_planner=false;

SELECT URL, EventDate, max(URL) FROM hits WHERE CounterID = 1704509 AND UserID = 4322253409885123546 GROUP BY URL, EventDate, EventDate ORDER BY URL, EventDate
