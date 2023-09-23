SELECT CounterID, min(WatchID), max(WatchID) FROM hits GROUP BY CounterID ORDER BY count() DESC LIMIT 20
