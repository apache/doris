SELECT CounterID, count(distinct UserID) FROM hits WHERE 0 != 0 GROUP BY CounterID
