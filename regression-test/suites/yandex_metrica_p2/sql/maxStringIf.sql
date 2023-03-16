SELECT CounterID, count(), max(if(SearchPhrase != "", SearchPhrase, "")) FROM hits GROUP BY CounterID ORDER BY count() DESC LIMIT 20
