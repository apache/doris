SELECT
    yearweek(day) AS k,
    count() AS c
FROM opensky
WHERE origin IN ('UUEE', 'UUDD', 'UUWW')
GROUP BY k
ORDER BY k ASC;
