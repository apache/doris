SELECT loyalty, count() AS c
FROM
(
    SELECT UserID, CAST(((if(yandex > google, yandex / (yandex + google), 0 - google / (yandex + google))) * 10) AS TINYINT) AS loyalty
    FROM
    (
        SELECT UserID, sum(if(SearchEngineID = 2, 1, 0)) AS yandex, sum(if(SearchEngineID = 3, 1, 0)) AS google
        FROM hits
        WHERE SearchEngineID = 2 OR SearchEngineID = 3 GROUP BY UserID HAVING yandex + google > 10
    ) t1
) t2
GROUP BY loyalty
ORDER BY loyalty
