SELECT
    origin,
    count(),
    round(avg(st_distance_sphere(longitude_1, latitude_1, longitude_2, latitude_2))) AS distance
FROM opensky
WHERE origin != ''
GROUP BY origin
ORDER BY count() DESC
LIMIT 100;


