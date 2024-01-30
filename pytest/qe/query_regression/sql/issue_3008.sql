-- https://github.com/apache/incubator-doris/issues/3008
select str_to_date(substring('2020-02-09', 1, 1024), '%b %e %Y')
select ifnull(date(substring('2020-02-09', 1, 1024)), null)
select ifnull(date(substring('2020-02-09', 1, 1024)), str_to_date(substring('2020-02-09', 1, 1024), '%b %e %Y'))
SELECT CASE  WHEN NOT ISNULL(DATE(TIMESTAMP(STR_TO_DATE(SUBSTRING('2020-02-98', 1, 1024), '%Y-%m-%d')))) THEN DATE(TIMESTAMP(STR_TO_DATE(SUBSTRING('2020-02-09', 1, 1024), '%Y-%m-%d'))) WHEN NOT ISNULL(IFNULL(DATE(SUBSTRING('2020-02-09', 1, 1024)), STR_TO_DATE(SUBSTRING('2020-02-09', 1, 1024), '%b %e %Y'))) THEN IFNULL(DATE(SUBSTRING('2020-02-09', 1, 1024)), STR_TO_DATE(SUBSTRING('2020-02-09', 1, 1024), '%b %e %Y')) ELSE NULL END = TIMESTAMP('2020-02-09 00:00:00') AS c1
