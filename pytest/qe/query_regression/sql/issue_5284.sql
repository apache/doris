-- https://github.com/apache/incubator-doris/issues/5284
SELECT a.aid, b.bid FROM (SELECT 3 AS aid)a RIGHT JOIN (SELECT 4 AS bid)b ON (a.aid=b.bid);
