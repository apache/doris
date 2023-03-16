SELECT COUNT(*) FROM workers GROUP BY id_department * 2 HAVING SUM(log10(salary + 1)) > 0
