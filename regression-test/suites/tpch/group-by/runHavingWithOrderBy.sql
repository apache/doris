SELECT id_department, COUNT(*) FROM workers GROUP BY id_department HAVING COUNT(*) > 1 ORDER BY id_department desc
