SELECT COUNT(*) FROM workers GROUP BY salary * id_department HAVING salary * id_department IS NOT NULL
