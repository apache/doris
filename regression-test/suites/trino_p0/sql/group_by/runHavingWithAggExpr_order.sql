SELECT COUNT(*) FROM workers HAVING SUM(salary * 2)/COUNT(*) > 0
