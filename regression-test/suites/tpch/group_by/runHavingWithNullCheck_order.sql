SELECT first_name, COUNT(*) FROM workers GROUP BY first_name HAVING first_name IS NULL
