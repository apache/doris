SELECT
*
FROM
(
SELECT
table_1.abcd AS exist_key,
table_1.create_time AS business_time
FROM
table_1
) source
RIGHT JOIN (
SELECT
table_2.abcd AS exist_key,
table_2.create_time AS business_time
FROM
table_2
) target ON source.exist_key = target.exist_key order by target.exist_key;
