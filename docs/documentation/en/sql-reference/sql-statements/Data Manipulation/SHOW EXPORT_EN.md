# SHOW EXPORT
## Description
This statement is used to show the execution of the specified export task
Grammar:
SHOW EXPORT
[FROM both names]
[
WHERE
[EXPORT_JOB_ID = your_job_id]
[STATE = ["PENDING"|"EXPORTING"|"FINISHED"|"CANCELLED"]]
]
[ORDER BY ...]
[LIMIT limit];

Explain:
1) If db_name is not specified, use the current default DB
2) If STATE is specified, the EXPORT state is matched
3) Any column combination can be sorted using ORDER BY
4) If LIMIT is specified, the limit bar matching record is displayed. Otherwise, all of them will be displayed.

## example
1. Show all export tasks of default DB
SHOW EXPORT;

2. Show the export tasks of the specified db, sorted in descending order by StartTime
SHOW EXPORT FROM example_db ORDER BY StartTime DESC;

3. Show the export task of the specified db, state is "exporting" and sorted in descending order by StartTime
SHOW EXPORT FROM example_db WHERE STATE = "exporting" ORDER BY StartTime DESC;

4. Show the export task of specifying dB and job_id
SHOW EXPORT FROM example_db WHERE EXPORT_JOB_ID = job_id;

## keyword
SHOW,EXPORT

