# CANCEL LOAD
Description

This statement is used to undo the import job for the batch of the specified load label.
This is an asynchronous operation, which returns if the task is submitted successfully. After execution, you can use the SHOW LOAD command to view progress.
Grammar:
CANCEL LOAD
[FROM both names]
WHERE LABEL = "load_label";

'35;'35; example

1. Revoke the import job of example_db_test_load_label on the database example_db
CANCEL LOAD
FROM example_db
WHERE LABEL = "example_db_test_load_label";

## keyword
CANCEL,LOAD
