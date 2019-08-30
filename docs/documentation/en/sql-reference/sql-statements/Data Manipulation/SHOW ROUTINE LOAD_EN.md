# SHOW ROUTINE LOAD
## example

1. Show all routine import jobs named test 1 (including stopped or cancelled jobs). The result is one or more lines.

SHOW ALL ROUTINE LOAD FOR test1;

2. Show the current running routine import job named test1

SHOW ROUTINE LOAD FOR test1;

3. Display all routine import jobs (including stopped or cancelled jobs) under example_db. The result is one or more lines.

use example_db;
SHOW ALL ROUTINE LOAD;

4. Display all running routine import jobs under example_db

use example_db;
SHOW ROUTINE LOAD;

5. Display the current running routine import job named test1 under example_db

SHOW ROUTINE LOAD FOR example_db.test1;

6. Display all routine import jobs named test1 (including stopped or cancelled jobs) under example_db. The result is one or more lines.

SHOW ALL ROUTINE LOAD FOR example_db.test1;

## keyword
SHOW,ROUTINE,LOAD
