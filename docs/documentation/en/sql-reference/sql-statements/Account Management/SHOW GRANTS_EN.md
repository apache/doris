# SHOW GRANTS
## Description

This statement is used to view user rights.

Grammar:
SHOW [ALL] GRANTS [FOR user_identity];

Explain:
1. SHOW ALL GRANTS can view the privileges of all users.
2. If you specify user_identity, view the permissions of the specified user. And the user_identity must be created for the CREATE USER command.
3. If you do not specify user_identity, view the permissions of the current user.


## example

1. View all user rights information

SHOW ALL GRANTS;

2. View the permissions of the specified user

SHOW GRANTS FOR jack@'%';

3. View the permissions of the current user

SHOW GRANTS;

## keyword
SHOW, GRANTS
