# DROP USER
## Description

Syntax:

DROP USER 'user_name'

The DROP USER command deletes a Palo user. Doris does not support deleting the specified user_identity here. When a specified user is deleted, all user_identities corresponding to that user are deleted. For example, two users, Jack @'192%'and Jack @['domain'] were created through the CREATE USER statement. After DROP USER'jack' was executed, Jack @'192%'and Jack @['domain'] would be deleted.

## example

1. Delete user jack

DROP USER 'jack'

## keyword
DROP, USER
