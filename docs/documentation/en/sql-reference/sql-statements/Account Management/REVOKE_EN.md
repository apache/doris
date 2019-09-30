# REVOKE
## Description

The REVOKE command is used to revoke the rights specified by the specified user or role.
Syntax
REVOKE privilege_list ON db_name[.tbl_name] FROM user_identity [ROLE role_name]

user_identity：

The user_identity syntax here is the same as CREATE USER. And you must create user_identity for the user using CREATE USER. The host in user_identity can be a domain name. If it is a domain name, the revocation time of permission may be delayed by about one minute.

You can also revoke the permission of the specified ROLE, which must exist for execution.

## example

1. Revoke the rights of user Jack database testDb

REVOKE SELECT_PRIV ON db1.* FROM 'jack'@'192.%';

## keyword

REVOKE
