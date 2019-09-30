# SET PASSWORD
## Description

Syntax:

SET PASSWORD [FOR user_identity] =
[PASSWORD('plain password')]|['hashed password']

The SET PASSWORD command can be used to modify a user's login password. If the [FOR user_identity] field does not exist, modify the password of the current user.

Note that the user_identity here must match exactly the user_identity specified when creating a user using CREATE USER, otherwise the user will be reported as non-existent. If user_identity is not specified, the current user is'username'@'ip', which may not match any user_identity. The current user can be viewed through SHOW GRANTS.

PASSWORD () input is a plaintext password, and direct use of strings, you need to pass the encrypted password.
If you change the password of other users, you need to have administrator privileges.

## example

1. Modify the password of the current user

SET PASSWORD = PASSWORD('123456')
SET PASSWORD = '*6BB4837EB74329105EE4568DDA7DC67ED2CA2AD9'

2. Modify the specified user password

SET PASSWORD FOR 'jack'@'192.%' = PASSWORD('123456')
SET PASSWORD FOR 'jack'@['domain'] = '*6BB4837EB74329105EE4568DDA7DC67ED2CA2AD9'

## keyword
SET, PASSWORD
