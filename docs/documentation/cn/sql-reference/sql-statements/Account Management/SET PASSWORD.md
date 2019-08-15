# SET PASSWORD
## description

Syntax:

    SET PASSWORD [FOR user_identity] = 
    [PASSWORD('plain password')]|['hashed password']

    SET PASSWORD 命令可以用于修改一个用户的登录密码。如果 [FOR user_identity] 字段不存在，那么修改当前用户的密码。
    
    注意这里的 user_identity 必须完全匹配在使用 CREATE USER 创建用户时指定的 user_identity，否则会报错用户不存在。如果不指定 user_identity，则当前用户为 'username'@'ip'，这个当前用户，可能无法匹配任何 user_identity。可以通过 SHOW GRANTS 查看当前用户。
    
    PASSWORD() 方式输入的是明文密码; 而直接使用字符串，需要传递的是已加密的密码。
    如果修改其他用户的密码，需要具有管理员权限。

## example

1. 修改当前用户的密码
      
    SET PASSWORD = PASSWORD('123456')
    SET PASSWORD = '*6BB4837EB74329105EE4568DDA7DC67ED2CA2AD9'

2. 修改指定用户密码
   
    SET PASSWORD FOR 'jack'@'192.%' = PASSWORD('123456')
    SET PASSWORD FOR 'jack'@['domain'] = '*6BB4837EB74329105EE4568DDA7DC67ED2CA2AD9'

## keyword
    SET, PASSWORD

