# DROP USER
## description

Syntax:

    DROP USER 'user_name'

    DROP USER 命令会删除一个 palo 用户。这里 Doris 不支持删除指定的 user_identity。当删除一个指定用户后，该用户所对应的所有 user_identity 都会被删除。比如之前通过 CREATE USER 语句创建了 jack@'192.%' 以及 jack@['domain'] 两个用户，则在执行 DROP USER 'jack' 后，jack@'192.%' 以及 jack@['domain'] 都将被删除。

## example

1. 删除用户 jack
   
    DROP USER 'jack'

## keyword

    DROP, USER

