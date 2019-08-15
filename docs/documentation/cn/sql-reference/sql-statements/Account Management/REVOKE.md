# REVOKE
## description

    REVOKE 命令用于撤销指定用户或角色指定的权限。
    Syntax：
        REVOKE privilege_list ON db_name[.tbl_name] FROM user_identity [ROLE role_name]
        
    user_identity：

    这里的 user_identity 语法同 CREATE USER。且必须为使用 CREATE USER 创建过的 user_identity。user_identity 中的host可以是域名，如果是域名的话，权限的撤销时间可能会有1分钟左右的延迟。
    
    也可以撤销指定的 ROLE 的权限，执行的 ROLE 必须存在。
        
## example

    1. 撤销用户 jack 数据库 testDb 的权限
   
        REVOKE SELECT_PRIV ON db1.* FROM 'jack'@'192.%';

## keyword

    REVOKE

