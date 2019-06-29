# 权限管理

Doris 新的权限管理系统参照了 Mysql 的权限管理机制，做到了表级别细粒度的权限控制，基于角色的权限访问控制，并且支持白名单机制。

## 名词解释

1. 用户标识 user_identity

    在权限系统中，一个用户被识别为一个 User Identity（用户标识）。用户标识由两部分组成：username 和 userhost。其中 username 为用户名，由英文大小写组成。userhost 表示该用户链接来自的 IP。user_identity 以 username@'userhost' 的方式呈现，表示来自 userhost 的 username。
    
    user_identity 的另一种表现方式为 username@['domain']，其中 domain 为域名，可以通过 DNS 会 BNS（百度名字服务）解析为一组 ip。最终表现为一组 username@'userhost'，所以后面我们统一使用 username@'userhost' 来表示。
    
2. 权限 Privilege

    权限作用的对象是节点、数据库或表。不同的权限代表不同的操作许可。
    
3. 角色 Role

    Doris可以创建自定义命名的角色。角色可以被看做是一组权限的集合。新创建的用户可以被赋予某一角色，则自动被赋予该角色所拥有的权限。后续对角色的权限变更，也会体现在所有属于该角色的用户权限上。
    
4. 用户属性 user_property

    用户属性直接附属于某一用户，而不是用户标识。即 cmy@'192.%' 和 cmy@['domain'] 都拥有同一组用户属性，该属性属于用户 cmy，而不是 cmy@'192.%' 或 cmy@['domain']。
    
    用户属性包括但不限于： 用户最大连接数、导入集群配置等等。

## 支持的操作

1. 创建用户：CREATE USER
2. 删除用户：DROP USER
3. 授权：GRANT
4. 撤权：REVOKE
5. 创建角色：CREATE ROLE
6. 删除角色：DROP ROLE
7. 查看当前用户权限：SHOW GRANTS
8. 查看所有用户权限：SHOW ALL GRANTS
9. 查看已创建的角色：SHOW ROELS
10. 查看用户属性：SHOW PROPERTY

关于以上命令的详细帮助，可以通过 mysql 客户端连接 Doris 后，使用 help + command 获取帮助。如 `help create user`。

## 权限说明

Doris 目前支持以下几种权限

1. Node_priv

    节点变更权限。包括 FE、BE、BROKER 节点的添加、删除、下线等操作。目前该权限只能授予 Root 用户。

2. Grant_priv

    权限变更权限。允许执行包括授权、撤权、添加/删除/变更 用户/角色 等操作。

3. Select_priv

    对数据库、表的只读权限。

4. Load_priv

    对数据库、表的写权限。包括 Load、Insert、Delete 等。

5. Alter_priv

   对数据库、表的更改权限。包括重命名 库/表、添加/删除/变更 列等操作。

6. Create_priv

   创建数据库、表的权限。

7. Drop_priv

    删除数据库、表的权限。

## 一些说明

1. Doris 初始化时，会自动创建如下用户和角色：

    1. operator 角色：该角色拥有 Node\_priv 和 Admin\_priv，即对Doris的所有权限。后续某个升级版本中，我们可能会将该角色的权限限制为 Node\_priv，即仅授予节点变更权限。以满足某些云上部署需求。
    
    2. admin 角色：该角色拥有 Admin\_priv，即除节点变更以外的所有权限。

    3. root@'%'：root 用户，允许从任意节点登陆，角色为 operator。

    4. admin@'%'：admin 用户，允许从任意节点登陆，角色为 admin。

2. 不支持删除或更改默认创建的角色或用户的权限。
    
3. operator 角色的用户有且只有一个。admin 角色的用户可以创建多个。

4. 一些可能产生冲突的操作说明

    1. 域名与ip冲突：
    
        假设创建了如下用户：
    
        CREATE USER cmy@['domain'];
    
        并且授权：
        
        GRANT SELECT_PRIV ON \*.\* TO cmy@['domain']
        
        该 domain 被解析为两个 ip：ip1 和 ip2
        
        假设之后，我们对 cmy@'ip1' 进行一次单独授权：
        
        GRANT ALTER_PRIV ON \*.\* TO cmy@'ip1';
        
        则 cmy@'ip1' 的权限会被修改为 SELECT\_PRIV, ALTER\_PRIV。并且当我们再次变更 cmy@['domain'] 的权限时，cmy@'ip1' 也不会跟随改变。
        
    2. 重复ip冲突：

        假设创建了如下用户：
    
        CREATE USER cmy@'%' IDENTIFIED BY "12345";
        
        CREATE USER cmy@'192.%' IDENTIFIED BY "abcde";
        
        在优先级上，'192.%' 优先于 '%'，因此，当用户 cmy 从 192.168.1.1 这台机器尝试使用密码 '12345' 登陆 Doris 会被拒绝。
        
    3. 权限冲突

5. 忘记密码

    如果忘记了密码无法登陆 Doris，可以在 Doris FE 节点所在机器，使用如下命令无密码登陆 Doris：
    
    `mysql-client -h 127.0.0.1 -P query_port -uroot`
    
    登陆后，可以通过 SET PASSWORD 命令重置密码。

6. 任何用户都不能重置 root 用户的密码，除了 root 用户自己。

7. 拥有 GRANT 权限的用户可以设置密码。如果没有 GRANT 用户，则用户仅可以设置自己对应的 UserIdentity 的密码。自己对应的 UserIdentity 可以通过 `SELECT CURRENT_USER();` 命令查看。

8. ADMIN\_PRIV 和 GRANT\_PRIV 权限只能 `GRANT ON *.*` 或 `REVOKE FROM *.*`。因为对于指定的库和表，这两个权限没有意义。同时，拥有 GRANT_PRIV 其实等同于拥有 ADMIN\_PRIV，因为 GRANT\_PRIV 有授予任意权限的权限，请谨慎使用。
