# Palo权限管理

## 问题
1. 当前对Palo中数据的访问权限控制仅到 DB 级别，无法更细粒度的控制到表甚至列级别的访问权限。无法满足部分用户对于数据仓库权限管理的需求。
2. 当前白名单机制和账户管理模块各自独立，而实际这些都数据权限管理模块，应该整合。
3. 用户角色无法满足云上部署需求。Root用户拥有所有权限，既可以操作集群启停，又可以访问用户数据，无法满足在云上同时进行集群管理和屏蔽用户数据访问权限的需求。

## 名词解释

1. 权限管理

    权限管理主要负责控制，对于满足特定标识的行为主体，对数据仓库中某一个（或一类）特定元素，可以执行哪些操作。比如来自 127.0.0.1 的 Root 用户（行为主体），可以对 Backend 节（元素）点进行添加或删除操作。
    
2. 账户管理

    账户管理主要负责控制，允许哪些满足特定标识的行为主体对数据仓库进行连接和操作。包括密码检查等。
    
3. 权限（Privilege）

    数据库中定义个多种权限，如Select、Insert 等，用于描述对于对应操作的权限。
    
4. 角色（Role）

    角色是一组权限的集合
    
5. 用户（User）

    用户以 Who@Where 的方式表示。如 root@127.0.0.1 表示来自 127.0.0.1，名为root的用户。用户可以被赋予权限，已可以归属于某个角色。
    
## 详细设计

### 操作 
Palo 中的操作主要分为以下几类：

1. 节点管理

    包括各类节点的添加、删除、下线等。以及对cluster的创建、删除、更改

2. 数据库管理

    包括对数据库的创建、删除及更改操作。
    
3. 表管理

    包括在数据库内，对表的创建、删除及更改操作。
    
4. 读写

    对用户数据的读写操作。
    
5. 权限操作

    包括创建、删除用户，授予、撤销权限

6. 备份恢复操作

    对数据库表的备份恢复操作。
    
### 权限

#### Node_priv

节点级别的操作权限
对应操作：Add/Drop/Decommission Frontend/Backend/Broker

#### Grant_priv

进行权限操作以及添加、删除用户的权限。拥有该权限的用户，只能对自身拥有的权限进行授予、撤回等操作。

#### Select_priv

对用户数据的读取权限。
如果赋予DB级别，则可以读取该DB下的所有数据表。如果赋予Table级别，则仅可以读取该Table中的数据。对于没有 Select\_priv 权限的表，用户无法访问，也不可见。

#### Load_priv
因为Palo目前只有导入这一种数据写入方式，所以不再区分Insert、Update、Delete这些细分权限。用户可以对有Load\_priv的表进行数据更改操作。但如果没有对该表的 Select\_priv 权限，则不可以读取该表的数据，但可以查看该表的Schema等信息。

#### Alter_priv

修改DB或者Table的权限。该权限可以修改对应DB或者Table的Schema。同时拥有查看Schema的权限。但是不能进行DB或Table的创建和删除操作。

#### Create_priv

创建DB或者Table的权限。同时拥有查看Schema的权限。

#### Drop_priv

删除DB或者Table的权限。同时拥有查看Schema的权限。

### 角色

#### Root

为了和之前的代码保持兼容，这里沿用Root这个名称。实际上，该角色为集群管理员，只拥有 Node\_priv 权限。在Palo集群创建时，默认会创建一个名为 root@'%' 的用户。该角色有且仅有一个用户，不可创建、更改或删除。

#### Superuser

该角色拥有除 Node\_priv 以外的所有权限，意味管理员角色。在Palo集群创建时，默认会创建一个 Admin@'%'的用户。因为Superuser拥有 Grant\_priv，所以可以创建其他的拥有除Node\_priv以外的任意权限的角色，包括创建新的 Superuser。

#### Other Roles

Root 和 Superuser 角色为保留角色。用户可以自定义其他角色。其他角色不可包含 Node\_priv 和 Grant\_priv 权限。

## 数据结构

参照Mysql的权限表组织方式。我们生成默认的名为mysql数据库，其中包含以下数据表

1. user

| Host | User | Password | Node\_priv | Grant\_priv | Select\_priv | Load\_priv | Alter\_priv | Create\_priv | Drop\_priv |
|---|---|---|---|---|---|---|---|---|---|---|
| % | root |*B371DC178FD00FA65915DBC87B05C7E88E3BE66A| Y | N | N | N | N | N | N | N |

Host 和 User 标识连接主体，通过 Host、User、Password 来验证连接是否合法。  
该表中的权限问全局权限（Global Priv），全局权限可以作用于任何数据库中的任何表。

2. db

| Host | Db | User | Select\_priv | Load\_priv | Alter\_priv | Create\_priv | Drop\_priv |
|---|---|---|---|---|---|---|---|---|---|---|
| % | example_db | cmy | Y | Y | N | N | N |

该表用于存储DB级别的权限，通过 Host、DB、User 匹配DB级别的权限。

3. tables_priv

| Host | Db | User | Table_name| Table_priv | Column_priv |
|---|---|---|---|---|---|---|---|---|---|---|
| % | example_db | cmy | example_tbl | Select\_priv/Load\_priv/Alter\_priv/Create\_priv/Drop\_priv | Not support yet |

## 权限检查规则

### 连接

通过 User 表进行连接鉴权。User 表按照 Host、User 列排序，取最先匹配到的Row作为授予权限。

### 请求

连接成功后，用户后续发送的请求需要进行鉴权。首先在User表中查看全局权限，如果没有找到，则再一次查找 db、tables_priv 表。当然如 Node\_priv 这种权限只会出现在 User 表中，则不再继续后面的查找了。


## 语法

### 创建用户

```
CREATE USER [IF NOT EXIST] 'user_name'@'host' IDENTIFIED BY 'password' DEFAULT ROLE 'role_name';
```

1. user\_name: 用户名
2. host: 可以是ip、hostname。允许通配符，如："%", "192.168.%", "%.baidu.com"（如何解析BNS？？）
3. 如果设置了 default role，则直接赋予default role 对应的权限。

### 删除用户

```
DROP USER 'user_name';
```

删除对应名称的用户，该用户的所有对应host的权限信息都会被删除。


### 设置密码
```
SET PASSWORD FOR 'user_name'@'host' = PASSWORD('xxxx');
```

### 授权

```
GRANT PRIVILEGE 'priv1'[, 'priv2', ...] on `db`.`tbl` to 'user'@'host'[ROLE 'role'];
```

```
GRANT ROLE 'role1' on `db`.`tbl` to 'user'@'host';
```

1. db 和 tbl 可以是通配符，如：\*.\*, db.* 。 Palo不检查执行授权时，db或tbl是否存在。只是更新授权表。
2. Node\_priv 和 Grant\_priv 不能通过该语句授权。不能授予 all privileges
3. 如果是授权给 Role，且Role不存在，会自动创建这个 Role。如果user不存在，则报错。
4. 可以授权 ROLE 给某一个user。

### 撤权

```
REVOKE 'priv1'[, 'priv2', ...]|[all privileges] on `db`.`tbl` from 'user'@'host'[ROLE 'role'];
```

1. db 和 tbl 可以是通配符，Palo会匹配权限表中的所有可匹配项，并撤销对应权限
2. 该语句中的 host 会进行精确匹配，而不是通配符。

### 创建角色

```
CREATE ROLE 'role';
```

1. 创建角色后，可以通过授权语句或撤权语句对该 Role 的权限进行修改。
2. 默认有 ROOT 和 SUPERUSER 两个 ROLE。这两个ROLE的权限不可修改。


## 最佳实践

1. 创建集群后，会自动创建 ROOT@'%' 和 ADMIN@'%' 两个用户。以及 ROOT 和 SUPERUSER 两个角色。 ROOT@'%' 为 ROOT 角色。ADMIN@'%' 为 SUPERUSER 角色。
2. ROOT 角色仅拥有 Node_priv。SUPERUSER 角色拥有其他所有权限。
3. ROOT@'%' 主要由于运维人员搭建系统。或者用于云上的部署程序。
4. SUPERUSER 角色的用户为数据库的实际使用者和管理员。
5. superuser 用户可以创建各个普通用户。比如给外包人员创建 Alter_priv、Load_priv、Create_priv、Drop_priv 权限，用于数据创建和导入，但没有Select_priv。给用户开通 Select_priv，可以查看数据，但不能对数据进行修改。
6. 初始时，只有 superuser 可以创建db。或者可以先授权某个普通用户对一个**不存在的DB**的创建权限，之后，该普通用户就可以创建数据库了。（这是一个先有鸡还是先有蛋的问题）
7. 任何对数据库内对象（DB、Table）的创建和删除都不会影响已经存在的权限。如果有必要，必须手动修改权限表对应的条目。


## 排期
预计6月底完成，本期实现表级别的权限管理。

## 遗留问题
1. 白名单，如何解决bns和dns的问题
    保留当前通过 add whitelist 的方式添加白名单的机制。通过这个机制添加的白名单，默认都是DNS或BNS。Palo会有后台线程定期解析这些域名，将解析出来的host或ip加入到权限列表中。加入权限表时，取此user在db权限表中，各db最大权限集合。
    
    这种方式，能够兼容之前创建的用户权限。但存在一些问题，假设用户之前的权限为：
    
    GRANT ALL on db1 to user;
    GRANT ALL on db2 to user;
    
    如果该user的dns解析后的host有10个，那么更新后的 db 权限表中，会产生 10 * 2 个条目。但我们认为当前数量级不会影响性能。
    


2. 自动生成的 ROOT@'%' 和 ADMIN@'%'，如何更改其中的 host


## 权限逻辑

### CREATE USER

1. create user cmy@'ip' identified by '12345';

    检查 cmy@'ip' 是否存在，如果存在则报错。
    
2. create user cmy@'ip' identified by '12345' default role role1;

    检查 cmy@'ip' 是否存在，如果存在则报错。
    赋予 cmy@'ip' role1 的所有权限，即时生效。
    role1 中加入 cmy@'ip'。
    
3. create user cmy@['domian'] identified by '12345';

    在白名单中检查 cmy@['domian'] 是否存在，如果存在则报错。

4. create user cmy@['domian'] identified by '12345' default role role1;

    在白名单中检查 cmy@['domian'] 是否存在，如果存在则报错。
    赋予该白名单，role1的所有权限。后台线程轮询生效。
    role1 中加入 cmy@['domian']
    
### GRANT

1. grant select on \*.* to cmy@'ip';

    检查 cmy@'ip' 是否存在，不存在则报错。
    将 select 权限赋给 cmy@'ip' on \*.*
    
2. grant select on \*.* to cmy@['domain'];

    检查 cmy@['domain'] 是否存在，不存在则报错。
    将 select 权限赋给 cmy@['domain'] on \*.*

3. grant select on \*.* to ROLE role1;

    检查 role1 是否存在，不存在则报错。
    将 select 权限赋给 role1  on \*.*。
    将 select 权限赋给所有 role1 的用户。
    






