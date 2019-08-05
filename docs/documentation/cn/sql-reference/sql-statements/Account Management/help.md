# CREATE USER
## description

Syntax:
    
    CREATE USER user_identity [IDENTIFIED BY 'password'] [DEFAULT ROLE 'role_name']

    user_identity:
        'user_name'@'host'
        
CREATE USER 命令用于创建一个 Doris 用户。在 Doris 中，一个 user_identity 唯一标识一个用户。user_identity 由两部分组成，user_name 和 host，其中 username 为用户名。host 标识用户端连接所在的主机地址。host 部分可以使用 % 进行模糊匹配。如果不指定 host，默认为 '%'，即表示该用户可以从任意 host 连接到 Doris。
    
host 部分也可指定为 domain，语法为：'user_name'@['domain']，即使用中括号包围，则 Doris 会认为这个是一个 domain，并尝试解析其 ip 地址。目前仅支持百度内部的 BNS 解析。
    
如果指定了角色（ROLE），则会自动将该角色所拥有的权限赋予新创建的这个用户。如果不指定，则该用户默认没有任何权限。指定的 ROLE 必须已经存在。

## example

1. 创建一个无密码用户（不指定 host，则等价于 jack@'%'）
   
    CREATE USER 'jack';

2. 创建一个有密码用户，允许从 '172.10.1.10' 登陆
   
    CREATE USER jack@'172.10.1.10' IDENTIFIED BY '123456';

3. 为了避免传递明文，用例2也可以使用下面的方式来创建
   
    CREATE USER jack@'172.10.1.10' IDENTIFIED BY PASSWORD '*6BB4837EB74329105EE4568DDA7DC67ED2CA2AD9';
   
    后面加密的内容可以通过PASSWORD()获得到,例如：
    
    SELECT PASSWORD('123456');

4. 创建一个允许从 '192.168' 子网登陆的用户，同时指定其角色为 example_role
   
    CREATE USER 'jack'@'192.168.%' DEFAULT ROLE 'example_role';
        
5. 创建一个允许从域名 'example_domain' 登陆的用户

    CREATE USER 'jack'@['example_domain'] IDENTIFIED BY '12345';

6. 创建一个用户，并指定一个角色

    CREATE USER 'jack'@'%' IDENTIFIED BY '12345' DEFAULT ROLE 'my_role';

## keyword
    CREATE, USER

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

# GRANT
## description

GRANT 命令用于赋予指定用户或角色指定的权限。

Syntax:

    GRANT privilege_list ON db_name[.tbl_name] TO user_identity [ROLE role_name]


privilege_list 是需要赋予的权限列表，以逗号分隔。当前 Doris 支持如下权限：

    NODE_PRIV：集群节点操作权限，包括节点上下线等操作，只有 root 用户有该权限，不可赋予其他用户。
    ADMIN_PRIV：除 NODE_PRIV 以外的所有权限。
    GRANT_PRIV: 操作权限的权限。包括创建删除用户、角色，授权和撤权，设置密码等。
    SELECT_PRIV：对指定的库或表的读取权限
    LOAD_PRIV：对指定的库或表的导入权限
    ALTER_PRIV：对指定的库或表的schema变更权限
    CREATE_PRIV：对指定的库或表的创建权限
    DROP_PRIV：对指定的库或表的删除权限
    
    旧版权限中的 ALL 和 READ_WRITE 会被转换成：SELECT_PRIV,LOAD_PRIV,ALTER_PRIV,CREATE_PRIV,DROP_PRIV；
    READ_ONLY 会被转换为 SELECT_PRIV。

db_name[.tbl_name] 支持以下三种形式：

    1. *.* 权限可以应用于所有库及其中所有表
    2. db.* 权限可以应用于指定库下的所有表
    3. db.tbl 权限可以应用于指定库下的指定表

    这里指定的库或表可以是不存在的库和表。

user_identity：

    这里的 user_identity 语法同 CREATE USER。且必须为使用 CREATE USER 创建过的 user_identity。user_identity 中的host可以是域名，如果是域名的话，权限的生效时间可能会有1分钟左右的延迟。
    
    也可以将权限赋予指定的 ROLE，如果指定的 ROLE 不存在，则会自动创建。

## example

    1. 授予所有库和表的权限给用户
   
        GRANT SELECT_PRIV ON *.* TO 'jack'@'%';

    2. 授予指定库表的权限给用户

        GRANT SELECT_PRIV,ALTER_PRIVS,LOAD_PRIV ON db1.tbl1 TO 'jack'@'192.8.%';
        
    3. 授予指定库表的权限给角色

        GRANT LOAD_PRIV ON db1.* TO ROLE 'my_role';

## keyword
   GRANT
   
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

# SET PROPERTY
## description

   Syntax:

    SET PROPERTY [FOR 'user'] 'key' = 'value' [, 'key' = 'value']

    设置用户的属性，包括分配给用户的资源、导入cluster等。这里设置的用户属性，是针对 user 的，而不是 user_identity。即假设通过 CREATE USER 语句创建了两个用户 'jack'@'%' 和 'jack'@'192.%'，则使用 SET PROPERTY 语句，只能针对 jack 这个用户，而不是 'jack'@'%' 或 'jack'@'192.%'

    导入 cluster 仅适用于百度内部用户。

   key:
   
    超级用户权限:
        max_user_connections: 最大连接数。
        resource.cpu_share: cpu资源分配。
        load_cluster.{cluster_name}.priority: 为指定的cluster分配优先级，可以为 HIGH 或 NORMAL

    普通用户权限：
        quota.normal: normal级别的资源分配。
        quota.high: high级别的资源分配。
        quota.low: low级别的资源分配。

        load_cluster.{cluster_name}.hadoop_palo_path: palo使用的hadoop目录，需要存放etl程序及etl生成的中间数据供palo导入。导入完成后会自动清理中间数据，etl程序自动保留下次使用。    
        load_cluster.{cluster_name}.hadoop_configs: hadoop的配置，其中fs.default.name、mapred.job.tracker、hadoop.job.ugi必须填写。
        load_cluster.{cluster_name}.hadoop_http_port: hadoop hdfs name node http端口。其中 hdfs 默认为8070，afs 默认 8010。
        default_load_cluster: 默认的导入cluster。

## example

    1. 修改用户 jack 最大连接数为1000
    SET PROPERTY FOR 'jack' 'max_user_connections' = '1000';

    2. 修改用户 jack 的cpu_share为1000
    SET PROPERTY FOR 'jack' 'resource.cpu_share' = '1000';

    3. 修改 jack 用户的normal组的权重
    SET PROPERTY FOR 'jack' 'quota.normal' = '400';

    4. 为用户 jack 添加导入cluster 
    SET PROPERTY FOR 'jack' 
        'load_cluster.{cluster_name}.hadoop_palo_path' = '/user/palo/palo_path', 
        'load_cluster.{cluster_name}.hadoop_configs' = 'fs.default.name=hdfs://dpp.cluster.com:port;mapred.job.tracker=dpp.cluster.com:port;hadoop.job.ugi=user,password;mapred.job.queue.name=job_queue_name_in_hadoop;mapred.job.priority=HIGH;';

    5. 删除用户 jack 下的导入cluster。
    SET PROPERTY FOR 'jack' 'load_cluster.{cluster_name}' = '';

    6. 修改用户 jack 默认的导入cluster
    SET PROPERTY FOR 'jack' 'default_load_cluster' = '{cluster_name}';
    
    7. 修改用户 jack 的集群优先级为 HIGH
    SET PROPERTY FOR 'jack' 'load_cluster.{cluster_name}.priority' = 'HIGH';

## keyword
    SET, PROPERTY
    
# CREATE ROLE

## description
    该语句用户创建一个角色
    
    语法：
        CREATE ROLE role1;
        
    该语句创建一个无权限的角色，可以后续通过 GRANT 命令赋予该角色权限。
    
## example

    1. 创建一个角色
   
        CREATE ROLE role1;
        
## keyword
   CREATE, ROLE
   
   
# DROP ROLE

## description
    该语句用户删除一个角色
    
    语法：
        DROP ROLE role1;
        
    删除一个角色，不会影响之前属于该角色的用户的权限。仅相当于将该角色与用户解耦。用户已经从该角色中获取到的权限，不会改变。
    
## example

    1. 删除一个角色
   
        DROP ROLE role1;
        
## keyword
   DROP, ROLE     

# SHOW ROLES

## description
    该语句用于展示所有已创建的角色信息，包括角色名称，包含的用户以及权限。

    语法：
        SHOW ROLES;

## example

    1. 查看已创建的角色：

        SHOW ROLES;

## keyword
    SHOW,ROLES

## description
    该语句用户删除一个角色
    
    语法：
        DROP ROLE role1;
        
    删除一个角色，不会影响之前属于该角色的用户的权限。仅相当于将该角色与用户解耦。用户已经从该角色中获取到的权限，不会改变。
    
## example

    1. 删除一个角色
   
        DROP ROLE role1;
        
## keyword
   DROP, ROLE     

# SHOW GRANTS

## description
    
    该语句用于查看用户权限。
    
    语法：
        SHOW [ALL] GRANTS [FOR user_identity];
        
    说明：
        1. SHOW ALL GRANTS 可以查看所有用户的权限。
        2. 如果指定 user_identity，则查看该指定用户的权限。且该 user_identity 必须为通过 CREATE USER 命令创建的。
        3. 如果不指定 user_identity，则查看当前用户的权限。
    
        
## example

    1. 查看所有用户权限信息
   
        SHOW ALL GRANTS;
        
    2. 查看指定 user 的权限

        SHOW GRANTS FOR jack@'%';
        
    3. 查看当前用户的权限

        SHOW GRANTS;

## keyword
   SHOW, GRANTS
