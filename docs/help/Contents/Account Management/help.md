# CREATE USER
## description

    Syntax:
    CREATE USER user_specification [SUPERUSER]

    user_specification:
        'user_name' [IDENTIFIED BY [PASSWORD] 'password']

    CREATE USER 命令可用于创建一个 palo 用户，使用这个命令需要使用者必须有管理员权限
    SUPERUSER用于指定需要创建的用户是个超级用户

## example

   1. 创建一个没有密码的用户，用户名为 jack
   CREATE USER 'jack'

   2. 创建一个带有密码的用户，用户名为 jack，并且密码被指定为 123456
   CREATE USER 'jack' IDENTIFIED BY '123456'

   3. 为了避免传递明文，用例2也可以使用下面的方式来创建
   CREATE USER 'jack' IDENTIFIED BY PASSWORD '*6BB4837EB74329105EE4568DDA7DC67ED2CA2AD9'
   后面加密的内容可以通过PASSWORD()获得到,例如：
   SELECT PASSWORD('123456')

   4. 创建一个超级用户'jack'
   CREATE USER 'jack' SUPERUSER

## keyword
    CREATE, USER

# DROP USER
## description

    Syntax:
    DROP USER 'user_name'

    DROP USER 命令会删除一个 palo 用户，使用这个命令需要使用者必须有管理员权限

## example
   1. 删除用户 jack
   DROP USER 'jack'

## keyword
    DROP, USER

# SET PASSWORD
## description

    Syntax:
    SET PASSWORD [FOR 'user_name'] = 
    [PASSWORD('plain password')]|['hashed password']

    SET PASSWORD 命令可以用于修改一个用户的登录密码。如果 [FOR 'user_name'] 字段不存在，那么修改当前用户的密码。
    PASSWORD() 方式输入的是明文密码; 而直接使用字符串，需要传递的是已加密的密码。
    如果修改其他用户的密码，需要具有管理员权限。

## example

   1. 修改当前用户的密码为 123456
   SET PASSWORD = PASSWORD('123456')
   SET PASSWORD = '*6BB4837EB74329105EE4568DDA7DC67ED2CA2AD9'

   2. 修改用户 jack 的密码为 123456
   SET PASSWORD FOR 'jack' = PASSWORD('123456')
   SET PASSWORD FOR 'jack' = '*6BB4837EB74329105EE4568DDA7DC67ED2CA2AD9'

## keyword
    SET, PASSWORD

# GRANT
## description
    GRANT 命令是将一个数据库的具体权限授权给具体用户。调用者必须是管理员身份。
    权限当前只包括只读 (READ_ONLY)，读写 (READ_WRITE) 两种权限，如果指定为 ALL，
    那么就是将全部权限授予该用户。

    Syntax:
    GRANT privilege_list ON db_name TO 'user_name'

    privilege_list: 
        privilege [, privilege] ...

    privilege:
        READ_ONLY
        | READ_WRITE
        | ALL

## example

   1. 授予用户 jack 数据库 testDb 的写权限
   GRANT READ_ONLY ON testDb to 'jack';

   2. 授予用户 jack 数据库 testDb 全部权限
   GRANT ALL ON testDb to 'jack';

## keyword
   GRANT
   
# REVOKE
## description
    REVOKE 命令用于撤销用户对于某一个数据库的权限（当前只支持撤销所有权限）
    语法：
        REVOKE ALL ON db_name FROM 'user_name'
        
## example
   1. 撤销用户 jack 数据库 testDb 的权限
   REVOKE ALL ON testDb FROM 'jack';

## keyword
   REVOKE

# SET PROPERTY
## description
   Syntax:
   SET PROPERTY [FOR 'user'] 'key' = 'value' [, 'key' = 'value']

   设置用户的属性，包括分配给用户的资源、导入cluster等。

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
        load_cluster.{cluster_name}.hadoop_http_port: hadoop hdfs name node http端口，默认为8070。
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
    SET PROPERTY FOR 'jack' 'load_cluster.{cluster_name}' = NULL;

    6. 修改用户 jack 默认的导入cluster
    SET PROPERTY FOR 'jack' 'default_load_cluster' = '{cluster_name}';
    
    7. 修改用户 jack 的集群优先级为 HIGH
    SET PROPERTY FOR 'jack' 'load_cluster.{cluster_name}.priority' = 'HIGH';

## keyword
    SET, PROPERTY

# SHOW USER
## description
    用于显示当前用户有权限查看的所有用户的权限信息。
    如果是普通用户，仅可以查看自己的权限信息。
    如果是 superuser 用户，可以看到自己所属 cluster 的所有用户的权限信息。
    如果是 admin（root）用户，可以看到所有用户的权限信息。
    语法：
        SHOW USER;
        
## example
   1. 查看用户权限信息
   SHOW USER;

## keyword
   SHOW,USER
