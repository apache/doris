# ALTER SYSTEM
## description

    该语句用于操作一个系统内的节点。（仅管理员使用！）
    语法：
        1) 增加节点(不使用多租户功能则按照此方法添加)
            ALTER SYSTEM ADD BACKEND "host:heartbeat_port"[,"host:heartbeat_port"...];
        2) 增加空闲节点(即添加不属于任何cluster的BACKEND)
            ALTER SYSTEM ADD FREE BACKEND "host:heartbeat_port"[,"host:heartbeat_port"...];
        3) 增加节点到某个cluster
            ALTER SYSTEM ADD BACKEND TO cluster_name "host:heartbeat_port"[,"host:heartbeat_port"...];
        4) 删除节点
            ALTER SYSTEM DROP BACKEND "host:heartbeat_port"[,"host:heartbeat_port"...];
        5) 节点下线
            ALTER SYSTEM DECOMMISSION BACKEND "host:heartbeat_port"[,"host:heartbeat_port"...];
        6) 增加Broker
            ALTER SYSTEM ADD BROKER broker_name "host:port"[,"host:port"...];
        7) 减少Broker
            ALTER SYSTEM DROP BROKER broker_name "host:port"[,"host:port"...];
        8) 删除所有Broker
            ALTER SYSTEM DROP ALL BROKER broker_name
        9) 设置一个 Load error hub，用于集中展示导入时的错误信息
            ALTER SYSTEM SET LOAD ERRORS HUB PROPERTIES ("key" = "value"[, ...]);

    说明：
        1) host 可以是主机名或者ip地址
        2) heartbeat_port 为该节点的心跳端口
        3) 增加和删除节点为同步操作。这两种操作不考虑节点上已有的数据，节点直接从元数据中删除，请谨慎使用。
        4) 节点下线操作用于安全下线节点。该操作为异步操作。如果成功，节点最终会从元数据中删除。如果失败，则不会完成下线。
        5) 可以手动取消节点下线操作。详见 CANCEL DECOMMISSION
        6) Load error hub:
            当前支持两种类型的 Hub：Mysql 和 Broker。需在 PROPERTIES 中指定 "type" = "mysql" 或 "type" = "broker"。
            如果需要删除当前的 load error hub，可以将 type 设为 null。
            1) 当使用 Mysql 类型时，导入时产生的错误信息将会插入到指定的 mysql 库表中，之后可以通过 show load warnings 语句直接查看错误信息。
               
                Mysql 类型的 Hub 需指定以下参数：
                    host：mysql host
                    port：mysql port
                    user：mysql user
                    password：mysql password
                    database：mysql database
                    table：mysql table

            2) 当使用 Broker 类型时，导入时产生的错误信息会形成一个文件，通过 broker，写入到指定的远端存储系统中。须确保已经部署对应的 broker
                Broker 类型的 Hub 需指定以下参数：
                    broker: broker 的名称
                    path: 远端存储路径
                    other properties: 其他访问远端存储所必须的信息，比如认证信息等。
        
## example

    1. 增加一个节点
        ALTER SYSTEM ADD BACKEND "host:port";

    2. 增加一个空闲节点
        ALTER SYSTEM ADD FREE BACKEND "host:port";
        
    3. 删除两个节点
        ALTER SYSTEM DROP BACKEND "host1:port", "host2:port";
        
    4. 下线两个节点
        ALTER SYSTEM DECOMMISSION BACKEND "host1:port", "host2:port";

    5. 增加两个Hdfs Broker
        ALTER SYSTEM ADD BROKER hdfs "host1:port", "host2:port";

    6. 添加一个 Mysql 类型的 load error hub
        ALTER SYSTEM SET LOAD ERRORS HUB PROPERTIES
        ("type"= "mysql",
         "host" = "192.168.1.17"
         "port" = "3306",
         "user" = "my_name",
         "password" = "my_passwd",
         "database" = "doris_load",
         "table" = "load_errors"
        );

    7. 添加一个 Broker 类型的 load error hub
        ALTER SYSTEM SET LOAD ERRORS HUB PROPERTIES
        ("type"= "broker",
         "name" = "bos",
         "path" = "bos://backup-cmy/logs",
         "bos_endpoint" = "http://gz.bcebos.com",
         "bos_accesskey" = "069fc278xxxxxx24ddb522",
         "bos_secret_accesskey"="700adb0c6xxxxxx74d59eaa980a"
        );

    8. 删除当前的 load error hub
        ALTER SYSTEM SET LOAD ERRORS HUB PROPERTIES
        ("type"= "null");
        
## keyword
    ALTER,SYSTEM,BACKEND,BROKER,FREE

# CANCEL DECOMMISSION
## description

    该语句用于撤销一个节点下线操作。（仅管理员使用！）
    语法：
        CANCEL DECOMMISSION BACKEND "host:heartbeat_port"[,"host:heartbeat_port"...];
        
## example

    1. 取消两个节点的下线操作：
        CANCEL DECOMMISSION BACKEND "host1:port", "host2:port";

## keyword
    CANCEL,DECOMMISSION,BACKEND

# CREATE CLUSTER
## description

    该语句用于新建逻辑集群 (cluster), 需要管理员权限。如果不使用多租户，直接创建一个名称为default_cluster的cluster。否则创建一个自定义名称的cluster。

    语法

        CREATE CLUSTER [IF NOT EXISTS] cluster_name

        PROPERTIES ("key"="value", ...)
        
        IDENTIFIED BY 'password'
        
    1. PROPERTIES

        指定逻辑集群的属性

        PROPERTIES ("instance_num" = "3")

        instance_num 逻辑集群节点树

    2. identified by ‘password' 每个逻辑集群含有一个superuser，创建逻辑集群时必须指定其密码

## example

    1. 新建一个含有3个be节点逻辑集群 test_cluster, 并指定其superuser用户密码

       CREATE CLUSTER test_cluster PROPERTIES("instance_num"="3") IDENTIFIED BY 'test';

    2. 新建一个含有3个be节点逻辑集群 default_cluster(不使用多租户), 并指定其superuser用户密码

       CREATE CLUSTER default_cluster PROPERTIES("instance_num"="3") IDENTIFIED BY 'test';
  
## keyword
    CREATE,CLUSTER

# ALTER CLUSTER
## description

    该语句用于更新逻辑集群。需要有管理员权限

    语法

        ALTER CLUSTER cluster_name PROPERTIES ("key"="value", ...);

    1. 缩容，扩容 （根据集群现有的be数目，大则为扩容，小则为缩容), 扩容为同步操作，缩容为异步操作，通过backend的状态可以得知是否缩容完成

        PROERTIES ("instance_num" = "3")

        instance_num 逻辑集群节点树

## example

    1. 缩容，减少含有3个be的逻辑集群test_cluster的be数为2

        ALTER CLUSTER test_cluster PROPERTIES ("instance_num"="2");

    2. 扩容，增加含有3个be的逻辑集群test_cluster的be数为4

        ALTER CLUSTER test_cluster PROPERTIES ("instance_num"="4");

## keyword
    ALTER,CLUSTER

# DROP CLUSTER
## description

    该语句用于删除逻辑集群，成功删除逻辑集群需要首先删除集群内的db，需要管理员权限

    语法

    DROP CLUSTER [IF EXISTS] cluster_name 

## example

    删除逻辑集群 test_cluster

    DROP CLUSTER test_cluster;

## keyword
    DROP,CLUSTER

# LINK DATABASE
## description

    该语句用户链接一个逻辑集群的数据库到另外一个逻辑集群, 一个数据库只允许同时被链接一次，删除链接的数据库

    并不会删除数据，并且被链接的数据库不能被删除, 需要管理员权限

    语法

    LINK DATABASE src_cluster_name.src_db_name des_cluster_name.des_db_name 

## example

    1. 链接test_clusterA中的test_db到test_clusterB,并命名为link_test_db
    
       LINK DATABASE test_clusterA.test_db test_clusterB.link_test_db;
    
    2. 删除链接的数据库link_test_db

       DROP DATABASE link_test_db;

## keyword
    LINK,DATABASE

# MIGRATE DATABASE
## description

    该语句用于迁移一个逻辑集群的数据库到另外一个逻辑集群，执行此操作前数据库必须已经处于链接状态, 需要管理

    员权限

    语法

    MIGRATE DATABASE src_cluster_name.src_db_name des_cluster_name.des_db_name

## example

    1. 迁移test_clusterA中的test_db到test_clusterB
    
       MIGRATE DATABASE test_clusterA.test_db test_clusterB.link_test_db;

## keyword
    MIGRATE,DATABASE

# SHOW MIGRATIONS
## description

    该语句用于查看数据库迁移的进度

    语法

    SHOW MIGRATIONS

## keyword
    SHOW,MIGRATIONS

# ENTER 
## description

    该语句用于进入一个逻辑集群, 所有创建用户、创建数据库都需要在一个逻辑集群内执行，创建后并且隶属于这个逻

    辑集群，需要管理员权限

    ENTER cluster_name

## example

    1. 进入逻辑集群test_cluster

       ENTER test_cluster;

## keyword
    ENTER
    
# SHOW BACKENDS
## description
    该语句用于查看 cluster 内的 BE 节点
    语法：
        SHOW BACKENDS;

    说明：
        1. LastStartTime 表示最近一次 BE 启动时间。
        2. LastHeartbeat 表示最近一次心跳。
        3. Alive 表示节点是否存活。
        4. SystemDecommissioned 为 true 表示节点正在安全下线中。
        5. ClusterDecommissioned 为 true 表示节点正在冲当前cluster中下线。
        6. TabletNum 表示该节点上分片数量。
        7. DataUsedCapacity 表示实际用户数据所占用的空间。
        8. AvailCapacity 表示磁盘的可使用空间。
        9. TotalCapacity 表示总磁盘空间。TotalCapacity = AvailCapacity + DataUsedCapacity + 其他非用户数据文件占用空间。
       10. UsedPct 表示磁盘已使用量百分比。
       11. ErrMsg 用于显示心跳失败时的错误信息。
        
## keyword
    SHOW, BACKENDS

# SHOW FRONTENDS
## description
    该语句用于查看 FE 节点
    语法：
        SHOW FRONTENDS;

    说明：
        1. name 表示该 FE 节点在 bdbje 中的名称。
        2. Join 为 true 表示该节点曾经加入过集群。但不代表当前还在集群内（可能已失联）
        3. Alive 表示节点是否存活。
        4. ReplayedJournalId 表示该节点当前已经回放的最大元数据日志id。
        5. LastHeartbeat 是最近一次心跳。
        6. IsHelper 表示该节点是否是 bdbje 中的 helper 节点。
        7. ErrMsg 用于显示心跳失败时的错误信息。
        
## keyword
    SHOW, FRONTENDS

# SHOW BROKER
## description
    该语句用于查看当前存在的 broker 
    语法：
        SHOW BROKER;

    说明：
        1. LastStartTime 表示最近一次 BE 启动时间。
        2. LastHeartbeat 表示最近一次心跳。
        3. Alive 表示节点是否存活。
        4. ErrMsg 用于显示心跳失败时的错误信息。
        
## keyword
    SHOW, BROKER

# ADMIN SET CONFIG
## description

    该语句用于设置集群的配置项（当前仅支持设置FE的配置项）。
    可设置的配置项，可以通过 AMDIN SHOW FRONTEND CONFIG; 命令查看。

    语法：

        ADMIN SET FRONTEND CONFIG ("key" = "value");

## example

    1. 设置 'disable_balance' 为 true

        ADMIN SET FRONTEND CONFIG ("disable_balance" = "true");

## keyword
    ADMIN,SET,CONFIG
