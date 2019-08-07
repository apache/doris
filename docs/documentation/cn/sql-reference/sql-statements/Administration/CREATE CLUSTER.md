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

