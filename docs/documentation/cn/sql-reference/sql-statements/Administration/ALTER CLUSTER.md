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

