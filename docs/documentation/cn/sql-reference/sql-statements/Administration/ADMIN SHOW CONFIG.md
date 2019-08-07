# ADMIN SHOW CONFIG
## description

    该语句用于展示当前集群的配置（当前仅支持展示 FE 的配置项）

    语法：

        ADMIN SHOW FRONTEND CONFIG;

    说明：

        结果中的各列含义如下：
        1. Key：        配置项名称
        2. Value：      配置项值
        3. Type：       配置项类型
        4. IsMutable：  是否可以通过 ADMIN SET CONFIG 命令设置
        5. MasterOnly： 是否仅适用于 Master FE
        6. Comment：    配置项说明
        
## example

    1. 查看当前FE节点的配置

        ADMIN SHOW FRONTEND CONFIG;

## keyword
    ADMIN,SHOW,CONFIG
