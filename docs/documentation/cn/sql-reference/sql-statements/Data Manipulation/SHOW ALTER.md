# SHOW ALTER
## description
    该语句用于展示当前正在进行的各类修改任务的执行情况
    语法：
        SHOW ALTER [CLUSTER | TABLE [COLUMN | ROLLUP] [FROM db_name]];
        
    说明：
        TABLE COLUMN：展示修改列的 ALTER 任务
        TABLE ROLLUP：展示创建或删除 ROLLUP index 的任务
        如果不指定 db_name，使用当前默认 db
        CLUSTER: 展示集群操作相关任务情况（仅管理员使用！待实现...）
        
## example
    1. 展示默认 db 的所有修改列的任务执行情况
        SHOW ALTER TABLE COLUMN;
        
    2. 展示指定 db 的创建或删除 ROLLUP index 的任务执行情况
        SHOW ALTER TABLE ROLLUP FROM example_db;
        
    3. 展示集群操作相关任务（仅管理员使用！待实现...）
        SHOW ALTER CLUSTER;
        
## keyword
    SHOW,ALTER
    
