# CANCEL LOAD
## description

    该语句用于撤销指定 load label 的批次的导入作业。
    这是一个异步操作，任务提交成功则返回。执行后可使用 SHOW LOAD 命令查看进度。
    语法：
        CANCEL LOAD
        [FROM db_name]
        WHERE LABEL = "load_label";
        
## example

    1. 撤销数据库 example_db 上， label 为 example_db_test_load_label 的导入作业
        CANCEL LOAD
        FROM example_db
        WHERE LABEL = "example_db_test_load_label";
        
## keyword
    CANCEL,LOAD

