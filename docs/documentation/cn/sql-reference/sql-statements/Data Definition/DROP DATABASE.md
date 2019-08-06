# DROP DATABASE
## description
    该语句用于删除数据库（database）
    语法：
        DROP DATABASE [IF EXISTS] db_name;

    说明：
        执行 DROP DATABASE 一段时间内，可以通过 RECOVER 语句恢复被删除的 database。详见 RECOVER 语句
        
## example
    1. 删除数据库 db_test
        DROP DATABASE db_test;
        
## keyword
    DROP,DATABASE
        
