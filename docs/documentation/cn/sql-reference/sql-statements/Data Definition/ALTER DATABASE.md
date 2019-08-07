# ALTER DATABASE
## description
    该语句用于设置指定数据库的属性。（仅管理员使用）
    语法：
        1) 设置数据库数据量配额，单位为B/K/KB/M/MB/G/GB/T/TB/P/PB
            ALTER DATABASE db_name SET DATA QUOTA quota;
            
        2) 重命名数据库
            ALTER DATABASE db_name RENAME new_db_name;
            
    说明：
        重命名数据库后，如需要，请使用 REVOKE 和 GRANT 命令修改相应的用户权限。 

## example
    1. 设置指定数据库数据量配额
        ALTER DATABASE example_db SET DATA QUOTA 10995116277760;
        上述单位为字节,等价于
        ALTER DATABASE example_db SET DATA QUOTA 10T;

        ALTER DATABASE example_db SET DATA QUOTA 100G;

        ALTER DATABASE example_db SET DATA QUOTA 200M;

    2. 将数据库 example_db 重命名为 example_db2
        ALTER DATABASE example_db RENAME example_db2;

## keyword
    ALTER,DATABASE,RENAME
    
