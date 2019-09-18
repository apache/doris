# SHOW TABLE STATUS
## description
    该语句用于查看 Table 的一些信息。
    语法：
        SHOW TABLE STATUS
        [FROM db] [LIKE "pattern"]
        
    说明：
        1. 该语句主要用于兼容 MySQL 语法，目前仅显示 Comment 等少量信息
        
## example
    1. 查看当前数据库下所有表的信息

        SHOW TABLE STATUS;
        
    2. 查看指定数据库下，名称包含 example 的表的信息
    
        SHOW TABLE STATUS FROM db LIKE "%example%";

## keyword
    
    SHOW,TABLE,STATUS    
