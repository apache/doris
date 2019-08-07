# SHOW PROPERTY
## description
    该语句用于查看用户的属性
    语法：
        SHOW PROPERTY [FOR user] [LIKE key]

## example
    1. 查看 jack 用户的属性
        SHOW PROPERTY FOR 'jack'

    2. 查看 jack 用户导入cluster相关属性
        SHOW PROPERTY FOR 'jack' LIKE '%load_cluster%'

## keyword
    SHOW, PROPERTY
    
