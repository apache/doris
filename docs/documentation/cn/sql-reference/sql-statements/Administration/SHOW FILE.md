# SHOW FILE
## description

    该语句用于展示一个 database 内创建的文件

    语法：

        SHOW FILE [FROM database];

    说明：

        FileId:     文件ID，全局唯一
        DbName:     所属数据库名称
        Catalog:    自定义分类
        FileName:   文件名
        FileSize:   文件大小，单位字节
        MD5:        文件的 MD5
        
## example

    1. 查看数据库 my_database 中已上传的文件

        SHOW FILE FROM my_database;

## keyword
    SHOW,FILE

