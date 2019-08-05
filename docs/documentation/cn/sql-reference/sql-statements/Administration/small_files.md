# CREATE FILE
## description

    该语句用于创建并上传一个文件到 Doris 集群。
    该功能通常用于管理一些其他命令中需要使用到的文件，如证书、公钥私钥等等。

    该命令只用 amdin 权限用户可以执行。
    某个文件都归属与某一个的 database。对 database 拥有访问权限的用户都可以使用该文件。

    单个文件大小限制为 1MB。
    一个 Doris 集群最多上传 100 个文件。

    语法：
        
        CREATE FILE "file_name" [IN database]
        [properties]

    说明：
        file_name:  自定义文件名。
        database: 文件归属于某一个 db，如果没有指定，则使用当前 session 的 db。
        properties 支持以下参数:

            url: 必须。指定一个文件的下载路径。当前仅支持无认证的 http 下载路径。命令之行成功后，文件将被保存在 doris 中，该 url 将不再需要。
            catalog: 必须。对文件的分类名，可以自定义。但在某些命令中，会查找指定 catalog 中的文件。比如例行导入中的，数据源为 kafka 时，会查找 catalog 名为 kafka 下的文件。
            md5: 可选。文件的 md5。如果指定，会在下载文件后进行校验。

## example

    1. 创建文件 ca.pem ，分类为 kafka

        CREATE FILE "ca.pem"
        PROPERTIES
        (
            "url" = "https://test.bj.bcebos.com/kafka-key/ca.pem",
            "catalog" = "kafka"
        );

    2. 创建文件 client.key，分类为 my_catalog

        CREATE FILE "client.key"
        IN my_database
        PROPERTIES
        (
            "url" = "https://test.bj.bcebos.com/kafka-key/client.key",
            "catalog" = "my_catalog",
            "md5" = "b5bb901bf10f99205b39a46ac3557dd9"
        );
        
## keyword
    CREATE,FILE

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

# DROP FILE
## description

    该语句用于删除一个已上传的文件。

    语法：

        DROP FILE "file_name" [FROM database]
        [properties]

    说明：
        file_name:  文件名。
        database: 文件归属的某一个 db，如果没有指定，则使用当前 session 的 db。
        properties 支持以下参数:

            catalog: 必须。文件所属分类。

## example

    1. 删除文件 ca.pem

        DROP FILE "ca.pem" properties("catalog" = "kafka");

## keyword
    DROP,FILE
