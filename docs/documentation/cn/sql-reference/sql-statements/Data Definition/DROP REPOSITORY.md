# DROP REPOSITORY
## description
    该语句用于删除一个已创建的仓库。仅 root 或 superuser 用户可以删除仓库。
    语法：
        DROP REPOSITORY `repo_name`;
            
    说明：
        1. 删除仓库，仅仅是删除该仓库在 Palo 中的映射，不会删除实际的仓库数据。删除后，可以再次通过指定相同的 broker 和 LOCATION 映射到该仓库。 
        
## example
    1. 删除名为 bos_repo 的仓库：
        DROP REPOSITORY `bos_repo`;
            
## keyword
    DROP REPOSITORY

