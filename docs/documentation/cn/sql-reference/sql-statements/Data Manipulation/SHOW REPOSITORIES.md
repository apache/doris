# SHOW REPOSITORIES
## description
    该语句用于查看当前已创建的仓库。
    语法：
        SHOW REPOSITORIES;
        
    说明：
        1. 各列含义如下：
            RepoId：     唯一的仓库ID
            RepoName：   仓库名称
            CreateTime： 第一次创建该仓库的时间
            IsReadOnly： 是否为只读仓库
            Location：   仓库中用于备份数据的根目录
            Broker：     依赖的 Broker
            ErrMsg：     Palo 会定期检查仓库的连通性，如果出现问题，这里会显示错误信息
    
## example
    1. 查看已创建的仓库：
        SHOW REPOSITORIES;
        
## keyword
    SHOW, REPOSITORY, REPOSITORIES
    
