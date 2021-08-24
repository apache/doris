## Doris Manager 编译部署文档

### 编译
直接运行manager路径下的build.sh脚本，会在manager路径下生成安装运行包——output，包中包括:
1、Doris Manager的运行包doris-manager.jar
2、运行的配置文件夹conf
3、启动脚本start_manager.sh
4、停止脚本stop_manager.sh

### 运行
#### 1 配置
进入生成的安装运行包，查看配置文件conf路径，打开路径中的配置文件manager.conf，重点关注的配置项内容如下：
```$xslt
服务的启动http端口
STUDIO_PORT=8080

后端数据存放的数据库的类型，包括mysql/h2/postgresql.默认是支持mysql
MB_DB_TYPE=mysql

数据库连接信息
如果是配置的h2类型数据库，就不需要配置这些信息，会把数据以本地文件存放在本地
如果是mysql/postgresql就需要配置如下连接信息
数据库地址
MB_DB_HOST=

数据库端口
MB_DB_PORT=3306

数据库访问端口
MB_DB_USER=
数据库访问密码
MB_DB_PASS=123456

数据库的database名称
MB_DB_DBNAME
```

#### 2 启动
配置修改完成后，回到安装运行包，直接运行如下命令
```$xslt
nohup sh ./start_manager.sh > start.log 2>&1 &
```
查看logs中的日志即可判断程序是否启动成功

#### 3 使用
预设了一个超级管理员用户，信息如下：
```$xslt
用户名: Admin
密码: Admin@123
(大小写敏感)
```
为了确保使用安全，登陆后请修改密码！