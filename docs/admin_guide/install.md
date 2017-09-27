 # Palo 安装文档

本文档会介绍 Palo 的系统依赖，编译，部署和常见问题解决。

## 1. 系统依赖

Palo 当前只能运行在 Linux 系统上，无论是编译还是部署，都建议确保系统安装了如下软件或者库：

* GCC 4.8.2+，Oracle JDK 1.8+，Python 2.7+，确认 gcc, java, python 命令指向正确版本, 设置 JAVA_HOME 环境变量

* Ubuntu需要安装：`sudo apt-get install g++ ant cmake zip byacc flex automake libtool binutils-dev libiberty-dev bison`；安转完成后，需要执行 `sudo updatedb`。

* CentOS需要安装：`sudo yum install ant cmake byacc flex automake libtool binutils-devel bison`；安装完成后，需要执行 `sudo updatedb`。

## 2. 编译

默认提供了 Ubuntu 16.04, Centos 7.2 环境的预编译版本，可以直接下载使用。
下载链接（20170926 update）：
[palo-0.8.0_centos7.2.tar.gz](http://palo-opensource.gz.bcebos.com/palo-0.8.0_20170926_centos7.2_gcc485.tar.gz?authorization=bce-auth-v1%2F069fc2786e464e63a5f1183824ddb522%2F2017-09-27T03%3A27%3A37Z%2F-1%2Fhost%2F3544aac7e4b05998238df641d9cc982fed004a0c10a7f930d81e13c75adcf98a), [palo-0.8.0_ubuntu16.04.tar.gz](http://palo-opensource.gz.bcebos.com/palo-0.8.0_20170926_ubuntu16.04_gcc540.tar.gz?authorization=bce-auth-v1%2F069fc2786e464e63a5f1183824ddb522%2F2017-09-27T03%3A28%3A00Z%2F-1%2Fhost%2F28b1eddbee1ad2e1f92d46a48251c418d0fcd3b4c7ce76472a0cd8c5c78f83cc)

同时我们提供了 docker 镜像下载（20170822 update）：[palo-0.8.0-centos-docker.tar](http://palo-opensource.gz.bcebos.com/palo-0.8.0-centos-docker-20170822.tar?authorization=bce-auth-v1%2F069fc2786e464e63a5f1183824ddb522%2F2017-08-22T11%3A38%3A15Z%2F-1%2Fhost%2F1e56d3d3dbc51f0d36af792197130b85743792aae282a93aaa43515e5eba5dc6)

docker 镜像的使用方式参见本文最后一节。

如需自行编译，请按照下面步骤进行源码编译。

### 2.1 编译第三方依赖库

为防止从官网下载第三方库失败，我们提前打包了palo所需的第三方库，下载地址如下: [palo-thirdparty.tar.gz](http://palo-opensource.gz.bcebos.com/palo-thirdparty.tar.gz?authorization=bce-auth-v1%2F069fc2786e464e63a5f1183824ddb522%2F2017-08-11T13%3A18%3A14Z%2F-1%2Fhost%2Fec3d7693a3ab4fe76fb23f8e77dff40624bde867cab75d3842e719816cbd1d2b)。下载解压完成之后，需将palo-thirdparty目录下的全部文件拷贝到thirdparty/src目录下。

运行`sh thirdparty/build-thirdparty.sh`编译第三方库。

**注意**：`build-thirdparty.sh` 依赖 thirdparty 目录下的其它两个脚本，其中 `vars.sh` 定义了一些编译第三方库时依赖的环境变量；`download-thirdparty.sh` 负责完成从官网下载所需第三方依赖库。

### 2.2 编译 Palo FE 和 BE

`sh build.sh`

最终的部署文件将产出到 output/ 目录下。

### 2.3 (可选) 编译 FS_Broker

FS_Broker 用于从其他数据源（如Hadoop HDFS、百度云 BOS）导入数据时使用，如果不需要从这两个数据源导入数据可以先不编译和部署。需要的时候，可以后面再编译和部署。

需要哪个 broker，就进入 fs_brokers/ 下对应的 broker 的目录，执行相应的build脚本即可，执行完毕后，产生的部署文件生成在对应 broker 的 output 目录下。

## 3. 部署

Palo 主要包括 Frontend（FE）和 Backend（BE）两个进程。其中 FE 主要负责元数据管理、集群管理、接收用户请求和查询计划生成；BE 主要负责数据存储和查询计划执行。

一般 100 台规模的集群，可根据性能需求部署 1 到 5 台 FE，而剩下的全部机器部署 BE。其中建议 FE 的机器采用带有 RAID 卡的 SAS 硬盘，或者 SSD 硬盘，不建议使用普通 SATA 硬盘；而 BE 对硬盘没有太多要求。

### 3.1 单 FE 部署

* 拷贝 FE 部署文件到指定节点

    将源码编译生成的 output 下的 fe 文件夹拷贝到要部署 FE 的节点的一个账户根目录下的fe目录下。

* 配置 FE

    配置文件为 conf/fe.conf。其中注意：`meta_dir`：元数据存放位置。默认在 fe/palo-meta/ 下。需**手动创建**该目录。

* 启动FE

    `sh bin/start_fe.sh`

    FE进程启动进入后台执行。日志默认存放在 fe/log/ 目录下。如启动失败，可以通过查看 fe/log/fe.log 或者 fe/log/fe.out 查看错误信息。

* 如需部署多 FE，请参见 "FE 高可用" 章节

### 3.2 多 BE 部署

* 拷贝 BE 部署文件到所有要部署BE的节点

    将源码编译生成的 output 下的 be 文件夹拷贝到要部署 BE 的节点的一个账户根目录下的be目录下。

* 修改所有 BE 的配置

    修改 be/conf/be.conf。主要是配置 `storage_root_path`：数据存放目录，使用 `;` 分隔（最后一个目录后不要加 `;`），其它可以采用默认值。

* 在 FE 中添加所有 BE 节点

    BE 节点需要先在 FE 中添加，才可加入集群。可以使用 mysql-client 连接到FE：

    `./mysql-client -h host -P port -uroot`

    其中 host 为 FE 所在节点 ip；port 为 fe/conf/fe.conf 中的 query_port；默认使用root账户，无密码登录。

    登录后，执行以下命令来添加每一个BE：

    `ALTER SYSTEM ADD BACKEND "host:port";`

    其中 host 为 BE所在节点 ip；port 为 be/conf/be.conf 中的 heartbeat_service_port。

* 启动 BE

    `sh bin/start_be.sh`

    BE 进程将启动并进入后台执行。日志默认存放在 be/log/ 目录下。如启动失败，可以通过查看 be/log/be.log 或者 be/log/be.out 查看错误信息。

* 查看BE状态

    使用 mysql-client 连接到 FE，并执行 `SHOW PROC '/backends';` 查看 BE 运行情况。如一切正常，`isAlive` 列应为 `true`。

### 3.3 （可选）FS_Broker 部署

broker 以插件的形式，独立于 Palo 部署。如果需要从第三方存储系统导入数据，需要部署相应的 broker，默认提供了读取 HDFS 和百度云 BOS 的 fs_broker。fs_broker 是无状态的，建议每一个 FE 和 BE 节点都部署一个 broker。

* 拷贝源码 fs_broker 的 output 目录下的相应 broker 目录到需要部署的所有节点上。建议和 BE 或者 FE 目录保持同级。

* 修改相应broker配置

    在相应 broker/conf/ 目录下对应的配置文件中，可以修改相应配置。

 * 启动broker

    sh bin/start_broker.sh 启动broker。

* 添加broker

    要让 palo 的 fe 和 be 知道 broker 在哪些节点上，通过 sql 命令添加 broker 节点列表。

    使用 mysql-client 连接启动的 FE，执行以下命令：

    `ALTER SYSTEM ADD BROKER broker_name "host1:port1","host2:port2",...;`

    其中 host 为 broker 所在节点 ip；port 为 broker 配置文件中的 broker_ipc_port。

* 查看broker 状态

    使用 mysql-client 连接任一已启动的 FE，执行以下命令查看 broker 状态：`SHOW PROC "/brokers";`

## 4. 常见问题解决

* 执行 `sh build-thirdparty.sh` 报错: source: not found

    请使用 /bin/bash 代替 sh 执行脚本。

* 编译 gperftools：找不到 libunwind

    创建到 libunwind.so.x 的软链：`cd thirdparty/installed/lib && ln -s libunwind.so.8 libunwind.so`，之后重新执行：`build-thirdparty.sh`。

* 编译 thrift：找不到 libssl 或 libcrypto

    创建到系统 libssl.so.x 的软链：

    `cd thirdparty/installed/lib`

    `rm libssl.so libcrypto.so`

    `ln -s /usr/lib64/libssl.so.10 libssl.so`

    `ln -s /lib64/libcrypto.so.10 libcrypto.so`

    (系统库路径可能不相同，请对应修改)

    在 thirdparty/build-thirdparty.sh 中，注释掉 build_openssl 之后重新执行 `build-thirdparty.sh`。

* 编译 thrift：No rule to make target \`gen-cpp/Service.cpp', needed by \`Service.lo'.  Stop.

    重新执行：`sh build-thirdparty.sh`

* 编译 Boost：Boost.Context fails to build -> Call of overloaded 'callcc(...) is ambiguous'

    如果你使用 gcc 4.8 或 4.9 版本，则可能出现这个问题。执行以下命令：

    `cd thirdparty/src/boost_1_64_0`

    `patch -p0 < ../../patches/boost-1.64.0-gcc4.8.patch`

    之后重新执行：`sh build-thirdparty.sh`；

    > 参考：https://github.com/boostorg/fiber/issues/121

* 编译 mysql：Inconsistency detected by ld.so: dl-version.c: 224: _dl_check_map_versions: Assertion `needed != ((void *)0)' failed!

    如果你使用 Ubuntu 14.04 LTS apt-get 安装的 cmake 2.8.2 版本，则可能出现这个问题。

    用 apt-get 删除 cmake，并从cmake官网下载安装最新的 cmake。

    之后重新执行：`sh build-thirdparty.sh`；

    > 参考：https://forum.directadmin.com/archive/index.php/t-51343.html

* 编译 thrift：syntax error near unexpected token `GLIB,'

    检查是否安装了 pkg-config，并且版本高于 0.22。如果已经安装，但依然出现此问题，请删除 `thirdparty/src/thrift-0.9.3` 后，重新执行：sh build-thirdparty.sh`

* 编译 curl：libcurl.so: undefined reference to `SSL_get0_alpn_selected'

    在 thirdparty/build-thirdparty.sh 中确认没有注释掉 build_openssl。同时注释掉 build_thrift。之后重新执行：`sh build-thirdparty.sh`

* 编译 Palo FE 和 BE：[bits/c++config.h] 或 [cstdef] 找不到

    在 be/CMakeLists.txt 中修改对应的 CLANG_BASE_FLAGS 中设置的 include 路径。可以通过命令：`locate c++config.h` 确认头文件的地址。

* 编译 Palo FE 和 BE：cstddef: no member named 'max_align_t' in the global namespace

    在 Ubuntu 14.04 LTS 和 16.04 环境下可能会遇到此问题。首先通过 `locate cstddef` 定位到系统的 cstddef 文件位置。打开 cstddef 文件，修改如下片段：
    ```
    namespace std {
      // We handle size_t, ptrdiff_t, and nullptr_t in c++config.h.
      +#ifndef __clang__
      using ::max_align_t;
      +#endif
    }
    ```
    > 参考：http://clang-developers.42468.n3.nabble.com/another-try-lastest-ubuntu-14-10-gcc4-9-1-can-t-build-clang-td4043875.html

* 编译 Palo FE 和 BE：/bin/bash^M: bad interpreter: No such file or directory

    对应的脚本程序可能采用了 window 的换行格式。使用 vim 打开对应的脚本文件，执行： `:set ff=unix` 保存并退出。

## 5. FE 高可用

FE 分为 leader，follower 和 observer 三种角色。 默认一个集群，只能有一个 leader，可以有多个 follower 和 observer。其中 leader 和 follower 组成一个 Paxos 选择组，如果 leader 宕机，则剩下的 follower 会自动选出新的 leader，保证写入高可用。observer 同步 leader 的数据，但是不参加选举。如果只部署一个 FE，则 FE默认就是 leader。

第一个启动的 FE 自动成为 leader。在此基础上，可以添加若干 follower 和 observer。

添加 follower 或 observer。使用 mysql-client 连接到已启动的 FE，并执行：

`ALTER SYSTEM ADD FOLLOWER "host:port";`

或

`ALTER SYSTEM ADD OBSERVER "host:port";`

其中 host 为 follower 或 observer 所在节点 ip；port 为其配置文件fe.conf中的 edit_log_port。

配置及启动 follower 或 observer。follower 和 observer 的配置同 leader 的配置。第一次启动时，需执行以下命令：

`sh bin/start_fe.sh -helper host:port`

其中host为 Leader 所在节点 ip；port 为 Leader 的配置文件 fe.conf 中的 edit_log_port。-helper 参数仅在 follower 和 observer 第一次启动时才需要。

查看 Follower 或 Observer 运行状态。使用 mysql-client 连接到任一已启动的 FE，并执行：SHOW PROC '/frontend'; 可以查看当前已加入集群的 FE 及其对应角色。

## 6. Docker 镜像

请确保已安装 docker 并启动 docker 服务。

* 加载 Docker 镜像

    `docker load -i palo-0.8.0-centos-docker.tar`

* 创建工作目录

    `cd /your/workspace/ && mkdir -p fe/palo-meta fe/log be/data/ be/log`

    以上目录分别用于存放 FE 元信息、FE 日志、BE 数据、BE 日志。

* 启动 container

    `docker run --privileged -p 9030:9030 -p 8030:8030 -p 9010:9010 -p 9020:9020 -p 9060:9060 -p 9070:9070 -p 8040:8040 -p 9050:9050 -v $PWD/fe/log:/home/palo/run/fe/log -v $PWD/fe/palo-meta:/home/palo/run/fe/palo-meta -v $PWD/be/log:/home/palo/run/be/log -v $PWD/be/data:/home/palo/run/be/data -d -i -t palo:0.8.0 /bin/bash`

    该命令将 FE 和 BE 所需的所有端口映射到宿主机对应端口，并将 FE 和 BE 所需的持久化目录（元信息、数据、日志）挂载到之前创建的工作目录下。

* Attach container

    执行 `docker ps -l` 获取 `CONTAINER_ID`。

    执行 `docker attach CONTAINER_ID` 进入 container。之后按照前文所述，启动 FE 和 BE 即可。

* 退出 container

    若想保持 container 运行，执行 `ctrl + pq` 退出。

    若需退出并关闭 container，执行 `ctrl + d`。

