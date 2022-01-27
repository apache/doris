---
{
    "title": "FAQ",
    "language": "zh-CN"
}
---

<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# FAQ

本文档主要用于记录 Doris 使用过程中的常见问题。会不定期更新。

### Q1. 使用 Stream Load 访问 FE 的公网地址导入数据，被重定向到内网 IP？

当 stream load 的连接目标为FE的http端口时，FE仅会随机选择一台BE节点做http 307 redirect 操作，因此用户的请求实际是发送给FE指派的某一个BE的。而redirect返回的是BE的ip，也即内网IP。所以如果你是通过FE的公网IP发送的请求，很有可能因为redirect到内网地址而无法连接。

通常的做法，一种是确保自己能够访问内网IP地址，或者是给所有BE上层假设一个负载均衡，然后直接将 stream load 请求发送到负载均衡器上，由负载均衡将请求透传到BE节点。

### Q2. 通过 DECOMMISSION 下线BE节点时，为什么总会有部分tablet残留？

在下线过程中，通过 show backends 查看下线节点的 tabletNum ，会观察到 tabletNum 数量在减少，说明数据分片正在从这个节点迁移走。当数量减到0时，系统会自动删除这个节点。但某些情况下，tabletNum 下降到一定数值后就不变化。这通常可能有以下两种原因：

1. 这些 tablet 属于刚被删除的表、分区或物化视图。而刚被删除的对象会保留在回收站中。而下线逻辑不会处理这些分片。可以通过修改 FE 的配置参数 catalog_trash_expire_second 来修改对象在回收站中驻留的时间。当对象从回收站中被删除后，这些 tablet就会被处理了。

2. 这些 tablet 的迁移任务出现了问题。此时需要通过 show proc "/cluster_balance" 来查看具体任务的错误了。

对于以上情况，可以先通过 show proc "/statistic" 查看集群是否还有 unhealthy 的分片，如果为0，则可以直接通过 drop backend 语句删除这个 BE 。否则，还需要具体查看不健康分片的副本情况。


### Q3. priorty_network 应该如何设置？

priorty_network 是 FE、BE 都有的配置参数。这个参数主要用于帮助系统选择正确的网卡 IP 作为自己的 IP 。建议任何情况下，都显式的设置这个参数，以防止后续机器增加新网卡导致IP选择不正确的问题。

priorty_network 的值是 CIDR 格式表示的。分为两部分，第一部分是点分十进制的 IP 地址，第二部分是一个前缀长度。比如 10.168.1.0/8 会匹配所有 10.xx.xx.xx 的IP地址，而 10.168.1.0/16 会匹配所有 10.168.xx.xx 的 IP 地址。

之所以使用 CIDR 格式而不是直接指定一个具体 IP，是为了保证所有节点都可以使用统一的配置值。比如有两个节点：10.168.10.1 和 10.168.10.2，则我们可以使用 10.168.10.0/24 来作为 priorty_network 的值。

### Q4. FE的Master、Follower、Observer都是什么？

首先明确一点，FE 只有两种角色：Follower 和 Observer。而 Master 只是一组 Follower 节点中选择出来的一个 FE。Master 可以看成是一种特殊的 Follower。所以当我们被问及一个集群有多少 FE，都是什么角色时，正确的回答当时应该是所有 FE 节点的个数，以及 Follower 角色的个数和 Observer 角色的个数。

所有 Follower 角色的 FE 节点会组成一个可选择组，类似 Poxas 一致性协议里的组概念。组内会选举出一个 Follower 作为 Master。当 Master 挂了，会自动选择新的 Follower 作为 Master。而 Observer 不会参与选举，因此 Observer 也不会称为 Master 。

一条元数据日志需要在多数 Follower 节点写入成功，才算成功。比如3个 FE ，2个写入成功才可以。这也是为什么 Follower 角色的个数需要是奇数的原因。

Observer 角色和这个单词的含义一样，仅仅作为观察者来同步已经成功写入的元数据日志，并且提供元数据读服务。他不会参与多数写的逻辑。

通常情况下，可以部署 1 Follower + 2 Observer 或者 3 Follower + N Observer。前者运维简单，几乎不会出现 Follower 之间的一致性协议导致这种复杂错误情况（百度内部集群大多使用这种方式）。后者可以保证元数据写的高可用，如果是高并发查询场景，可以适当增加 Observer。

### Q5. Doris 是否支持修改列名？

不支持修改列名。

Doris支持修改数据库名、表名、分区名、物化视图（Rollup）名称，以及列的类型、注释、默认值等等。但遗憾的是，目前不支持修改列名。

因为一些历史原因，目前列名称是直接写入到数据文件中的。Doris在查询时，也是通过类名查找到对应的列的。所以修改列名不仅是简单的元数据修改，还会涉及到数据的重写，是一个非常重的操作。

我们不排除后续通过一些兼容手段来支持轻量化的列名修改操作。

### Q6. Unique Key模型的表是否支持创建物化视图？

不支持。

Unique Key模型的表是一个对业务比较友好的表，因为其特有的按照主键去重的功能，能够很方便的同步数据频繁变更的业务数据库。因此，很多用户在将数据接入到Doris时，会首先考虑使用Unique Key模型。

但遗憾的是，Unique Key模型的表是无法建立物化视图的。原因在于，物化视图的本质，是通过预计算来将数据“预先算好”，这样在查询时直接返回已经计算好的数据，来加速查询。在物化视图中，“预计算”的数据通常是一些聚合指标，比如求和、求count。这时，如果数据发生变更，如udpate或delete，因为预计算的数据已经丢失了明细信息，因此无法同步的进行更新。比如一个求和值5，可能是 1+4，也可能是2+3。因为明细信息的丢失，我们无法区分这个求和值是如何计算出来的，因此也就无法满足更新的需求。

### Q7. show backends/frontends 查看到的信息不完整

在执行如` show backends/frontends` 等某些语句后，结果中可能会发现有部分列内容不全。比如show backends结果中看不到磁盘容量信息等。

通常这个问题会出现在集群有多个FE的情况下，如果用户连接到非Master FE节点执行这些语句，就会看到不完整的信息。这是因为，部分信息仅存在于Master FE节点。比如BE的磁盘使用量信息等。所以只有在直连Master FE后，才能获得完整信息。

当然，用户也可以在执行这些语句前，先执行 `set forward_to_master=true;` 这个会话变量设置为true后，后续执行的一些信息查看类语句会自动转发到Master FE获取结果。这样，不论用户连接的是哪个FE，都可以获取到完整结果了。

### Q8. 节点新增加了新的磁盘，为什么数据没有均衡到新的磁盘上？

当前Doris的均衡策略是以节点为单位的。也就是说，是按照节点整体的负载指标（分片数量和总磁盘利用率）来判断集群负载。并且将数据分片从高负载节点迁移到低负载节点。如果每个节点都增加了一块磁盘，则从节点整体角度看，负载并没有改变，所以无法触发均衡逻辑。

此外，Doris目前并不支持单个节点内部，各个磁盘间的均衡操作。所以新增磁盘后，不会将数据均衡到新的磁盘。

但是，数据在节点之间迁移时，Doris会考虑磁盘的因素。比如一个分片从A节点迁移到B节点，会优先选择B节点中，磁盘空间利用率较低的磁盘。

这里我们提供3种方式解决这个问题：

1. 重建新表

    通过create table like 语句建立新表，然后使用 insert into select的方式将数据从老表同步到新表。因为创建新表时，新表的数据分片会分布在新的磁盘中，从而数据也会写入新的磁盘。这种方式适用于数据量较小的情况（几十GB以内）。

2. 通过Decommission命令

    decommission命令用于安全下线一个BE节点。该命令会先将该节点上的数据分片迁移到其他节点，然后在删除该节点。前面说过，在数据迁移时，会优先考虑磁盘利用率低的磁盘，因此该方式可以“强制”让数据迁移到其他节点的磁盘上。当数据迁移完成后，我们在cancel掉这个decommission操作，这样，数据又会重新均衡回这个节点。当我们对所有BE节点都执行一遍上述步骤后，数据将会均匀的分布在所有节点的所有磁盘上。

    注意，在执行decommission命令前，先执行以下命令，以避免节点下线完成后被删除。

    `admin set frontend config("drop_backend_after_decommission" = "false");`

3. 使用API手动迁移数据

    Doris提供了[HTTP API](../administrator-guide/http-actions/tablet-migration-action.md)，可以手动指定一个磁盘上的数据分片迁移到另一个磁盘上。

### Q9. 如何正确阅读 FE/BE 日志?

很多情况下我们需要通过日志来排查问题。这里说明一下FE/BE日志的格式和查看方式。

1. FE

    FE日志主要有：
    
    * fe.log：主日志。包括除fe.out外的所有内容。
    * fe.warn.log：主日志的子集，仅记录 WARN 和 ERROR 级别的日志。
    * fe.out：标准/错误输出的日志（stdout和stderr）。
    * fe.audit.log：审计日志，记录这个FE接收的所有SQL请求。

    一条典型的FE日志如下：

    ```
    2021-09-16 23:13:22,502 INFO (tablet scheduler|43) [BeLoadRebalancer.selectAlternativeTabletsForCluster():85] cluster is balance: default_cluster with medium: HDD. skip
    ```
    
    * `2021-09-16 23:13:22,502`：日志时间。
    * `INFO：日志级别，默认是INFO`。
    * `(tablet scheduler|43)`：线程名称和线程id。通过线程id，就可以查看这个线程上下文信息，方面排查这个线程发生的事情。
    * `BeLoadRebalancer.selectAlternativeTabletsForCluster():85`：类名、方法名和代码行号。
    * `cluster is balance xxx`：日志内容。

    通常情况下我们主要查看fe.log日志。特殊情况下，有些日志可能输出到了fe.out中。

2. BE

    BE日志主要有：

    * be.INFO：主日志。这其实是个软连，连接到最新的一个 be.INFO.xxxx上。
    * be.WARNING：主日志的子集，仅记录 WARN 和 FATAL 级别的日志。这其实是个软连，连接到最新的一个 be.WARN.xxxx上。
    * be.out：标准/错误输出的日志（stdout和stderr）。

    一条典型的BE日志如下：

    ```
    I0916 23:21:22.038795 28087 task_worker_pool.cpp:1594] finish report TASK. master host: 10.10.10.10, port: 9222
    ```

    * `I0916 23:21:22.038795`：日志等级和日期时间。大写字母I表示INFO，W表示WARN，F表示FATAL。
    * `28087`：线程id。通过线程id，就可以查看这个线程上下文信息，方面排查这个线程发生的事情。
    * `task_worker_pool.cpp:1594`：代码文件和行号。
    * `finish report TASK xxx`：日志内容。

    通常情况下我们主要查看be.INFO日志。特殊情况下，如BE宕机，则需要查看be.out。

### Q10. FE/BE 节点挂了应该如何排查原因?

1. BE

    BE进程是 C/C++ 进程，可能会因为一些程序Bug（内存越界，非法地址访问等）或 Out Of Memory（OOM）导致进程挂掉。此时我们可以通过以下几个步骤查看错误原因：
    
    1. 查看be.out
    
        BE进程实现了在程序因异常情况退出时，会打印当前的错误堆栈到be.out里（注意是be.out，不是be.INFO或be.WARNING）。通过错误堆栈，通常能够大致获悉程序出错的位置。
    
        注意，如果be.out中出现错误堆栈，通常情况下是因为程序bug，普通用户可能无法自行解决，欢迎前往微信群、github discussion 或dev邮件组寻求帮助，并贴出对应的错误堆栈，以便快速排查问题。
    
    2. dmesg
    
        如果be.out没有堆栈信息，则大概率是因为OOM被系统强制kill掉了。此时可以通过dmesg -T 这个命令查看linux系统日志，如果最后出现  Memory cgroup out of memory: Kill process 7187 (palo_be) score 1007 or sacrifice child 类似的日志，则说明是OOM导致的。
    
        内存问题可能有多方面原因，如大查询、导入、compaction等。Doris也在不断优化内存使用。欢迎前往微信群、github discussion 或dev邮件组寻求帮助。
    
    3. 查看be.INFO中是否有F开头的日志。
    
        F开头的的日志是 Fatal 日志。如 F0916 ，表示9月16号的Fatal日志。Fatal日志通常表示程序断言错误，断言错误会直接导致进程退出（说明程序出现了Bug）。欢迎前往微信群、github discussion 或dev邮件组寻求帮助。
        
    4. Minidump
    
        Mindump 是 Doris 0.15 版本之后加入的功能，具体可参阅[文档](../developer-guide/minidump.md)。

2. FE

    FE 是 java 进程，健壮程度要由于 C/C++ 程序。通常FE 挂掉的原因可能是 OOM（Out-of-Memory）或者是元数据写入失败。这些错误通常在 fe.log 或者 fe.out 中有错误堆栈。需要根据错误堆栈信息进一步排查。

### Q11. 关于数据目录SSD和HDD的配置。

Doris支持一个BE节点配置多个存储路径。通常情况下，每块盘配置一个存储路径即可。同时，Doris支持指定路径的存储介质属性，如SSD或HDD。SSD代表高速存储设备，HDD代表低速存储设备。

通过指定路径的存储介质属性，我们可以利用Doris的冷热数据分区存储功能，在分区级别将热数据存储在SSD中，而冷数据会自动转移到HDD中。

需要注意的是，Doris并不会自动感知存储路径所在磁盘的实际存储介质类型。这个类型需要用户在路径配置中显式的表示。比如路径 "/path/to/data1.SSD" 即表示这个路径是SSD存储介质。而 "data1.SSD" 就是实际的目录名称。Doris是根据目录名称后面的 ".SSD" 后缀来确定存储介质类型的，而不是实际的存储介质类型。也就是说，用户可以指定任意路径为SSD存储介质，而Doris仅识别目录后缀，不会去判断存储介质是否匹配。如果不写后缀，则默认为HDD。

换句话说，".HDD" 和 ".SSD" 只是用于标识存储目录“相对”的“低速”和“高速”之分，而并不是标识实际的存储介质类型。所以如果BE节点上的存储路径没有介质区别，则无需填写后缀。

### Q12. Unique Key 模型查询结果不一致

某些情况下，当用户使用相同的 SQL 查询一个 Unique Key 模型的表时，可能会出现多次查询结果不一致的现象。并且查询结果总在 2-3 种之间变化。

这可能是因为，在同一批导入数据中，出现了 key 相同但 value 不同的数据，这会导致，不同副本间，因数据覆盖的先后顺序不确定而产生的结果不一致的问题。

比如表定义为 k1, v1。一批次导入数据如下：

```
1, "abc"
1, "def"
```

那么可能副本1 的结果是 `1, "abc"`，而副本2 的结果是 `1, "def"`。从而导致查询结果不一致。

为了确保不同副本之间的数据先后顺序唯一，可以参考 [Sequence Column](../administrator-guide/load-data/sequence-column-manual.md) 功能。

### Q13. 多个FE，在使用Nginx实现web UI负载均衡时，无法登录

Doris 可以部署多个FE，在访问Web UI的时候，如果使用Nginx进行负载均衡，因为Session问题会出现不停的提示要重新登录，这个问题其实是Session共享的问题，Nginx提供了集中Session共享的解决方案，这里我们使用的是nginx中的ip_hash技术，ip_hash能够将某个ip的请求定向到同一台后端，这样一来这个ip下的某个客户端和某个后端就能建立起稳固的session，ip_hash是在upstream配置中定义的：

```
upstream  doris.com {
   server    172.22.197.238:8030 weight=3;
   server    172.22.197.239:8030 weight=4;
   server    172.22.197.240:8030 weight=4;
   ip_hash;
}
```
完整的Nginx示例配置如下:

```
user nginx;
worker_processes auto;
error_log /var/log/nginx/error.log;
pid /run/nginx.pid;

# Load dynamic modules. See /usr/share/doc/nginx/README.dynamic.
include /usr/share/nginx/modules/*.conf;

events {
    worker_connections 1024;
}

http {
    log_format  main  '$remote_addr - $remote_user [$time_local] "$request" '
                      '$status $body_bytes_sent "$http_referer" '
                      '"$http_user_agent" "$http_x_forwarded_for"';

    access_log  /var/log/nginx/access.log  main;

    sendfile            on;
    tcp_nopush          on;
    tcp_nodelay         on;
    keepalive_timeout   65;
    types_hash_max_size 2048;

    include             /etc/nginx/mime.types;
    default_type        application/octet-stream;

    # Load modular configuration files from the /etc/nginx/conf.d directory.
    # See http://nginx.org/en/docs/ngx_core_module.html#include
    # for more information.
    include /etc/nginx/conf.d/*.conf;
    #include /etc/nginx/custom/*.conf;
    upstream  doris.com {
      server    172.22.197.238:8030 weight=3;
      server    172.22.197.239:8030 weight=4;
      server    172.22.197.240:8030 weight=4;
      ip_hash;
    }

    server {
        listen       80;
        server_name  gaia-pro-bigdata-fe02;
        if ($request_uri ~ _load) {
           return 307 http://$host$request_uri ;
        }

        location / {
            proxy_pass http://doris.com;
            proxy_redirect default;
        }
        error_page   500 502 503 504  /50x.html;
        location = /50x.html {
            root   html;
        }
    }
 }
```
