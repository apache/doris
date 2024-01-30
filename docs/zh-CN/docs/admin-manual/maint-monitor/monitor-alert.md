---
{
    "title": "监控和报警",
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

# 监控和报警

本文档主要介绍 Doris 的监控项及如何采集、展示监控项。以及如何配置报警（TODO）

Dashboard 模板点击下载

| Doris 版本    | Dashboard 版本                                                               |
|--------------|----------------------------------------------------------------------------|
| 1.2.x        | [revision 5](https://grafana.com/api/dashboards/9734/revisions/5/download) |

Dashboard 模板会不定期更新。更新模板的方式见最后一小节。

欢迎提供更优的 dashboard。

## 组件

Doris 使用 [Prometheus](https://prometheus.io/) 和 [Grafana](https://grafana.com/) 进行监控项的采集和展示。

![](/images/dashboard_overview.png)

1. Prometheus
    
    Prometheus 是一款开源的系统监控和报警套件。它可以通过 Pull 或 Push 采集被监控系统的监控项，存入自身的时序数据库中。并且通过丰富的多维数据查询语言，满足用户的不同数据展示需求。

2. Grafana
    
    Grafana 是一款开源的数据分析和展示平台。支持包括 Prometheus 在内的多个主流时序数据库源。通过对应的数据库查询语句，从数据源中获取展现数据。通过灵活可配置的 Dashboard，快速的将这些数据以图表的形式展示给用户。

> 注: 本文档仅提供一种使用 Prometheus 和 Grafana 进行 Doris 监控数据采集和展示的方式。原则上不开发、维护这些组件。更多关于这些组件的详细介绍，请移步对应官方文档进行查阅。

## 监控数据

Doris 的监控数据通过 Frontend 和 Backend 的 http 接口向外暴露。监控数据以 Key-Value 的文本形式对外展现。每个 Key 还可能有不同的 Label 加以区分。当用户搭建好 Doris 后，可以在浏览器，通过以下接口访问到节点的监控数据：

* Frontend: `fe_host:fe_http_port/metrics`
* Backend: `be_host:be_web_server_port/metrics`
* Broker: 暂不提供

用户将看到如下监控项结果（示例为 FE 部分监控项）：

```
# HELP  jvm_heap_size_bytes jvm heap stat
# TYPE  jvm_heap_size_bytes gauge
jvm_heap_size_bytes{type="max"} 8476557312
jvm_heap_size_bytes{type="committed"} 1007550464
jvm_heap_size_bytes{type="used"} 156375280
# HELP  jvm_non_heap_size_bytes jvm non heap stat
# TYPE  jvm_non_heap_size_bytes gauge
jvm_non_heap_size_bytes{type="committed"} 194379776
jvm_non_heap_size_bytes{type="used"} 188201864
# HELP  jvm_young_size_bytes jvm young mem pool stat
# TYPE  jvm_young_size_bytes gauge
jvm_young_size_bytes{type="used"} 40652376
jvm_young_size_bytes{type="peak_used"} 277938176
jvm_young_size_bytes{type="max"} 907345920
# HELP  jvm_old_size_bytes jvm old mem pool stat
# TYPE  jvm_old_size_bytes gauge
jvm_old_size_bytes{type="used"} 114633448
jvm_old_size_bytes{type="peak_used"} 114633448
jvm_old_size_bytes{type="max"} 7455834112
# HELP  jvm_young_gc jvm young gc stat
# TYPE  jvm_young_gc gauge
jvm_young_gc{type="count"} 247
jvm_young_gc{type="time"} 860
# HELP  jvm_old_gc jvm old gc stat
# TYPE  jvm_old_gc gauge
jvm_old_gc{type="count"} 3
jvm_old_gc{type="time"} 211
# HELP  jvm_thread jvm thread stat
# TYPE  jvm_thread gauge
jvm_thread{type="count"} 162
jvm_thread{type="peak_count"} 205
jvm_thread{type="new_count"} 0
jvm_thread{type="runnable_count"} 48
jvm_thread{type="blocked_count"} 1
jvm_thread{type="waiting_count"} 41
jvm_thread{type="timed_waiting_count"} 72
jvm_thread{type="terminated_count"} 0
...
```

这是一个以 [Prometheus 格式](https://prometheus.io/docs/practices/naming/) 呈现的监控数据。我们以其中一个监控项为例进行说明：

```
# HELP  jvm_heap_size_bytes jvm heap stat
# TYPE  jvm_heap_size_bytes gauge
jvm_heap_size_bytes{type="max"} 8476557312
jvm_heap_size_bytes{type="committed"} 1007550464
jvm_heap_size_bytes{type="used"} 156375280
```

1. "#" 开头的行为注释行。其中 HELP 为该监控项的描述说明；TYPE 表示该监控项的数据类型，示例中为 Gauge，即标量数据。还有 Counter、Histogram 等数据类型。具体可见 [Prometheus 官方文档](https://prometheus.io/docs/practices/instrumentation/#counter-vs.-gauge,-summary-vs.-histogram) 。
2. `jvm_heap_size_bytes` 即监控项的名称（Key）；`type="max"` 即为一个名为 `type` 的 Label，值为 `max`。一个监控项可以有多个 Label。
3. 最后的数字，如 `8476557312`，即为监控数值。

## 监控架构

整个监控架构如下图所示：

![](/images/monitor_arch.png)

1. 黄色部分为 Prometheus 相关组件。Prometheus Server 为 Prometheus 的主进程，目前 Prometheus 通过 Pull 的方式访问 Doris 节点的监控接口，然后将时序数据存入时序数据库 TSDB 中（TSDB 包含在 Prometheus 进程中，无需单独部署）。Prometheus 也支持通过搭建 [Push Gateway](https://github.com/prometheus/pushgateway) 的方式，允许被监控系统将监控数据通过 Push 的方式推到 Push Gateway, 再由 Prometheus Server 通过 Pull 的方式从 Push Gateway 中获取数据。
2. [Alert Manager](https://github.com/prometheus/alertmanager) 为 Prometheus 报警组件，需单独部署（暂不提供方案，可参照官方文档自行搭建）。通过 Alert Manager，用户可以配置报警策略，接收邮件、短信等报警。
3. 绿色部分为 Grafana 相关组件。Grafana Server 为 Grafana 的主进程。启动后，用户可以通过 Web 页面对 Grafana 进行配置，包括数据源的设置、用户设置、Dashboard 绘制等。这里也是最终用户查看监控数据的地方。


## 开始搭建

请在完成 Doris 的部署后，开始搭建监控系统。

### Prometheus

1. 在 [Prometheus 官网](https://prometheus.io/download/) 下载最新版本的 Prometheus 或者直接[点击下载](https://doris-community-test-1308700295.cos.ap-hongkong.myqcloud.com/monitor/prometheus-2.43.0.linux-amd64.tar.gz)。这里我们以 2.43.0-linux-amd64 版本为例。
2. 在准备运行监控服务的机器上，解压下载后的 tar 文件。
3. 打开配置文件 prometheus.yml。这里我们提供一个示例配置并加以说明（配置文件为 yml 格式，一定注意统一的缩进和空格）：

    这里我们使用最简单的静态文件的方式进行监控配置。Prometheus 支持多种 [服务发现](https://prometheus.io/docs/prometheus/latest/configuration/configuration/) 方式，可以动态的感知节点的加入和删除。
 
    ```
    # my global config
    global:
      scrape_interval:     15s # 全局的采集间隔，默认是 1m，这里设置为 15s
      evaluation_interval: 15s # 全局的规则触发间隔，默认是 1m，这里设置 15s
    
    # Alertmanager configuration
    alerting:
      alertmanagers:
      - static_configs:
        - targets:
          # - alertmanager:9093
    
    # A scrape configuration containing exactly one endpoint to scrape:
    # Here it's Prometheus itself.
    scrape_configs:
      # The job name is added as a label `job=<job_name>` to any timeseries scraped from this config.
      - job_name: 'DORIS_CLUSTER' # 每一个 Doris 集群，我们称为一个 job。这里可以给 job 取一个名字，作为 Doris 集群在监控系统中的名字。
        metrics_path: '/metrics' # 这里指定获取监控项的 restful api。配合下面的 targets 中的 host:port，Prometheus 最终会通过 host:port/metrics_path 来采集监控项。
        static_configs: # 这里开始分别配置 FE 和 BE 的目标地址。所有的 FE 和 BE 都分别写入各自的 group 中。
          - targets: ['fe_host1:8030', 'fe_host2:8030', 'fe_host3:8030']
            labels:
              group: fe # 这里配置了 fe 的 group，该 group 中包含了 3 个 Frontends
    
          - targets: ['be_host1:8040', 'be_host2:8040', 'be_host3:8040']
            labels:
              group: be # 这里配置了 be 的 group，该 group 中包含了 3 个 Backends
    
      - job_name: 'DORIS_CLUSTER_2' # 我们可以在一个 Prometheus 中监控多个 Doris 集群，这里开始另一个 Doris 集群的配置。配置同上，以下略。
        metrics_path: '/metrics'
        static_configs: 
          - targets: ['fe_host1:8030', 'fe_host2:8030', 'fe_host3:8030']
            labels:
              group: fe 
    
          - targets: ['be_host1:8040', 'be_host2:8040', 'be_host3:8040']
            labels:
              group: be 
                  
    ```

4. 启动 Prometheus

    通过以下命令启动 Prometheus：
    
    `nohup ./prometheus --web.listen-address="0.0.0.0:8181" &`
    
    该命令将后台运行 Prometheus，并指定其 web 端口为 8181。启动后，即开始采集数据，并将数据存放在 data 目录中。
    
5. 停止 Prometheus
    
    目前没有发现正式的进程停止方式，直接 kill -9 即可。当然也可以将 Prometheus 设为一种 service，以 service 的方式启停。
    
6. 访问 Prometheus

    Prometheus 可以通过 web 页面进行简单的访问。通过浏览器打开 8181 端口，即可访问 Prometheus 的页面。点击导航栏中，`Status` -> `Targets`，可以看到所有分组 Job 的监控主机节点。正常情况下，所有节点都应为 `UP`，表示数据采集正常。点击某一个 `Endpoint`，即可看到当前的监控数值。如果节点状态不为 UP，可以先访问 Doris 的 metrics 接口（见前文）检查是否可以访问，或查询 Prometheus 相关文档尝试解决。
    
7. 至此，一个简单的 Prometheus 已经搭建、配置完毕。更多高级使用方式，请参阅 [官方文档](https://prometheus.io/docs/introduction/overview/)

### Grafana

1. 在 [Grafana 官网](https://grafana.com/grafana/download) 下载最新版本的 Grafana 或者直接[点击下载](https://doris-community-test-1308700295.cos.ap-hongkong.myqcloud.com/monitor/grafana-enterprise-8.5.22.linux-amd64.tar.gz)。这里我们以 8.5.22.linux-amd64 版本为例。

2. 在准备运行监控服务的机器上，解压下载后的 tar 文件。

3. 打开配置文件 conf/defaults.ini。这里我们仅列举需要改动的配置项，其余配置可使用默认。

    ```
    # Path to where grafana can store temp files, sessions, and the sqlite3 db (if that is used)
    data = data
    
    # Directory where grafana can store logs
    logs = data/log
    
    # Protocol (http, https, socket)
    protocol = http
    
    # The ip address to bind to, empty will bind to all interfaces
    http_addr =
    
    # The http port to use
    http_port = 8182
    ```

4. 启动 Grafana

    通过以下命令启动 Grafana
    
    `nohup ./bin/grafana-server &`
    
    该命令将后台运行 Grafana，访问端口为上面配置的 8182
    
5. 停止 Grafana

    目前没有发现正式的进程停止方式，直接 kill -9 即可。当然也可以将 Grafana 设为一种 service，以 service 的方式启停。
    
6. 访问 Grafana

    通过浏览器，打开 8182 端口，可以开始访问 Grafana 页面。默认用户名密码为 admin。
    
7. 配置 Grafana

    初次登陆，需要根据提示设置数据源（data source）。我们这里的数据源，即上一步配置的 Prometheus。
    
    数据源配置的 Setting 页面说明如下：
    
    1. Name: 数据源的名称，自定义，比如 doris_monitor_data_source
    2. Type: 选择 Prometheus
    3. URL: 填写 Prometheus 的 web 地址，如 http://host:8181
    4. Access: 这里我们选择 Server 方式，即通过 Grafana 进程所在服务器，访问 Prometheus。
    5. 其余选项默认即可。
    6. 点击最下方 `Save & Test`，如果显示 `Data source is working`，即表示数据源可用。
    7. 确认数据源可用后，点击左边导航栏的 + 号，开始添加 Dashboard。这里我们已经准备好了 Doris 的 Dashboard 模板（本文档开头）。下载完成后，点击上方的 `New dashboard`->`Import dashboard`->`Upload .json File`，将下载的 json 文件导入。
    8. 导入后，可以命名 Dashboard，默认是 `Doris Overview`。同时，需要选择数据源，这里选择之前创建的 `doris_monitor_data_source`
    9. 点击 `Import`，即完成导入。之后，可以看到 Doris 的 Dashboard 展示。

8. 至此，一个简单的 Grafana 已经搭建、配置完毕。更多高级使用方式，请参阅 [官方文档](http://docs.grafana.org/)


## Dashboard 说明

这里我们简要介绍 Doris Dashboard。Dashboard 的内容可能会随版本升级，不断变化，本文档不保证是最新的 Dashboard 说明。

1. 顶栏

    ![](/images/dashboard_navibar.png)
    
    * 左上角为 Dashboard 名称。
    * 右上角显示当前监控时间范围，可以下拉选择不同的时间范围，还可以指定定时刷新页面间隔。
    * cluster\_name: 即 Prometheus 配置文件中的各个 job\_name，代表一个 Doris 集群。选择不同的 cluster，下方的图表将展示对应集群的监控信息。
    * fe_master: 对应集群的 Master Frontend 节点。
    * fe_instance: 对应集群的所有 Frontend 节点。选择不同的 Frontend，下方的图表将展示对应 Frontend 的监控信息。
    * be_instance: 对应集群的所有 Backend 节点。选择不同的 Backend，下方的图表将展示对应 Backend 的监控信息。
    * interval: 有些图表展示了速率相关的监控项，这里可选择以多大间隔进行采样计算速率（注：15s 间隔可能导致一些图表无法显示）。
    
2. Row

    ![](/images/dashboard_row.png)

    Grafana 中，Row 的概念，即一组图表的集合。如上图中的 Overview、Cluster Overview 即两个不同的 Row。可以通过点击 Row，对 Row 进行折叠。当前 Dashboard 有如下 Rows（持续更新中）：
    
    1. Overview: 所有 Doris 集群的汇总展示。
    2. Cluster Overview: 选定集群的汇总展示。
    3. Query Statistic: 选定集群的查询相关监控。
    4. FE JVM: 选定 Frontend 的 JVM 监控。
    5. BE: 选定集群的 Backends 的汇总展示。
    6. BE Task: 选定集群的 Backends 任务信息的展示。

3. 图表

    ![](/images/dashboard_panel.png)

    一个典型的图标分为以下几部分：
    
    1. 鼠标悬停左上角的 i 图标，可以查看该图表的说明。
    2. 点击下方的图例，可以单独查看某一监控项。再次点击，则显示所有。
    3. 在图表中拖拽可以选定时间范围。
    4. 标题的 [] 中显示选定的集群名称。
    5. 一些数值对应左边的Y轴，一些对应右边的，可以通过图例末尾的 `-right` 区分。
    6. 点击图表名称->`Edit`，可以对图表进行编辑。

## Dashboard 更新

1. 点击 Grafana 左边栏的 `+`，点击 `Dashboard`。
2. 点击左上角的 `New dashboard`，在点击右侧出现的 `Import dashboard`。
3. 点击 `Upload .json File`，选择最新的模板文件。
4. 选择数据源
5. 点击 `Import(Overwrite)`，完成模板更新。
