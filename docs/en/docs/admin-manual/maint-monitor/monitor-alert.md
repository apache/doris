---
{
    "title": "Monitoring and alarming",
    "language": "en"
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

# Monitoring and alarming

This document mainly introduces Doris's monitoring items and how to collect and display them. And how to configure alarm (TODO)

Dashboard template click download

| Doris Version | Dashboard Version                                                          |
|---------------|----------------------------------------------------------------------------|
| 1.2.x         | [revision 5](https://grafana.com/api/dashboards/9734/revisions/5/download) |

Dashboard templates are updated from time to time. The way to update the template is shown in the last section.

Welcome to provide better dashboard.

## Components

Doris uses [Prometheus](https://prometheus.io/) and [Grafana](https://grafana.com/) to collect and display input monitoring items.

![](/images/dashboard_overview.png)

1. Prometheus

	Prometheus is an open source system monitoring and alarm suite. It can collect monitored items by Pull or Push and store them in its own time series database. And through the rich multi-dimensional data query language, to meet the different data display needs of users.

2. Grafana

	Grafana is an open source data analysis and display platform. Support multiple mainstream temporal database sources including Prometheus. Through the corresponding database query statements, the display data is obtained from the data source. With flexible and configurable dashboard, these data can be quickly presented to users in the form of graphs.

> Note: This document only provides a way to collect and display Doris monitoring data using Prometheus and Grafana. In principle, these components are not developed or maintained. For more details on these components, please step through the corresponding official documents.

## Monitoring data

Doris's monitoring data is exposed through the HTTP interface of Frontend and Backend. Monitoring data is presented in the form of key-value text. Each Key may also be distinguished by different Labels. When the user has built Doris, the monitoring data of the node can be accessed in the browser through the following interfaces:

* Frontend: `fe_host:fe_http_port/metrics`
* Backend: `be_host:be_web_server_port/metrics`
* Broker: Not available for now

Users will see the following monitoring item results (for example, FE partial monitoring items):

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
    
This is a monitoring data presented in [Prometheus Format](https://prometheus.io/docs/practices/naming/). We take one of these monitoring items as an example to illustrate:

```
# HELP  jvm_heap_size_bytes jvm heap stat
# TYPE  jvm_heap_size_bytes gauge
jvm_heap_size_bytes{type="max"} 8476557312
jvm_heap_size_bytes{type="committed"} 1007550464
jvm_heap_size_bytes{type="used"} 156375280
```

1. Behavior commentary line at the beginning of "#". HELP is the description of the monitored item; TYPE represents the data type of the monitored item, and Gauge is the scalar data in the example. There are also Counter, Histogram and other data types. Specifically, you can see [Prometheus Official Document](https://prometheus.io/docs/practices/instrumentation/#counter-vs.-gauge,-summary-vs.-histogram).
2. `jvm_heap_size_bytes` is the name of the monitored item (Key); `type= "max"` is a label named `type`, with a value of `max`. A monitoring item can have multiple Labels.
3. The final number, such as `8476557312`, is the monitored value.

## Monitoring Architecture

The entire monitoring architecture is shown in the following figure:

![](/images/monitor_arch.png)

1. The yellow part is Prometheus related components. Prometheus Server is the main process of Prometheus. At present, Prometheus accesses the monitoring interface of Doris node by Pull, and then stores the time series data in the time series database TSDB (TSDB is included in the Prometheus process, and need not be deployed separately). Prometheus also supports building [Push Gateway](https://github.com/prometheus/pushgateway) to allow monitored data to be pushed to Push Gateway by Push by monitoring system, and then data from Push Gateway by Prometheus Server through Pull.
2. [Alert Manager](https://github.com/prometheus/alertmanager) is a Prometheus alarm component, which needs to be deployed separately (no solution is provided yet, but can be built by referring to official documents). Through Alert Manager, users can configure alarm strategy, receive mail, short messages and other alarms.
3. The green part is Grafana related components. Grafana Server is the main process of Grafana. After startup, users can configure Grafana through Web pages, including data source settings, user settings, Dashboard drawing, etc. This is also where end users view monitoring data.


## Start building

Please start building the monitoring system after you have completed the deployment of Doris.

Prometheus

1. Download the latest version of Prometheus on the [Prometheus Website](https://prometheus.io/download/) or [click to download](https://doris-community-test-1308700295.cos.ap-hongkong.myqcloud.com/monitor/prometheus-2.43.0.linux-amd64.tar.gz). Here we take version 2.43.0-linux-amd64 as an example.
2. Unzip the downloaded tar file on the machine that is ready to run the monitoring service.
3. Open the configuration file prometheus.yml. Here we provide an example configuration and explain it (the configuration file is in YML format, pay attention to uniform indentation and spaces):

	Here we use the simplest way of static files to monitor configuration. Prometheus supports a variety of [service discovery](https://prometheus.io/docs/prometheus/latest/configuration/configuration/), which can dynamically sense the addition and deletion of nodes.

    ```
    # my global config
    global:
      scrape_interval:     15s # Global acquisition interval, default 1 m, set to 15s
      evaluation_interval: 15s # Global rule trigger interval, default 1 m, set 15s here
    
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
      - job_name: 'DORIS_CLUSTER' # Each Doris cluster, we call it a job. Job can be given a name here as the name of Doris cluster in the monitoring system.
        metrics_path: '/metrics' # Here you specify the restful API to get the monitors. With host: port in the following targets, Prometheus will eventually collect monitoring items through host: port/metrics_path.
        static_configs: # Here we begin to configure the target addresses of FE and BE, respectively. All FE and BE are written into their respective groups.
          - targets: ['fe_host1:8030', 'fe_host2:8030', 'fe_host3:8030']
            labels:
              group: fe # Here configure the group of fe, which contains three Frontends
    
          - targets: ['be_host1:8040', 'be_host2:8040', 'be_host3:8040']
            labels:
              group: be # Here configure the group of be, which contains three Backends
    
      - job_name: 'DORIS_CLUSTER_2' # We can monitor multiple Doris clusters in a Prometheus, where we begin the configuration of another Doris cluster. Configuration is the same as above, the following is outlined.
        metrics_path: '/metrics'
        static_configs: 
          - targets: ['fe_host1:8030', 'fe_host2:8030', 'fe_host3:8030']
            labels:
              group: fe 
    
          - targets: ['be_host1:8040', 'be_host2:8040', 'be_host3:8040']
            labels:
              group: be 
                  
    ```

4. start Prometheus

	Start Prometheus with the following command:

	`nohup ./prometheus --web.listen-address="0.0.0.0:8181" &`

	This command will run Prometheus in the background and specify its Web port as 8181. After startup, data is collected and stored in the data directory.

5. stop Promethues

	At present, there is no formal way to stop the process, kill - 9 directly. Of course, Prometheus can also be set as a service to start and stop in a service way.

6. access Prometheus

	Prometheus can be easily accessed through web pages. The page of Prometheus can be accessed by opening port 8181 through browser. Click on the navigation bar, `Status` -> `Targets`, and you can see all the monitoring host nodes of the grouped Jobs. Normally, all nodes should be `UP`, indicating that data acquisition is normal. Click on an `Endpoint` to see the current monitoring value. If the node state is not UP, you can first access Doris's metrics interface (see previous article) to check whether it is accessible, or query Prometheus related documents to try to resolve.

7. So far, a simple Prometheus has been built and configured. For more advanced usage, see [Official Documents](https://prometheus.io/docs/introduction/overview/)

### Grafana

1. Download the latest version of Grafana on [Grafana's official website](https://grafana.com/grafana/download) or [click to download](https://doris-community-test-1308700295.cos.ap-hongkong.myqcloud.com/monitor/grafana-enterprise-8.5.22.linux-amd64.tar.gz). Here we take version 8.5.22.linux-amd64 as an example.

2. Unzip the downloaded tar file on the machine that is ready to run the monitoring service.

3. Open the configuration file conf/defaults.ini. Here we only list the configuration items that need to be changed, and the other configurations can be used by default.

    ```
    # Path to where grafana can store temp files, sessions, and the sqlite3 db (if that is used)
    data = data
    
    # Directory where grafana can store logs
    logs = data/log
    
    # Protocol (http, https, socket)
    protocal = http
    
    # The ip address to bind to, empty will bind to all interfaces
    http_addr =
    
    # The http port to use
    http_port = 8182
    ```

4. start Grafana

	Start Grafana with the following command

	`nohup ./bin/grafana-server &`

	This command runs Grafana in the background, and the access port is 8182 configured above.

5. stop Grafana

	At present, there is no formal way to stop the process, kill - 9 directly. Of course, you can also set Grafana as a service to start and stop as a service.

6. access Grafana

	Through the browser, open port 8182, you can start accessing the Grafana page. The default username password is admin.

7. Configure Grafana

	For the first landing, you need to set up the data source according to the prompt. Our data source here is Prometheus, which was configured in the previous step.

	The Setting page of the data source configuration is described as follows:

	1. Name: Name of the data source, customized, such as doris_monitor_data_source
	2. Type: Select Prometheus
	3. URL: Fill in the web address of Prometheus, such as http://host:8181
	4. Access: Here we choose the Server mode, which is to access Prometheus through the server where the Grafana process is located.
	5. The other options are available by default.
	6. Click `Save & Test` at the bottom. If `Data source is working`, it means that the data source is available.
	7. After confirming that the data source is available, click on the + number in the left navigation bar and start adding Dashboard. Here we have prepared Doris's dashboard template (at the beginning of this document). When the download is complete, click `New dashboard` -> `Import dashboard` -> `Upload.json File` above to import the downloaded JSON file.
	8. After importing, you can name Dashboard by default `Doris Overview`. At the same time, you need to select the data source, where you select the `doris_monitor_data_source` you created earlier.
	9. Click `Import` to complete the import. Later, you can see Doris's dashboard display.

8. So far, a simple Grafana has been built and configured. For more advanced usage, see [Official Documents](http://docs.grafana.org/)


## Dashboard

Here we briefly introduce Doris Dashboard. The content of Dashboard may change with the upgrade of version. This document is not guaranteed to be the latest Dashboard description.

1. Top Bar

	![](/images/dashboard_navibar.png)

	* The upper left corner is the name of Dashboard.
	* The upper right corner shows the current monitoring time range. You can choose different time ranges by dropping down. You can also specify a regular refresh page interval.
	* Cluster name: Each job name in the Prometheus configuration file represents a Doris cluster. Select a different cluster, and the chart below shows the monitoring information for the corresponding cluster.
	* fe_master: The Master Frontend node corresponding to the cluster.
	* fe_instance: All Frontend nodes corresponding to the cluster. Select a different Frontend, and the chart below shows the monitoring information for the Frontend.
	* be_instance: All Backend nodes corresponding to the cluster. Select a different Backend, and the chart below shows the monitoring information for the Backend.
	* Interval: Some charts show rate-related monitoring items, where you can choose how much interval to sample and calculate the rate (Note: 15s interval may cause some charts to be unable to display).

2. Row.

	![](/images/dashboard_row.png)

	In Grafana, the concept of Row is a set of graphs. As shown in the figure above, Overview and Cluster Overview are two different Rows. Row can be folded by clicking Row. Currently Dashboard has the following Rows (in continuous updates):

	1. Overview: A summary display of all Doris clusters.
	2. Cluster Overview: A summary display of selected clusters.
	3. Query Statistic: Query-related monitoring of selected clusters.
	4. FE JVM: Select Frontend's JVM monitoring.
	5. BE: A summary display of the backends of the selected cluster.
	6. BE Task: Display of Backends Task Information for Selected Clusters.

3. Charts

	![](/images/dashboard_panel.png)

	A typical icon is divided into the following parts:

	1. Hover the I icon in the upper left corner of the mouse to see the description of the chart.
	2. Click on the illustration below to view a monitoring item separately. Click again to display all.
	3. Dragging in the chart can select the time range.
	4. The selected cluster name is displayed in [] of the title.
	5. Some values correspond to the Y-axis on the left and some to the right, which can be distinguished by the `-right` at the end of the legend.
	6. Click on the name of the chart -> `Edit` to edit the chart.

## Dashboard Update

1. Click on `+` in the left column of Grafana and `Dashboard`.
2. Click `New dashboard` in the upper left corner, and `Import dashboard` appears on the right.
3. Click `Upload .json File` to select the latest template file.
4. Selecting Data Sources
5. Click on `Import (Overwrite)` to complete the template update.
