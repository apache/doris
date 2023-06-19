---
{
    "title": "服务自动拉起",
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

# 服务自动拉起

本文档主要介绍如何配置Doris集群的自动拉起，保证生产环境中出现特殊情况导致服务宕机后未及时拉起服务从而影响到业务的正常运行。

Doris集群必须完全搭建完成后再配置FE和BE的自动拉起服务。

## Systemd配置Doris服务

systemd具体使用以及参数解析可以参考[这里](https://blog.51cto.com/arm2012/1963238) 

### sudo 权限控制

在使用 systemd 控制 doris 服务时，需要有 sudo 权限。为了保证最小粒度的 sudo 权限分配，可以将 doris-fe 与 doris-be 服务的 systemd 控制权限分配给指定的非 root 用户。在 visudo 来配置 doris-fe 与 doris-be 的 systemctl 管理权限。

```
Cmnd_Alias DORISCTL=/usr/bin/systemctl start doris-fe,/usr/bin/systemctl stop doris-fe,/usr/bin/systemctl start doris-be,/usr/bin/systemctl stop doris-be

## Allow root to run any commands anywhere
root    ALL=(ALL)       ALL
doris   ALL=(ALL)       NOPASSWD:DORISCTL
```

### 配置步骤

1. 下载doris-fe.service文件: [doris-fe.service](https://github.com/apache/doris/blob/master/tools/systemd/doris-fe.service)

2. doris-fe.service具体内容如下:

    ```
    # Licensed to the Apache Software Foundation (ASF) under one
    # or more contributor license agreements.  See the NOTICE file
    # distributed with this work for additional information
    # regarding copyright ownership.  The ASF licenses this file
    # to you under the Apache License, Version 2.0 (the
    # "License"); you may not use this file except in compliance
    # with the License.  You may obtain a copy of the License at
    #
    #   http://www.apache.org/licenses/LICENSE-2.0
    #
    # Unless required by applicable law or agreed to in writing,
    # software distributed under the License is distributed on an
    # "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    # KIND, either express or implied.  See the License for the
    # specific language governing permissions and limitations
    # under the License.

    [Unit]
    Description=Doris FE
    After=network-online.target
    Wants=network-online.target

    [Service]
    Type=forking
    User=root
    Group=root
    LimitCORE=infinity
    LimitNOFILE=200000
    Restart=on-failure
    RestartSec=30
    StartLimitInterval=120
    StartLimitBurst=3
    KillMode=none
    ExecStart=/home/doris/fe/bin/start_fe.sh --daemon 
    ExecStop=/home/doris/fe/bin/stop_fe.sh

    [Install]
    WantedBy=multi-user.target
    ```

#### 注意事项

- ExecStart、ExecStop根据实际部署的fe的路径进行配置

3. 下载doris-be.service文件: [doris-be.service](https://github.com/apache/doris/blob/master/tools/systemd/doris-be.service)

4. doris-be.service具体内容如下: 
    ```
    # Licensed to the Apache Software Foundation (ASF) under one
    # or more contributor license agreements.  See the NOTICE file
    # distributed with this work for additional information
    # regarding copyright ownership.  The ASF licenses this file
    # to you under the Apache License, Version 2.0 (the
    # "License"); you may not use this file except in compliance
    # with the License.  You may obtain a copy of the License at
    #
    #   http://www.apache.org/licenses/LICENSE-2.0
    #
    # Unless required by applicable law or agreed to in writing,
    # software distributed under the License is distributed on an
    # "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    # KIND, either express or implied.  See the License for the
    # specific language governing permissions and limitations
    # under the License.

    [Unit]
    Description=Doris BE
    After=network-online.target
    Wants=network-online.target

    [Service]
    Type=forking
    User=root
    Group=root
    LimitCORE=infinity
    LimitNOFILE=200000
    Restart=on-failure
    RestartSec=30
    StartLimitInterval=120
    StartLimitBurst=3
    KillMode=none
    ExecStart=/home/doris/be/bin/start_be.sh --daemon
    ExecStop=/home/doris/be/bin/stop_be.sh

    [Install]
    WantedBy=multi-user.target
    ```

#### 注意事项

- ExecStart、ExecStop根据实际部署的be的路径进行配置

5. 服务配置

   将doris-fe.service、doris-be.service两个文件放到 /usr/lib/systemd/system 目录下

6. 设置自启动

    添加或修改配置文件后，需要重新加载

    ```
    systemctl daemon-reload
    ```

    设置自启动，实质就是在 /etc/systemd/system/multi-user.target.wants/ 添加服务文件的链接

    ```
    systemctl enable doris-fe
    systemctl enable doris-be
    ```

7. 服务启动

    ```
    systemctl start doris-fe
    systemctl start doris-be
    ```










