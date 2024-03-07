{
"title": "Apache Superset 集成",
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


## 介绍
Apache Superset 是一个开源的数据挖掘平台，支持丰富的数据源连接，多种可视化方式，并能够对用户实现细粒度的权限控制。该工具主要特点是可自助分析、自定义仪表盘、分析结果可视化（导出）、用户/角色权限控制，还集成了一个 SQL 编辑器，可以进行 SQL 编辑查询等。

在 Apache Superset 3.1 版本中正式支持了 Apache Doris 的内部数据和外部数据进行查询和可视化处理。
## 前置条件
确保您已完成如下工具安装：
1. 在 Apache Superset 服务器上安装 Apache Doris 的 Python 客户端。
    ```
   pip install pydoris
   ```
2. 安装 Apache Superset 3.1 及其以上的版本。具体参见[安装 Superset 从 PyPI 库](https://superset.apache.org/docs/installation/installing-superset-from-pypi)或者[通过 Docker 容器的方式安装](https://hub.docker.com/r/apache/superset)

## 添加数据源
当你在 Apache Superset 中创建数据源时需要注意以下两点：
- 在 SUPPORTED DATABASES 里选择 Apache Doris 作为数据源。
- 在 SQLALCHEMY URI 里，按如下 Doris SQLAlchemy URI 格式填写 URI：

  ```doris://<User>:<Password>@<Host>:<Port>/<Catalog>.<Database>```
- URI 参数说明如下：
    - User：用于登录 Doris 集群的用户名，如 admin。
    - Password：用于登录 Doris 集群的用户密码。
    - Host：Doris 集群的 FE 主机 IP 地址。
    - Port：Doris 集群的 FE 查询端口，如 9030。
    - Catalog：Doris 集群中的目标 Catalog。Internal Catalog 和 External Catalog 均支持。
    - Database：Doris 集群中的目标数据库。内部数据库和外部数据库均支持。


:::tip
1. 当你使用最新的 Docker 镜像部署 Superset 时，如果发现找不到 Apache Doris 数据源，这个可能是因为 Superset Docker Image默认只包含基本的数据源构建，需要手动将 pydoris 包安装进来，您可以参考 [superset docker教程] (https://hub.docker.com/r/apache/superset)中的 How to extend this image 步骤进行 Apache Superset 的部署。
2. 推荐使用 Apache Doris 2.0.4 及以上版本。
:::
