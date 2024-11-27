---
{
"title": "Apache Superset Integrate",
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


## Introduction
Apache Superset is an open-source data exploration platform. It supports a rich variety of data source connections and numerous visualization methods. It also enables fine-grained access control for users. The main features of this tool include self-service analysis, customizable dashboards, visualization of analytical results (with export functionality), and user/role permission control. Moreover, it integrates an SQL editor for conducting SQL editing and queries.

In Apache Superset version 3.1 official support has been introduced for querying and visualizing both internal and external data from Apache Doris.
## Preconditions
Ensure you have completed the following tool installations:
1. Install the Python client for Apache Doris on the Apache Superset server.
   pip install pydoris
2. Install Apache Superset version 3.1 or above. For detailed instructions, refer to [Installing Superset from PyPI](https://superset.apache.org/docs/installation/installing-superset-from-pypi/) or [Installing Superset Locally Using Docker Compose](https://hub.docker.com/r/apache/superset).
## Add data source
When creating a data source in Apache Superset, please pay attention to the following two points:
- Choose Apache Doris as the data source in SUPPORTED DATABASES.
- In the SQLALCHEMY URI, fill in the URI following the Doris SQLAlchemy URI format as shown below.

  ```doris://<User>:<Password>@<Host>:<Port>/<Catalog>.<Database>```
- URI parameters are explained as follows:
  - User: The username for logging into the Doris cluster, e.g., admin.
  - Password: The password for logging into the Doris cluster.
  - Host: The IP address of the FE (Frontend) host in the Doris cluster.
  - Port: The query port of the FE in the Doris cluster, e.g. 9030.
  - Catalog: The target Catalog in the Doris cluster. Both Internal Catalog and External Catalog are supported.
  - Database: The target database in the Doris cluster. Both internal and external databases are supported.


:::tip
1. When deploying Superset using the latest Docker image, if you encounter the issue of not finding the Apache Doris data source, it may be because the default [Superset Docker image](https://hub.docker.com/r/apache/superset) includes only basic data source builds. You need to manually install the pydoris package. You can refer to the 'How to extend this image' section in the Superset Docker tutorial for the deployment steps of Superset.
2. It is recommended to use Apache Doris 2.0.4 and above.
:::
