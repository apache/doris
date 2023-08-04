---
{
    "title": "Debug Log",
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


# Debug Log

The system operation logs of Doris's FE and BE nodes are at INFO level by default. It can usually satisfy the analysis of system behavior and the localization of basic problems. However, in some cases, it may be necessary to enable DEBUG level logs to further troubleshoot the problem. This document mainly introduces how to enable the DEBUG log level of FE and BE nodes.

> It is not recommended to adjust the log level to WARN or higher, which is not conducive to the analysis of system behavior and the location of problems.

>Enable DEBUG log may cause a large number of logs to be generated, **Please be careful to open it in production environment**.

## Enable FE Debug Log

The Debug level log of FE can be turned on by modifying the configuration file, or it can be turned on at runtime through the interface or API.

1. Open via configuration file

   Add the configuration item `sys_log_verbose_modules` to fe.conf. An example is as follows:

   ````text
   # Only enable Debug log for class org.apache.doris.catalog.Catalog
   sys_log_verbose_modules=org.apache.doris.catalog.Catalog
   
   # Open the Debug log of all classes under the package org.apache.doris.catalog
   sys_log_verbose_modules=org.apache.doris.catalog
   
   # Enable Debug logs for all classes under package org
   sys_log_verbose_modules=org
   ````

   Add configuration items and restart the FE node to take effect.

2. Via FE UI interface

   The log level can be modified at runtime through the UI interface. There is no need to restart the FE node. Open the http port of the FE node (8030 by default) in the browser, and log in to the UI interface. Then click on the `Log` tab in the upper navigation bar.

   ![image.png](https://bce.bdstatic.com/doc/BaiduDoris/DORIS/image_f87b8c1.png)

   We can enter the package name or specific class name in the Add input box to open the corresponding Debug log. For example, enter `org.apache.doris.catalog.Catalog` to open the Debug log of the Catalog class:

   ![image.png](https://bce.bdstatic.com/doc/BaiduDoris/DORIS/image_f0d4a23.png)

   You can also enter the package name or specific class name in the Delete input box to close the corresponding Debug log.

   > The modification here will only affect the log level of the corresponding FE node. Does not affect the log level of other FE nodes.

3. Modification via API

   The log level can also be modified at runtime via the following API. There is no need to restart the FE node.

   ```bash
   curl -X POST -uuser:passwd fe_host:http_port/rest/v1/log?add_verbose=org.apache.doris.catalog.Catalog
   ````

   The username and password are the root or admin users who log in to Doris. The `add_verbose` parameter specifies the package or class name to enable Debug logging. Returns if successful:

   ````json
   {
       "msg": "success",
       "code": 0,
       "data": {
           "LogConfiguration": {
               "VerboseNames": "org,org.apache.doris.catalog.Catalog",
               "AuditNames": "slow_query,query,load",
               "Level": "INFO"
           }
       },
       "count": 0
   }
   ````

   Debug logging can also be turned off via the following API:

   ```bash
   curl -X POST -uuser:passwd fe_host:http_port/rest/v1/log?del_verbose=org.apache.doris.catalog.Catalog
   ````

   The `del_verbose` parameter specifies the package or class name for which to turn off Debug logging.

## Enable BE Debug Log

BE's Debug log currently only supports modifying and restarting the BE node through the configuration file to take effect.

````text
sys_log_verbose_modules=plan_fragment_executor,olap_scan_node
sys_log_verbose_level=3
````

`sys_log_verbose_modules` specifies the file name to be opened, which can be specified by the wildcard *. for example:

````text
sys_log_verbose_modules=*
````

Indicates that all DEBUG logs are enabled.

`sys_log_verbose_level` indicates the level of DEBUG. The higher the number, the more detailed the DEBUG log. The value range is 1-10.