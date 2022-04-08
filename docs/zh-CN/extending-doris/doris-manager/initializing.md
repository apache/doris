---
{
    "title": "初始化",
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

# 初始化

完成部署之后，超级管理员需要完成本地初始化。

## 管理用户

初始化第一步为管理用户，主要完成对认证方式的选择和配置。目前 Doris Manger 支持本地用户认证。

![](/images/doris-manager/initializing-1.png)

### 本地用户认证

本地用户认证是 Doris Manger 自带的用户系统。通过填入用户名、邮箱和密码可完成用户注册，用户的新增、信息修改、删除和权限关系均在本地内完成。

![](/images/doris-manager/initializing-2.png)

此时已经完成了本地初始化过程。超级管理员可以创建空间，空间管理员进入空间，进行空间管理，添加邀请用户进入空间进行数据分析等工作。