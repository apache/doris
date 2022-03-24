---
{
    "title": "系统设置",
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

# 系统设置

超级管理员管理员在平台模块下主要可进行如下操作：


- 对平台用户进行相关操作

## 用户

### 本地认证下用户管理

单击页面右侧“添加用户”按钮，可以输入用户名、邮箱。新用户默认不属于"超级管理员"。

Palo Studio将会为新用户分配临时密码，新用户需要使用所设定的用户名/邮箱和临时密码登录，登录后可在“账户设置”中创建新密码。

![](/images/doris-manager/systemsettings-1.png)

![](/images/doris-manager/systemsettings-2.png)


### 编辑用户

点击用户账号信息最右侧的三个按钮，可以对用户信息编辑的操作，包括编辑用户信息、重置用户密码、停用用户等操作。用户不能对本人进行“停用用户”的操作。

![](/images/doris-manager/systemsettings-3.png)

##### 编辑用户信息

点击选择选择“编辑”可以修改用户名、邮箱。若更新用户邮箱，则用户需要使用更新后的邮箱进行登录，密码不会被更新。

![](/images/doris-manager/systemsettings-4.png)

![](/images/doris-manager/systemsettings-5.png)

##### 重置用户密码

点击选择“重置密码”，确认执行此操作后，Doris Manger将会为该用户重新分配临时密码，用户需要使用所设定的邮箱和新的临时密码登录，登录后可在“账户设置”中创建新密码。

![](/images/doris-manager/systemsettings-6.png)

![](/images/doris-manager/systemsettings-7.png)

##### 停用/激活用户

点击选择“停用用户”，确认停用该用户后，该用户状态将由“启用”变更为“停用”。已停用用户将无法登录Doris Manger。

![](/images/doris-manager/systemsettings-8.png)

![](/images/doris-manager/systemsettings-9.png)

点击用户右侧的激活用户，可以“重新激活”该用户。该用户状态将重新变更为“启用”，能够再次登录Doris Manger，并且将被放回到被停用之前所在的角色中。

![](/images/doris-manager/systemsettings-10.png)

![](/images/doris-manager/systemsettings-11.png)

