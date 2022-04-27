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

超级管理员在平台模块下主要可进行如下操作：

- 对平台用户进行相关操作
- 拥有平台最高级权限

用户权限说明

## 用户

### 本地认证下用户管理

单击添加用户按钮，可以通过用户名与邮箱信息创建新用户。

 Doris Manger 将会为新用户分配临时密码，新用户需要使用所设定的用户名/邮箱和临时密码登录，登录后可在“账户设置”中创建新密码。

![](/images/doris-manager/systemsettings-1.png)

![](/images/doris-manager/systemsettings-2.png)


### 编辑用户

超级管理员可以对用户进行管理，包括编辑用户信息、重置用户密码、停用用户等操作。

#### 编辑用户信息

点击选择选择“编辑”可以修改用户名、邮箱。若更新用户邮箱，则用户需要使用更新后的邮箱进行登录，密码不会被更新。

![](/images/doris-manager/systemsettings-3.png)

#### 重置用户密码

点击选择“重置密码”，确认执行此操作后， Doris Manger 将会为该用户重新分配临时密码，用户需要使用所设定的邮箱和新的临时密码登录，登录后可在“账户设置”中创建新密码。


#### 停用/激活用户

点击选择停用用户，确认停用该用户后，该用户状态将由启用变更为停用。已停用用户将无法登录 Doris Manger 。

点击用户右侧的激活用户，可以重新激活该用户。该用户状态将重新变更为启用，能够再次登录 Doris Manger 。

注意，超级管理员不能停用自己的用户账户，系统内至少要有一位非停用状态的超级管理员用户。

![](/images/doris-manager/systemsettings-4.png)


## 用户权限说明

### 超级管理员权限
|   | 创建  | 编辑  | 删除  | 查看  |
|:----------|:----------|:----------|:----------|:----------|
| 用户    | ✓    | ✓    | ✓    | ✓    |
| 角色    | ✓    | ✓    | ✓    | ✓    |
| 空间    | ✓    | ✓    | ✓    | ✓    |

### 空间管理员权限
|   | 创建  | 编辑  | 删除  | 查看  |
|:----------|:----------|:----------|:----------|:----------|
| 用户    | X    | X    | X    | X    |
| 角色    | X    | X    | X    | ✓    |
| 空间    | X    | ✓    | X    | ✓    |

