---
{
    "title": "空间管理",
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

# 空间管理

## 空间

空间管理员在空间模块下主要可进行如下操作：


- 查看已经保存的空间信息


### 查看空间信息

如果空间信息已经完备，空间管理员可以在此查看空间相关信息，包括空间名称、空间简介、空间管理员等等。空间管理员也可修改空间简介。

![](/images/doris-manager/spacemanagement-1.png)

## 成员

若您为Doris Manger的空间管理员，您将有权限对于空间、空间成员、角色等进行设置或管理。


在“空间管理”界面的二级导航栏中选择“成员”，进入成员管理页面。此页面可以查看当前空间下的所有用户。

![](/images/doris-manager/spacemanagement-2.png)

## 角色

空间管理员可以通过单击导航栏“角色”按钮，对空间内成员的角色进行管理。新成员默认属于"空间成员"角色。默认角色有"空间管理员"和"空间成员"且不可被其他管理员更改。


![](/images/doris-manager/spacemanagement-3.png)


### 创建角色

Doris Manger默认角色为“空间管理员”和“空间成员”。

- **空间管理员**：这是一个特殊的角色，该角色成员可以查看本空间内实例中的所有内容，可以访问和更改管理面板中的设置，包括更改权限！ 因此，请小心地将人员添加到该组。为了防止Doris Manger被锁，需确保该群组中至少有一个用户。
- **空间成员**：所有属于“空间成员”角色的用户，并且不能从该群组中被移除。为该角色设置许可权限是一个确保你知道Doris Manger新用户将会能够看到什么的好办法。

点击角色信息页面右侧的“创建角色”，可以根据需要将用户添加到对应角色，将用户权限设定为对应角色的统一权限。

![](/images/doris-manager/spacemanagement-4.png)

点击角色信息最右侧的相应操作，可以在对群组信息编辑的操作。

![](/images/doris-manager/spacemanagement-5.png)

### 编辑角色

点击角色名称对应的操作，可以对角色信息编辑的操作，包括编辑角色名称、添加/移除角色成员、删除角色等操作。

![](/images/doris-manager/spacemanagement-6.png)

##### 编辑角色名称

在对应的角色名称选择“编辑”操作可以修改角色名称。

![](/images/doris-manager/spacemanagement-7.png)

##### 添加/移除角色成员

点击对应的角色名称，查看角色详情信息，可以将空间内用户进行添加和移除至本角色。用户不能将本人移除角色外。

![](/images/doris-manager/spacemanagement-8.png)

![](/images/doris-manager/spacemanagement-9.png)

##### 删除角色

在对应的角色名称选择“删除”操作可以删除角色。

![](/images/doris-manager/spacemanagement-10.png)


