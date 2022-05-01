---
{
    "title": "空间列表",
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

# 空间列表

超级管理员在空间列表主要可进行如下操作：

- 进行新建集群和集群托管操作

- 未完成空间的恢复和删除操作

- 已完成空间的删除操作

空间管理员在空间列表主要可进行如下操作：

- 查看有权限的空间信息

## 已完成空间

超级管理员可以通过空间名称右侧按钮对已完成空间进行操作。空间管理员可以点击进入空间，对空间内的集群或数据进行管理操作。

![](/images/doris-manager/spacelist-1.png)

## 未完成空间

Doris Manger 提供了空间创建流程的草稿保存功能，用以记录未完成的空间创建流程。超级管理员可从通过切换tab页查看未完成空间列表，进行恢复或是删除操作。

![](/images/doris-manager/spacelist-2.png)

# 新建空间

新建空间包括新建集群和集群托管两种方式。

## 新建集群

### 1 注册空间

空间信息包括空间名称、空间简介、选择空间管理员。

空间名称、管理员为必填/选字段。

![](/images/doris-manager/spacelist-3.png)

### 2 添加主机

![](/images/doris-manager/spacelist-4.png)

#### 配置SSH免登陆

Doris Manager 在安装时需要分发Agent安装包，故需要在待安装Doris的服务器(agent01)配置SSH免登陆。

```shell
#1.登录服务器，需要使用manager和agent账号保持一致
su - xxx
pwd
#2.在部署doris manager机器上生成密钥对
ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa

#3.将公钥拷贝到机器agent01上
scp  ~/.ssh/id_rsa.pub root@agent01:~

#4.登录agent01，将公钥追加到authorized_keys 
cat ~/id_rsa.pub >> .ssh/authorized_keys

#5.这样做完之后我们就可以在doris manger机器免密码登录agent01
ssh agent01@xx.xxx.xx.xx
```
详细可参考：https://blog.csdn.net/universe_hao/article/details/52296811

另外需要注意，.ssh目录的权限为700，其下文件authorized_keys和私钥的权限为600。否则会因为权限问题导致无法免密码登录。我们可以看到登陆后会有known_hosts文件生成。同时启动doris时需要使用免密码登录的账号。


在Doris Manager 安装集群第二步添加节点时，使用部署doris manager机器的私钥即可，即~/.ssh/id_rsa(注意：包括密钥文件的头尾)

#### 主机列表
输入主机IP添加新的主机，也可通过批量添加。

### 3 安装选项

#### 获取安装包

1. 代码包路径

   通过Doris Manager 进行集群部署时，需要提供已编译好的 Doris 安装包，您可以通过 Doris 源码自行编译，或使用官方提供的[二进制版本](https://doris.apache.org/zh-CN/downloads/downloads.html)。

`Doris Manager 将通过 http 方式拉取Doris安装包，若您需要自建 http 服务，请参考文档底部-自建http服务`。

#### 指定安装路径

1. Doris与Doris Manger Agent将安装至该目录下。请确保该目录为Doirs以及相关组件专用。
2. 指定Agent启动端口，默认为8001，若有冲突，可自定义。

### 4 校验主机

系统会根据主机状态自动进行校验，当校验完成时既Agent启动回传心跳，可点击进行下一步。

![](/images/doris-manager/spacelist-5.png)

### 5 规划节点

点击分配节点按钮，对主机进行FE/BE/Broker节点的规划。

![](/images/doris-manager/spacelist-6.png)

### 6 配置参数

对上一步规划的节点进行配置参数，可以使用默认值也可以打开自定义配置开关对配置进行自定义。

### 7 部署集群

系统会根据主机安装进度状态自动进行校验，当校验完成时既启动节点并回传心跳，可点击进行下一步。

![](/images/doris-manager/spacelist-7.png)

### 8 完成创建

完成以上步骤即可完成新建集群。

![](/images/doris-manager/spacelist-8.png)

## 集群托管

### 1 注册空间

空间信息包括空间名称、空间简介、选择空间管理员。

空间名称、管理员为必填/选字段。

### 2 连接集群

集群信息包括集群地址、HTTP端口、JDBC端口、集群用户名和集群密码。用户可根据自身集群信息进行填写。

点击链接测试按钮进行测试。

### 3 托管选项

![](/images/doris-manager/spacelist-9.png)

#### 配置SSH免登陆

Doris Manager 在安装时需要分发Agent安装包，故需要在待安装Doris的服务器(agent01)配置SSH免登陆。

```shell
#1.登录服务器，需要使用manger和agent账号保持一致
su - xxx
pwd
#2.在部署doris manager机器上生成密钥对
ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa

#3.将公钥拷贝到机器agent01上
scp  ~/.ssh/id_rsa.pub root@agent01:~

#4.登录agent01，将公钥追加到authorized_keys 
cat ~/id_rsa.pub >> .ssh/authorized_keys

#5.这样做完之后我们就可以在doris manger机器免密码登录agent01
ssh agent01@xx.xxx.xx.xx
```

另外需要注意，.ssh目录的权限为700，其下文件authorized_keys和私钥的权限为600。否则会因为权限问题导致无法免密码登录。我们可以看到登陆后会有known_hosts文件生成。同时启动doris时需要使用免密码登录的账号。

在Doris Manager 安装集群时，使用部署doris manager机器的私钥即可，即~/.ssh/id_rsa

详细可参考：https://blog.csdn.net/universe_hao/article/details/52296811

#### 指定安装路径

1. Doris与Doris Manger Agent将安装至该目录下。请确保该目录为Doirs以及相关组件专用。
2. 指定Agent启动端口，默认为8001，若有冲突，可自定义。

### 4 校验主机

系统会根据主机状态自动进行校验，当校验完成时既Agent启动回传心跳，可点击进行下一步。

![](/images/doris-manager/spacelist-10.png)

### 5 校验集群
校验集群分位实例安装校验、实例依赖校验、实例启动校验，校验成功后点击下一步即可完成创建。

![](/images/doris-manager/spacelist-11.png)

### 6 完成接入
完成以上步骤即可完成集群托管。

## 自建http服务

### 1 yum源安装
1.安装
yum install -y nginx
2.启动
systemctl start nginx

### 2 源码安装
可参考：https://www.runoob.com/linux/nginx-install-setup.html

### 3 配置

1.将doris安装包放置nginx根目录
mv PALO-0.15.1-rc03-binary.tar.gz /usr/share/nginx/html

2.修改ngixn.conf
```
location /download {
                alias /home/work/nginx/nginx/html/;
        }
```
修改后重启ngxin访问 ：
https://host:port/download/PALO-0.15.1-rc03-binary.tar.gz
