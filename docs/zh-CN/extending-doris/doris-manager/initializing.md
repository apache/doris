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
完成部署之后，系统管理员需要完成本地初始化。

初始化分成两步，选择用户认证方式和新建空间连接集群。

## 管理用户

初始化第一步为管理用户，主要完成对认证方式的选择和配置。目前Doris Manger支持本地用户认证。


![](/images/doris-manager/initializing-1.png)

### 本地用户认证

本地用户认证是Doris Manger自带的用户系统。通过填入用户名、邮箱和密码可完成用户注册，用户的新增、信息修改、删除和权限关系均在本地内完成。

初始化时超级管理员选择后，无需配置任何信息，直接进行初始化的第二步，创建空间连接集群。

![](/images/doris-manager/initializing-2.png)

## 新建空间

新建空间包括新建集群和集群托管两种方式。

### 新建Doirs集群

#### 1 注册空间

空间信息包括空间名称、空间简介、选择空间管理员。

空间名称、管理员为必填/选字段。

![](/images/doris-manager/initializing-3.png)

#### 2 添加主机

![](/images/doris-manager/initializing-4.png)

##### 配置SSH免登陆

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

##### 主机列表
输入主机IP添加新的主机，也可通过批量添加。

#### 3 安装选项

![](/images/doris-manager/initializing-5.png)

##### 获取安装包
###### 1 源码部署
您可以通过以下连接下载对应版本的 Doris 源码进行编译和部署
http://doris.incubator.apache.org/zh-CN/downloads/downloads.html 源码部署根据不同版本的环境要求也不同。
Doris 0.14.0 之前的版本依赖如下：

    GCC 7.3+, Oracle JDK 1.8+, Python 2.7+, Apache Maven 3.5+, CMake 3.11+ Bison 3.0+

Doris 0.14.0 之后的依赖版本如下：

    GCC 10+, Oracle JDK 1.8+, Python 2.7+, Apache Maven 3.5+, CMake 3.19.2+ Bison 3.0+



###### 2 预编译部署

使用预编译版本进行安装
http://palo.baidu.com/docs/%E4%B8%8B%E8%BD%BD%E4%B8%93%E5%8C%BA/%E9%A2%84%E7%BC%96%E8%AF%91%E7%89%88%E6%9C%AC%E4%B8%8B%E8%BD%BD/ 
这里提供的预编译二进制文件仅在 CentOS 7.3, Intel(R) Xeon(R) Gold 6148 CPU @ 2.40GHz 上执行通过。在其他系统或 CPU 型号下，可能会因为 glibc 版本或者 CPU 支持的指令集不同，而导致程序无法运行。

部署时需要确保从output下copy的fe和be目录处于同一级。
列：
```
doris/fe
doris/be
doris/broker 
```

##### 搭建本地web服务器
在Doris Manager上安装集群时，需要提供Doris 的安装包，可以直接使用官网地址或将已有安装包放到自己的web服务器上。
###### 1 yum源安装
1.安装
yum install -y nginx
2.启动
systemctl start nginx
###### 2 源码安装
可参考：https://www.runoob.com/linux/nginx-install-setup.html

###### 3 配置

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

##### 指定安装路径

Doris与Doris Manger Agent将安装至该目录下。请确保该目录为Doirs以及相关组件专用。

#### 4 校验主机

系统会根据主机状态自动进行校验，当校验完成时既Agent启动回传心跳，可点击进行下一步。

![](/images/doris-manager/initializing-6.png)

#### 5 规划节点

点击分配节点按钮，对主机进行FE/BE/Broker节点的规划。

![](/images/doris-manager/initializing-7.png)

#### 6 配置参数

对上一步规划的节点进行配置参数，可以使用默认值也可以打开自定义配置开关对配置进行自定义。



#### 7 部署集群

系统会根据主机安装进度状态自动进行校验，当校验完成时既启动节点并回传心跳，可点击进行下一步。

![](/images/doris-manager/initializing-8.png)

#### 8 完成创建

完成以上步骤即可完成新建集群。

![](/images/doris-manager/initializing-9.png)

### 集群托管

#### 1 注册空间

空间信息包括空间名称、空间简介、选择空间管理员。

空间名称、管理员为必填/选字段。

![](/images/doris-manager/initializing-10.png)


#### 2 连接集群

集群信息包括集群地址、HTTP端口、JDBC端口、集群用户名和集群密码。用户可根据自身集群信息进行填写。

点击链接测试按钮进行测试。

![](/images/doris-manager/initializing-11.png)

#### 3 托管选项

![](/images/doris-manager/initializing-12.png)

##### 配置SSH免登陆

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

##### 指定安装路径

Doris与Doris Manger Agent将安装至该目录下。请确保该目录为Doirs以及相关组件专用。

#### 4 校验主机

系统会根据主机状态自动进行校验，当校验完成时既Agent启动回传心跳，可点击进行下一步。

![](/images/doris-manager/initializing-13.png)

#### 5 校验集群
校验集群分位实例安装校验、实例依赖校验、实例启动校验，校验成功后点击下一步即可完成创建。

![](/images/doris-manager/initializing-14.png)

#### 6 完成接入
完成以上步骤即可完成集群托管。

![](/images/doris-manager/initializing-15.png)

### 空间列表

点击左侧导航栏进入空间列表页，空间列表页用于展示用户拥有权限的空间名称。

空间列表分为已完成空间和未完成空间，已完成空间可以对其进行操作如进入空间、删除和编辑。
注意：仅空间管理员有权限进行删除和编辑权限

![](/images/doris-manager/initializing-16.png)

点击编辑按钮后可以对空间名称、空间简介、空间管理员进行相应编辑。

![](/images/doris-manager/initializing-17.png)

点击删除按钮后弹窗是否删除，点击确定删除成功。

![](/images/doris-manager/initializing-18.png)

点击上方二级导航栏进入未完成空间列表，未完成空间列表记录上次创建空间但未完成的操作，空间管理员可以继续对未完成空间进行恢复或者删除操作。

![](/images/doris-manager/initializing-19.png)

此时已经完成了本地初始化过程。空间管理员可以进入空间，进行空间管理，邀请用户进入空间进行数据分析等工作。

