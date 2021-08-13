---
{
    "title": "LDAP",
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

# LDAP

接入第三方LDAP服务为Doris提供验证登录和组授权服务。

LDAP验证登录指的是接入LDAP服务的密码验证来补充Doris的验证登录。Doris优先使用LDAP验证用户密码，如果LDAP服务中不存在该用户则继续使用Doris验证密码，如果LDAP密码正确但是Doris中没有对应账户则创建临时用户登录Doris。

LDAP组授权是将LDAP中的group映射到Doris中的Role，如果用户在LDAP中属于多个用户组，登录Doris后用户将获得所有组对应Role的权限，要求组名与Role名字相同。

## 名词解释

* LDAP： 轻量级目录访问协议，能够实现账号密码的集中管理。
* 权限 Privilege：权限作用的对象是节点、数据库或表。不同的权限代表不同的操作许可。
* 角色 Role：Doris可以创建自定义命名的角色。角色可以被看做是一组权限的集合。

## 启用LDAP认证
### server端配置

需要在fe/conf/ldap.conf文件中配置LDAP基本信息，另有LDAP管理员密码需要使用sql语句进行设置。

#### 配置fe/conf/ldap.conf文件：
* ldap_authentication_enabled = false  
  设置值为“true”启用LDAP验证；当值为“false”时，不启用LDAP验证，该配置文件的其他配置项都无效。
  
* ldap_host = 127.0.0.1  
  LDAP服务ip。
  
* ldap_port = 389  
  LDAP服务端口，默认明文传输端口为389，目前Doris的LDAP功能仅支持明文密码传输。
  
* ldap_admin_name = cn=admin,dc=domain,dc=com    
  LDAP管理员账户“Distinguished Name”。当用户使用LDAP验证登录Doris时，Doris会绑定该管理员账户在LDAP中搜索用户信息。
  
* ldap_user_basedn = ou=people,dc=domain,dc=com  
  Doris在LDAP中搜索用户信息时的base dn。
  
* ldap_user_filter = (&(uid={login}))  
* 
  Doris在LDAP中搜索用户信息时的过滤条件，占位符“{login}”会被替换为登录用户名。必须保证通过该过滤条件搜索的用户唯一，否则Doris无法通过LDAP验证密码，登录时会出现“ERROR 5081 (42000): user is not unique in LDAP server.”的错误信息。
  
  例如使用LDAP用户节点uid属性作为登录Doris的用户名可以配置该项为：  
  ldap_user_filter = (&(uid={login}))；  
  使用LDAP用户邮箱前缀作为用户名可配置该项：  
  ldap_user_filter = (&(mail={login}@baidu.com))。

* ldap_group_basedn = ou=group,dc=domain,dc=com  
  Doris在LDAP中搜索组信息时的base dn。如果不配置该项，将不启用LDAP组授权。

#### 设置LDAP管理员密码：
配置好ldap.conf文件后启动fe，使用root或admin账号登录Doris，执行sql：
```
set ldap_admin_password = 'ldap_admin_password';
```

### Client端配置
客户端使用LDAP验证需要启用mysql客户端明文验证插件，使用命令行登录Doris可以使用下面两种方式之一启用mysql明文验证插件：

* 设置环境变量LIBMYSQL_ENABLE_CLEARTEXT_PLUGIN值1。
  
  例如在linux或者max环境中可以使用：
  ```
  echo "export LIBMYSQL_ENABLE_CLEARTEXT_PLUGIN=1" >> ～/.bash_profile && source ～/.bash_profile
  ```
  
* 每次登录Doris时添加参数“--enable-cleartext-plugin”：
  ```
  mysql -hDORIS_HOST -PDORIS_PORT -u user -p --enable-cleartext-plugin

  输入ldap密码
  ```
  
## LDAP认证详解
LDAP密码验证和组授权是Doris密码验证和授权的补充，开启LDAP功能并不能完全替代Doris的密码验证和授权，而是与Doris密码验证和授权并存。

### LDAP验证登录详解
开启LDAP后，用户在Doris和DLAP中存在以下几种情况：

|LDAP用户|Doris用户|密码|登录情况|登录Doris的用户|
|--|--|--|--|--|
|存在|存在|LDAP密码|登录成功|Doris用户|
|存在|存在|Doris密码|登录失败|无|
|不存在|存在|Doris密码|登录成功|Doris用户|
|存在|不存在|LDAP密码|登录成功|Ldap临时用户|

开启LDAP后，用户使用mysql client登录时，Doris会先通过LDAP服务验证用户密码，如果LDAP存在用户且密码正确，Doris则使用该用户登录；此时Doris若存在对应账户则直接登录该账户，如果不存在对应账户则为用户创建临时账户并登录该账户。临时账户具有具有相应对权限（参见LDAP组授权），仅对当前连接有效，doris不会创建该用户，也不会产生创建用户对元数据。  
如果LDAP服务中不存在登录用户，则使用Doris进行密码认证。

以下假设已开启LDAP认证，配置ldap_user_filter = (&(uid={login}))，且其他配置项都正确,客户端设置环境变量LIBMYSQL_ENABLE_CLEARTEXT_PLUGIN=1

例如：

#### 1:Doris和LDAP中都存在账户：

存在Doris账户：jack@'172.10.1.10'，密码：123456  
LDAP用户节点存在属性：uid: jack 用户密码：abcdef  
使用以下命令登录Doris可以登录jack@'172.10.1.10'账户：
```
mysql -hDoris_HOST -PDoris_PORT -ujack -p abcdef
```

使用以下命令将登录失败：
```
mysql -hDoris_HOST -PDoris_PORT -ujack -p 123456
```

#### 2:LDAP中存在用户，Doris中不存在对应账户：

LDAP用户节点存在属性：uid: jack 用户密码：abcdef  
使用以下命令创建临时用户并登录jack@'%'，临时用户具有基本权限 DatabasePrivs：Select_priv， 用户退出登录后Doris将删除该临时用户：
```
mysql -hDoris_HOST -PDoris_PORT -ujack -p abcdef
```

#### 3:LDAP不存在用户：

存在Doris账户：jack@'172.10.1.10'，密码：123456  
使用Doris密码登录账户，成功：
```
mysql -hDoris_HOST -PDoris_PORT -ujack -p 123456
```

### LDAP组授权详解

DLAP用户dn是LDAP组节点的“member”属性则Doris认为用户属于该组。LDAP组授权是将LDAP中的group映射到Doris中的role，并将所有对应的role权限授予登录用户，用户退出登录后Doris会撤销对应的role权限。在使用LDAP组授权前应该在Doris中创建相应对role，并为role授权。

登录用户权限跟Doris用户和组权限有关，见下表：
|LDAP用户|Doris用户|登录用户的权限|
|--|--|--|
|存在|存在|LDAP组权限 + Doris用户权限|
|不存在|存在|Doris用户权限|
|存在|不存在|LDAP组权限|

如果登录的用户为临时用户，且不存在组权限，则该用户默认具有information_schema的select_priv权限

举例：  
LDAP用户dn是LDAP组节点的“member”属性则认为用户属于该组，Doris会截取组dn的第一个Rdn作为组名。  
例如用户dn为“uid=jack,ou=aidp,dc=domain,dc=com”， 组信息如下：  
```
dn: cn=doris_rd,ou=group,dc=domain,dc=com  
objectClass: groupOfNames  
member: uid=jack,ou=aidp,dc=domain,dc=com  
```
则组名为doris_rd。

假如jack还属于LDAP组doris_qa、doris_pm；Doris存在role：doris_rd、doris_qa、doris_pm，在使用LDAP验证登录后，用户不但具有该账户原有的权限，还将获得role doris_rd、doris_qa和doris_pm的权限。

## LDAP验证的局限

* 目前Doris的LDAP功能只支持明文密码验证，即用户登录时，密码在client与fe之间、fe与LDAP服务之间以明文的形式传输。
* 当前的LDAP验证只支持在mysql协议下进行密码验证，如果使用Http接口则无法使用LDAP用户进行验证。
* 临时用户不具有用户属性。

