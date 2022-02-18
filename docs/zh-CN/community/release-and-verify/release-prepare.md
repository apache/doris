---
{
"title": "发版准备",
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

# 发版准备

Apache 项目的版本发布必须严格遵循 Apache 基金会的版本发布流程。相关指导和政策可参阅：

* [Release Creation Process](https://infra.apache.org/release-publishing)
* [Release Policy](https://www.apache.org/legal/release-policy.html)
* [Publishing Maven Releases to Maven Central Repository](https://infra.apache.org/publishing-maven-artifacts.html)

本文档主要说明版本发布的主要流程和前期准备工作。具体 Doris 各组件的发版步骤，可以参阅各自的文档：

* [Doris Core Release](./release-doris-core.md)
* [Doris Connectors Release](./release-doris-connectors.md)
* [Doris Manager Release](./release-doris-manager.md)

Apache 项目的版本发布主要有以下三种形式：

* **Source Release：即源码发布，这个是必选项。**
* Binary Release：即二进制发布，比如发布编译好的可执行程序。这个是可选项。
* Convenience Binaries：为方便用户使用而发布到第三方平台的 Release。如Maven、Docker等。这个也是可选项。

## 发版流程

每个项目的发版都需要一位 PMC 成员或 Committer 作为 **Release Manager**。

总体的发版流程如下：

1. 环境准备
2. 发布准备
	1. 社区发起 DISCUSS 并与社区交流具体发布计划
	2. 创建分支用于发布
	3. 清理 issue
	4. 将必要的 Patch 合并到发布的分支
3. 验证分支
	1. QA 稳定性测试
	2. 验证分支代码的编译流程
	3. 准备 Release Nodes
4. 准备发布材料
    1. 打 Tag
    2. 将需要发布的内容上传至 [Apache Dev SVN 仓库](https://dist.apache.org/repos/dist/dev/incubator/doris)
    3. 其他 Convenience Binaries 的准备（如上传到 [Maven Staging 仓库](https://repository.apache.org/#stagingRepositories)）
4. 社区发布投票流程
	2. 在 [Doris 社区 Dev 邮件组](dev@doris.apache.org)发起投票。
	3. 投票通过后，在 Doris 社区发 Result 邮件。
	4. 如果是孵化项目（Incuator Project）还需：
	   1. 在 [Incubator General 邮件组](general@incubator.apache.org) 发起投票。
	   2. 发 Result 邮件到 general@incubator.apache.org。
5. 完成工作
	1. 上传签名的软件包到 [Apache Release 仓库](https://dist.apache.org/repos/dist/release/incubator/doris)，并生成相关链接。
	2. 在 Doris 官网和 github 发布下载链接，并且清理 svn 上的旧版本包。
	3. 发送 Announce 邮件到 dev@doris.apache.org
	4. 如果是孵化项目（Incuator Project）还需：
	   1. 发送 Announce 邮件到 general@incubator.apache.org

## 准备签名

如果这是你第一次发布，那么你需要在你的环境中准备如下工具

1. [Release Signing](https://www.apache.org/dev/release-signing.html)
2. [gpg](https://www.apache.org/dev/openpgp.html)
3. [svn](https://www.apache.org/dev/openpgp.html)

### 准备gpg key

Release manager 在发布前需要先生成自己的签名公钥，并上传到公钥服务器，之后就可以用这个公钥对准备发布的软件包进行签名。
如果在[KEY](https://downloads.apache.org/incubator/doris/KEYS)里已经存在了你的KEY，那么你可以跳过这个步骤了。

#### 签名软件 GnuPG 的安装配置

##### GnuPG

1991年，程序员 Phil Zimmermann 为了避开政府监视，开发了加密软件PGP。这个软件非常好用，迅速流传开来，成了许多程序员的必备工具。但是，它是商业软件，不能自由使用。所以，自由软件基金会决定，开发一个PGP的替代品，取名为GnuPG。这就是GPG的由来。

##### 安装配置

CentOS 安装命令：

```
yum install gnupg
```
安装完成后，默认配置文件 gpg.conf 会放在 home 目录下。

```
~/.gnupg/gpg.conf
```

如果不存在这个目录或文件，可以直接创建一个空文件。

Apache 签名推荐 SHA512， 可以通过配置 gpg 完成。
编辑gpg.conf, 增加下面的三行：

```
personal-digest-preferences SHA512
cert-digest-algo SHA512
default-preference-list SHA512 SHA384 SHA256 SHA224 AES256 AES192 AES CAST5 ZLIB BZIP2 ZIP Uncompressed
```

#### 生成新的签名

##### 准备签名

推荐的生成新签名的设置：

这里必须通过 SecureCRT 等终端直接登录用户账户，不能通过 su - user 或者 ssh 转，否则密码输入 box 会显示不出来而报错。

先看下 gpg 的 version 以及是否支持 SHA512.

```
$ gpg --version
gpg (GnuPG) 2.0.22
libgcrypt 1.5.3
Copyright (C) 2013 Free Software Foundation, Inc.
License GPLv3+: GNU GPL version 3 or later <http://gnu.org/licenses/gpl.html>
This is free software: you are free to change and redistribute it.
There is NO WARRANTY, to the extent permitted by law.

Home: ~/.gnupg
Supported algorithms:
Pubkey: RSA, ?, ?, ELG, DSA
Cipher: IDEA, 3DES, CAST5, BLOWFISH, AES, AES192, AES256, TWOFISH,
        CAMELLIA128, CAMELLIA192, CAMELLIA256
Hash: MD5, SHA1, RIPEMD160, SHA256, SHA384, SHA512, SHA224
Compression: Uncompressed, ZIP, ZLIB, BZIP2
```

##### 生成新的签名

```
$ gpg --gen-key
gpg (GnuPG) 2.0.22; Copyright (C) 2013 Free Software Foundation, Inc.
This is free software: you are free to change and redistribute it.
There is NO WARRANTY, to the extent permitted by law.

Please select what kind of key you want:
   (1) RSA and RSA (default)
   (2) DSA and Elgamal
   (3) DSA (sign only)
   (4) RSA (sign only)
Your selection? 1
RSA keys may be between 1024 and 4096 bits long.
What keysize do you want? (2048) 4096
Requested keysize is 4096 bits
Please specify how long the key should be valid.
         0 = key does not expire
      <n>  = key expires in n days
      <n>w = key expires in n weeks
      <n>m = key expires in n months
      <n>y = key expires in n years
Key is valid for? (0)
Key does not expire at all
Is this correct? (y/N) y

GnuPG needs to construct a user ID to identify your key.

Real name: xxx
Name must be at least 5 characters long
Real name: xxx-yyy
Email address: xxx@apache.org
Comment: xxx's key
You selected this USER-ID:
    "xxx-yyy (xxx's key) <xxx@apache.org>"

Change (N)ame, (C)omment, (E)mail or (O)kay/(Q)uit? o
```

其中 Real name 需保持和 id.apache.org 中显示的 id 一致。
Email address 为 apache 的邮箱。

输入 passphrase, 一共要输入两遍，超过8个字符即可。

**这里的秘钥一定要记住，后面签名的时候会用到。同时也会用于其他组件的发布**

##### 查看和输出

第一行显示公钥文件名（pubring.gpg），第二行显示公钥特征（4096位，Hash字符串和生成时间），第三行显示"用户ID"，注释，邮件，第四行显示私钥特征。

```
$ gpg --list-keys
/home/lide/.gnupg/pubring.gpg
-----------------------------
pub   4096R/33DBF2E0 2018-12-06
uid                  xxx-yyy  (xxx's key) <xxx@apache.org>
sub   4096R/0E8182E6 2018-12-06
```

其中 xxx-yyy 就是用户ID。

```
gpg --armor --output public-key.txt --export [用户ID]
```

```
$ gpg --armor --output public-key.txt --export xxx-yyy
文件‘public-key.txt’已存在。 是否覆盖？(y/N)y
$ cat public-key.txt
-----BEGIN PGP PUBLIC KEY BLOCK-----
Version: GnuPG v2.0.22 (GNU/Linux)

mQINBFwJEQ0BEACwqLluHfjBqD/RWZ4uoYxNYHlIzZvbvxAlwS2mn53BirLIU/G3
9opMWNplvmK+3+gNlRlFpiZ7EvHsF/YJOAP59HmI2Z...
```

#### 上传签名公钥

公钥服务器是网络上专门储存用户公钥的服务器。send-keys 参数可以将公钥上传到服务器。

```
gpg --send-keys xxxx --keyserver https://keyserver.ubuntu.com/

```
其中 xxxx 为上一步 `--list-keys` 结果中 pub 后面的字符串，如上为：33DBF2E0

也可以通过[https://keyserver.ubuntu.com/](https://keyserver.ubuntu.com/)上传上述 public-key.txt 的内容：

上传成功之后，可以通过查询这个[https://keyserver.ubuntu.com/](https://keyserver.ubuntu.com/)，输入 0x33DBF2E0 查询。（注意需要以 0x 开头）

该网站查询有延迟，可能需要等1个小时。

#### 生成 fingerprint 并上传到 apache 用户信息中

由于公钥服务器没有检查机制，任何人都可以用你的名义上传公钥，所以没有办法保证服务器上的公钥的可靠性。通常，你可以在网站上公布一个公钥指纹，让其他人核对下载到的公钥是否为真。

fingerprint参数生成公钥指纹：

```
gpg --fingerprint [用户ID]
```

```
$ gpg --fingerprint xxx-yyy
pub   4096R/33DBF2E0 2018-12-06
      Key fingerprint = 07AA E690 B01D 1A4B 469B  0BEF 5E29 CE39 33DB F2E0
uid                  xxx-yyy (xxx's key) <xxx@apache.org>
sub   4096R/0E8182E6 2018-12-06
```

将上面的 fingerprint （即 07AA E690 B01D 1A4B 469B  0BEF 5E29 CE39 33DB F2E0）粘贴到自己的用户信息中：

https://id.apache.org

`OpenPGP Public Key Primary Fingerprint:`

> 注：每个人可以有多个 Public Key。

#### 生成 keys

**注意不要删除 KEYS 文件中已有的内容，这能追加新增**

```
svn co https://dist.apache.org/repos/dist/dev/incubator/doris/
# edit doris/KEYS file
gpg --list-sigs [用户 ID] >> doris/KEYS
gpg --armor --export [用户 ID] >> doris/KEYS
svn ci --username $ASF_USERNAME --password "$ASF_PASSWORD" -m"Update KEYS"
```

注意，KEYS 文件要同时发布到如下 svn 库。

```
svn co https://dist.apache.org/repos/dist/release/incubator/doris
# edit doris/KEYS file
svn ci --username $ASF_USERNAME --password "$ASF_PASSWORD" -m"Update KEYS"
```

之后会自动同步到：
```
https://downloads.apache.org/incubator/doris/KEYS
```

在后续的发版投票邮件中，要使用 `https://downloads.apache.org/incubator/doris/KEYS` 这里的 KEYS 文件。


## Maven 发版准备

对于 Doris Connector 等组件，需要使用 maven 进行版本发布。

1. 生成主密码

    `mvn --encrypt-master-password <password>`
    
    这个密码仅用作加密后续的其他密码使用, 输出类似 `{VSb+6+76djkH/43...}`
    
    之后创建 `~/.m2/settings-security.xml` 文件，内容是

    ```
    <settingsSecurity>
      <master>{VSb+6+76djkH/43...}</master>
    </settingsSecurity>
    ```

2. 加密 apache 密码

    `mvn --encrypt-password <password>`
    
    这个密码是apache 账号的密码 输出和上面类似`{GRKbCylpwysHfV...}`
    
    在 `~/.m2/settings.xml` 中加入

	```
    <servers>
      <!-- To publish a snapshot of your project -->
      <server>
        <id>apache.snapshots.https</id>
        <username>yangzhg</username>
        <password>{GRKbCylpwysHfV...}</password>
      </server>
      <!-- To stage a release of your project -->
      <server>
        <id>apache.releases.https</id>
        <username>yangzhg</username>
        <password>{GRKbCylpwysHfV...}</password>
      </server>
    </servers>
	```
	
## 在社区发起 DISCUSS

DISCUSS 并不是发版前的必须流程，但强烈建议在重要版本发布前，在 dev@doris 邮件组发起讨论。内容包括但不限于重要功能的说明、Bug修复说明等。