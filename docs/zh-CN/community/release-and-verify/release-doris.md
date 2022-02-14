---
{
    "title": "发布 Doris 主代码",
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

# 发布 Doris 主代码

Apache 的发布必须至少是 IPMC 成员，拥有 apache 邮箱的committer，这个角色叫做 release manager。

本文档主要介绍 Doris 主代码的发布流程，即 [apache/incubator-doris](https://github.com/apache/incubator-doris) 代码库的版本发布。

发布的大致流程如下：

1. 环境准备
2. 发布准备
	1. 社区发起 DISCUSS 并与社区交流具体发布计划
	2. 创建分支用于发布
	3. 清理 issue
	4. 将必要的 Patch 合并到发布的分支
3. 验证分支
	1. QA 稳定性测试
	2. 验证编译镜像正确性
	3. 准备 Release Nodes
4. 社区发布投票流程
	1. 将 tag 打包，签名并上传到[Apache Dev svn 仓库](https://dist.apache.org/repos/dist/dev/incubator/doris)
	2. 在 [Doris 社区](dev@doris.apache.org)发起投票
	3. 投票通过后，在Doris社区发 Result 邮件
	4. 在 [Incubator 社区](general@incubator.apache.org) 发起新一轮投票
	5. 发 Result 邮件到 general@incubator.apache.org
5. 完成工作
	1. 上传签名的软件包到 [Apache release repo](https://dist.apache.org/repos/dist/release/incubator/doris)，并生成相关链接
	2. 在 Doris 官网和 github 发布下载链接，并且清理 svn 上的旧版本包
	3. 发送 Announce 邮件到 general@incubator.apache.org

## 准备环境

如果这是你第一次发布，那么你需要在你的环境中准备如下工具

1. release signing https://www.apache.org/dev/release-signing.html
2. gpg https://www.apache.org/dev/openpgp.html
3. svn https://www.apache.org/dev/openpgp.html

### 准备gpg key

Release manager 在发布前需要先生成自己的签名公钥，并上传到公钥服务器，之后就可以用这个公钥对准备发布的软件包进行签名。
如果在[KEY](https://dist.apache.org/repos/dist/dev/incubator/doris/KEYS)里已经存在了你的KEY，那么你可以跳过这个步骤了。

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

也可以通过[网站](https://keyserver.ubuntu.com/)上传上述 public-key.txt 的内容：

上传成功之后，可以通过查询这个[网站](https://keyserver.ubuntu.com/)，输入 0x33DBF2E0 查询。（注意需要以 0x 开头）

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

```
svn co https://dist.apache.org/repos/dist/dev/incubator/doris/
# edit doris/KEYS file
gpg --list-sigs [用户 ID] >> doris/KEYS
gpg --armor --export [用户 ID] >> doris/KEYS
svn ci --username $ASF_USERNAME --password "$ASF_PASSWORD" -m"Update KEYS"
```

注意，KEYS 文件要同时发布到如下 svn 库：

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

## 准备发布

### 在社区发起 DISCUSS

如果觉得已经修复了很多bug，开发了比较重要的 feature，任何 IPMC 成员都可以发起 DISCUSS 讨论发布新版本。
可以发起一个标题为 [DISCUSS] x.y.z release 的邮件，在社区内部进行讨论，说明已经修复了哪些bug，开发了哪些 features。
如果 DISCUSS 邮件得到大家支持就可以进行下一步。

### 准备分支

发布前需要先新建一个分支。例如：

```
$ git checkout -b branch-0.9

```

这个分支要进行比较充分的测试，使得功能可用，bug收敛，重要bug都得到修复。
这个过程需要等待社区，看看是否有必要的patch需要在这个版本合入，如果有，需要把它 cherry-pick 到发布分支。

### 清理issue

将属于这个版本的所有 issue 都过一遍，关闭已经完成的，如果没法完成的，推迟到更晚的版本。

### 合并必要的Patch

在发布等待过程中，可能会有比较重要的Patch合入，如果社区有人说要有重要的Bug需要合入，那么 Release Manager 需要评估并将重要的Patch合入到发布分支中。

## 验证分支

### QA 稳定性测试

将打好的分支交给 QA 同学进行稳定性测试。如果在测试过程中，出现需要修复的问题，则如果在测试过程中，出现需要修复的问题，待修复好后，需要将修复问题的 PR 合入到待发版本的分支中。

待整个分支稳定后，才能准备发版本。

### 验证编译镜像正确性

1. 下载编译镜像

	```
	docker pull apache/incubator-doris:build-env-1.3.1
	```

2. 使用官方文档编译新分支，编译方式见[Docker 开发镜像编译](http://doris.apache.org/master/zh-CN/installing/compilation.html)

	进入镜像后，编译可能需要大概3~4小时左右，请耐心等待。	

	如果编译中缺少某些三方库导致编译失败，则说明编译镜像需要更新。

3. 重新打编译镜像

### 准备 Release Nodes

## 社区发布投票流程

### 打 tag

当上述分支已经比较稳定后，就可以在此分支上打 tag。
记得在创建 tag 时，修改 `gensrc/script/gen_build_version.sh` 中的 `build_version` 变量。如 `build_version="0.10.0-release"`

例如：

```
$ git checkout branch-0.9
$ git tag -a 0.9.0-rc01 -m "0.9.0 release candidate 01"
$ git push origin 0.9.0-rc01
Counting objects: 1, done.
Writing objects: 100% (1/1), 165 bytes | 0 bytes/s, done.
Total 1 (delta 0), reused 0 (delta 0)
To git@github.com:apache/incubator-doris.git
 * [new tag]         0.9.0-rc01 -> 0.9.0-rc01

$ git tag
```

### 打包、签名上传

如下步骤，也需要通过 SecureCRT 等终端直接登录用户账户，不能通过 su - user 或者 ssh 转，否则密码输入 box 会显示不出来而报错。

```
$ git checkout 0.9.0-rc01

$ git archive --format=tar 0.9.0-rc01 --prefix=apache-doris-0.9.0-incubating-src/ | gzip > apache-doris-0.9.0-incubating-src.tar.gz

$ gpg -u xxx@apache.org --armor --output apache-doris-0.9.0-incubating-src.tar.gz.asc --detach-sign apache-doris-0.9.0-incubating-src.tar.gz

$ gpg --verify apache-doris-0.9.0-incubating-src.tar.gz.asc apache-doris-0.9.0-incubating-src.tar.gz

$ sha512sum apache-doris-0.9.0-incubating-src.tar.gz > apache-doris-0.9.0-incubating-src.tar.gz.sha512

$ sha512sum --check apache-doris-0.9.0-incubating-src.tar.gz.sha512
```

然后将打包的内容上传到svn仓库中，首先下载 svn 库：

```
svn co https://dist.apache.org/repos/dist/dev/incubator/doris/
```

将之前得到的全部文件组织成以下svn路径

```
./doris/
|-- 0.11.0-rc1
|   |-- apache-doris-0.11.0-incubating-src.tar.gz
|   |-- apache-doris-0.11.0-incubating-src.tar.gz.asc
|   `-- apache-doris-0.11.0-incubating-src.tar.gz.sha512
`-- KEYS
```

上传这些文件

```
svn add 0.11.0-rc1
svn commit -m "Add 0.11.0-rc1"
```

### 发邮件到社区 dev@doris.apache.org 进行投票

[VOTE] Release Apache Doris 0.9.0-incubating-rc01

```
Hi all,

Please review and vote on Apache Doris 0.9.0-incubating-rc01 release.

The release candidate has been tagged in GitHub as 0.9.0-rc01, available
here:
https://github.com/apache/incubator-doris/releases/tag/0.9.0-rc01

Release Notes are here:
https://github.com/apache/incubator-doris/issues/1891

Thanks to everyone who has contributed to this release.

The artifacts (source, signature and checksum) corresponding to this release
candidate can be found here:
https://dist.apache.org/repos/dist/dev/incubator/doris/0.9/0.9.0-rc1/

This has been signed with PGP key 33DBF2E0, corresponding to
lide@apache.org.
KEYS file is available here:
https://downloads.apache.org/incubator/doris/KEYS
It is also listed here:
https://people.apache.org/keys/committer/lide.asc

To verify and build, you can refer to following wiki:
https://github.com/apache/incubator-doris/wiki/How-to-verify-Apache-Release
https://wiki.apache.org/incubator/IncubatorReleaseChecklist

The vote will be open for at least 72 hours.
[ ] +1 Approve the release
[ ] +0 No opinion
[ ] -1 Do not release this package because ...

Best Regards,
xxx

----
DISCLAIMER-WIP: 
Apache Doris is an effort undergoing incubation at The Apache Software Foundation (ASF), 
sponsored by the Apache Incubator. Incubation is required of all newly accepted projects 
until a further review indicates that the infrastructure, communications, and decision 
making process have stabilized in a manner consistent with other successful ASF projects. 
While incubation status is not necessarily a reflection of the completeness or stability 
of the code, it does indicate that the project has yet to be fully endorsed by the ASF.

Some of the incubating project’s releases may not be fully compliant with ASF policy. For 
example, releases may have incomplete or un-reviewed licensing conditions. What follows is 
a list of known issues the project is currently aware of (note that this list, by definition, 
is likely to be incomplete): 

 * Releases may have incomplete licensing conditions

If you are planning to incorporate this work into your product/project, please be aware that
you will need to conduct a thorough licensing review to determine the overall implications of 
including this work. For the current status of this project through the Apache Incubator 
visit: https://incubator.apache.org/projects/doris.html
```

### 投票通过后，发 Result 邮件

[Result][VOTE] Release Apache Doris 0.9.0-incubating-rc01

```
Thanks to everyone, and this vote is now closed.

It has passed with 4 +1 (binding) votes and no 0 or -1 votes.

Binding:
+1 Zhao Chun
+1 xxx
+1 Li Chaoyong
+1 Mingyu Chen

Best Regards,
xxx

```

### 发邮件到 general@incubator.apache.org 进行投票

[VOTE] Release Apache Doris 0.9.0-incubating-rc01

```
Hi all,

Please review and vote on Apache Doris 0.9.0-incubating-rc01 release.

Apache Doris is an MPP-based interactive SQL data warehousing for reporting and analysis.

The Apache Doris community has voted on and approved this release:
https://lists.apache.org/thread.html/d70f7c8a8ae448bf6680a15914646005c6483564464cfa15f4ddc2fc@%3Cdev.doris.apache.org%3E

The vote result email thread:
https://lists.apache.org/thread.html/64d229f0ba15d66adc83306bc8d7b7ccd5910ecb7e842718ce6a61da@%3Cdev.doris.apache.org%3E

The release candidate has been tagged in GitHub as 0.9.0-rc01, available here:
https://github.com/apache/incubator-doris/releases/tag/0.9.0-rc01

There is no CHANGE LOG file because this is the first release of Apache Doris.
Thanks to everyone who has contributed to this release, and there is a simple release notes can be found here:
https://github.com/apache/incubator-doris/issues/406

The artifacts (source, signature and checksum) corresponding to this release candidate can be found here:
https://dist.apache.org/repos/dist/dev/incubator/doris/0.9/0.9.0-rc01/

This has been signed with PGP key 33DBF2E0, corresponding to lide@apache.org.
KEYS file is available here:
https://downloads.apache.org/incubator/doris/KEYS
It is also listed here:
https://people.apache.org/keys/committer/lide.asc

The vote will be open for at least 72 hours.
[ ] +1 Approve the release
[ ] +0 No opinion
[ ] -1 Do not release this package because ...

To verify and build, you can refer to following instruction:

Firstly, you must be install and start docker service, and then you could build Doris as following steps:

Step1: Pull the docker image with Doris building environment
$ docker pull apache/incubator-doris:build-env-1.3.1
You can check it by listing images, its size is about 3.28GB.

Step2: Run the Docker image
You can run image directly:
$ docker run -it apache/incubator-doris:build-env-1.3.1

Step3: Download Doris source
Now you should in docker environment, and you can download Doris source package.
(If you have downloaded source and it is not in image, you can map its path to image in Step2.)
$ wget https://dist.apache.org/repos/dist/dev/incubator/doris/0.9/0.9.0-rc01/apache-doris-0.9.0.rc01-incubating-src.tar.gz

Step4: Build Doris
Now you can decompress and enter Doris source path and build Doris.
$ tar zxvf apache-doris-0.9.0.rc01-incubating-src.tar.gz
$ cd apache-doris-0.9.0.rc01-incubating-src
$ sh build.sh

Best Regards,
xxx

----
DISCLAIMER-WIP: 
Apache Doris is an effort undergoing incubation at The Apache Software Foundation (ASF), 
sponsored by the Apache Incubator. Incubation is required of all newly accepted projects 
until a further review indicates that the infrastructure, communications, and decision 
making process have stabilized in a manner consistent with other successful ASF projects. 
While incubation status is not necessarily a reflection of the completeness or stability 
of the code, it does indicate that the project has yet to be fully endorsed by the ASF.

Some of the incubating project’s releases may not be fully compliant with ASF policy. For 
example, releases may have incomplete or un-reviewed licensing conditions. What follows is 
a list of known issues the project is currently aware of (note that this list, by definition, 
is likely to be incomplete): 

 * Releases may have incomplete licensing conditions

If you are planning to incorporate this work into your product/project, please be aware that
you will need to conduct a thorough licensing review to determine the overall implications of 
including this work. For the current status of this project through the Apache Incubator 
visit: https://incubator.apache.org/projects/doris.html
```

邮件的 thread 连接可以在这里找到：

`https://lists.apache.org/list.html?dev@doris.apache.org`


### 发 Result 邮件到 general@incubator.apache.org

[RESULT][VOTE] Release Apache Doris 0.9.0-incubating-rc01


```
Hi,

Thanks to everyone, and the vote for releasing Apache Doris 0.9.0-incubating-rc01 is now closed.

It has passed with 4 +1 (binding) votes and no 0 or -1 votes.

Binding:
+1 Willem Jiang
+1 Justin Mclean
+1 ShaoFeng Shi
+1 Makoto Yui

The vote thread:
https://lists.apache.org/thread.html/da05fdd8d84e35de527f27200b5690d7811a1e97d419d1ea66562130@%3Cgeneral.incubator.apache.org%3E

Best Regards,
xxx
```

## 完成发布流程

### 上传 package 到 release

当正式发布投票成功后，先发[Result]邮件，然后就准备 release package。
将之前在dev下发布的对应rc文件夹下的源码包、签名文件和hash文件拷贝到另一个目录 0.9.0-incubating，注意文件名字中不要rcxx (可以rename，但不要重新计算签名，hash可以重新计算，结果不会变)

第一次发布的话 KEYS 文件也需要拷贝过来。然后add到svn release 下。

```
add 成功后就可以在下面网址上看到你发布的文件
https://dist.apache.org/repos/dist/release/incubator/doris/0.xx.0-incubating/

稍等一段时间后，能在 apache 官网看到：
http://www.apache.org/dist/incubator/doris/0.9.0-incubating/

```

### 在 Doris 官网和 github 发布链接

#### 创建下载链接

下载链接：
http://www.apache.org/dyn/closer.cgi?filename=incubator/doris/0.9.0-incubating/apache-doris-0.9.0-incubating-src.tar.gz&action=download

wget --trust-server-names "https://www.apache.org/dyn/mirrors/mirrors.cgi?action=download&filename=incubator/doris/0.9.0-incubating/apache-doris-0.9.0-incubating-src.tar.gz"

原始位置:
https://www.apache.org/dist/incubator/doris/0.9.0-incubating/

http://www.apache.org/dyn/closer.cgi/incubator/doris/0.9.0-incubating/apache-doris-0.9.0-incubating-src.tar.gz

源码包（source package）:
http://www.apache.org/dyn/closer.cgi/incubator/doris/0.9.0-incubating/apache-doris-0.9.0-incubating-src.tar.gz

ASC:
http://archive.apache.org/dist/incubator/doris/0.9.0-incubating/apache-doris-0.9.0-incubating-src.tar.gz.asc

sha512:
http://archive.apache.org/dist/incubator/doris/0.9.0-incubating/apache-doris-0.9.0-incubating-src.tar.gz.sha512

KEYS:
http://archive.apache.org/dist/incubator/doris/KEYS

refer to: <http://www.apache.org/dev/release-download-pages#closer>

#### 准备 release note

需要修改如下两个地方：

1、Github 的 release 页面

```
https://github.com/apache/incubator-doris/releases/tag/0.9.0-rc01
```

2、Doris 官网下载页面

下载页面是一个 markdown 文件，地址如下。
```
docs/zh-CN/downloads/downloads.md
docs/en/downloads/downloads.md
```
1. 需要将上一次发布版本的下载包地址改为 apache 的归档地址（见后）。
2. 增加新版本的下载信息。

#### svn 上清理旧版本的包

1. svn 上删除旧版本的包

由于 svn 只需要保存最新版本的包，所以当有新版本发布的时候，旧版本的包就应该从 svn 上清理。

```
https://dist.apache.org/repos/dist/release/incubator/doris/
https://dist.apache.org/repos/dist/dev/incubator/doris/
```
保持这两个地址中，只有最新版本的包即可。

2. 将 Doris 官网的下载页面中，旧版本包的下载地址改为归档页面的地址 

```
下载页面: http://doris.apache.org/downloads.html
归档页面: http://archive.apache.org/dist/incubator/doris
```

Apache 会有同步机制去将历史的发布版本进行一个归档，具体操作见：[how to archive](https://www.apache.org/legal/release-policy.html#how-to-archive)
所以即使旧的包从 svn 上清除，还是可以在归档页面中找到。

### 发 Announce 邮件到 general@incubator.apache.org

Title:

```
[ANNOUNCE] Apache Doris (incubating) 0.9.0 Release
```

发送邮件组：

```
general@incubator.apache.org <general@incubator.apache.org>
dev@doris.apache.org <dev@doris.apache.org>
```

邮件正文：

```
Hi All,

We are pleased to announce the release of Apache Doris 0.9.0-incubating.

Apache Doris (incubating) is an MPP-based interactive SQL data warehousing for reporting and analysis.

The release is available at:
http://doris.apache.org/master/zh-CN/downloads/downloads.html

Thanks to everyone who has contributed to this release, and the release note can be found here:
https://github.com/apache/incubator-doris/releases

Best Regards,

On behalf of the Doris team,
xxx

----
DISCLAIMER-WIP: 
Apache Doris is an effort undergoing incubation at The Apache Software Foundation (ASF), 
sponsored by the Apache Incubator. Incubation is required of all newly accepted projects 
until a further review indicates that the infrastructure, communications, and decision 
making process have stabilized in a manner consistent with other successful ASF projects. 
While incubation status is not necessarily a reflection of the completeness or stability 
of the code, it does indicate that the project has yet to be fully endorsed by the ASF.

Some of the incubating project’s releases may not be fully compliant with ASF policy. For 
example, releases may have incomplete or un-reviewed licensing conditions. What follows is 
a list of known issues the project is currently aware of (note that this list, by definition, 
is likely to be incomplete): 

 * Releases may have incomplete licensing conditions

If you are planning to incorporate this work into your product/project, please be aware that
you will need to conduct a thorough licensing review to determine the overall implications of 
including this work. For the current status of this project through the Apache Incubator 
visit: https://incubator.apache.org/projects/doris.html
```



