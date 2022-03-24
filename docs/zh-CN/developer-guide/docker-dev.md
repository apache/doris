---
{
    "title": "Doris Docker 快速搭建开发环境",
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

# Doris Docker 快速搭建开发环境

## 相关详细文档导航

- [使用 Docker 开发镜像编译](https://doris.incubator.apache.org/zh-CN/installing/compilation.html#%E4%BD%BF%E7%94%A8-docker-%E5%BC%80%E5%8F%91%E9%95%9C%E5%83%8F%E7%BC%96%E8%AF%91-%E6%8E%A8%E8%8D%90)
- [部署](https://doris.incubator.apache.org/zh-CN/installing/install-deploy.html#%E9%9B%86%E7%BE%A4%E9%83%A8%E7%BD%B2)
- [VSCode Be 开发调试](https://doris.incubator.apache.org/zh-CN/developer-guide/be-vscode-dev.html)

## 环境准备

- 安装 Docker
- VSCode
    - Remote 插件

## 构建镜像

创建 dockerfile

VSCode 中使用 Ctrl-d 替换掉所有的

- <!!! your user !!!>
- <!!! your user password !!!>
- <!!! root password !!!>
- <!!! your git email !!!>
- <!!! your git username !!!>

```dockerfile
FROM apache/incubator-doris:build-env-latest

USER root
WORKDIR /root
RUN echo '<!!! root password !!!>' | passwd root --stdin

RUN yum install -y vim net-tools man wget git mysql lsof bash-completion \
        && cp /var/local/thirdparty/installed/bin/thrift /usr/bin

# 更安全的使用，创建用户而不是使用 root
RUN yum install -y sudo \
        && useradd -ms /bin/bash <!!! your user !!!> && echo <!!! your user password !!!> | passwd <!!! your user !!!> --stdin \
        && usermod -a -G wheel <!!! your user !!!>

USER <!!! your user !!!>
WORKDIR /home/<!!! your user !!!>
RUN git config --global color.ui true \
        && git config --global user.email "<!!! your git email !!!>" \
        && git config --global user.name "<!!! your git username !!!>"

# 按需安装 zsh and oh my zsh, 更易于使用，不需要的移除
USER root
RUN yum install -y zsh \
        && chsh -s /bin/zsh <!!! your user !!!>
USER <!!! your user !!!>
RUN wget https://github.com/robbyrussell/oh-my-zsh/raw/master/tools/install.sh -O - | zsh \
        && git clone https://github.com/zsh-users/zsh-autosuggestions ${ZSH_CUSTOM:-~/.oh-my-zsh/custom}/plugins/zsh-autosuggestions \
        && git clone https://github.com/zsh-users/zsh-syntax-highlighting.git ${ZSH_CUSTOM:-~/.oh-my-zsh/custom}/plugins/zsh-syntax-highlighting
```

运行构建命令

```bash
docker build -t doris .
```

运行镜像

此处按需注意 [挂载的问题](../installing/compilation.md)

> 见链接中：建议以挂载本地 Doris 源码目录的方式运行镜像 .....

由于如果是使用 windows 开发，挂载会存在跨文件系统访问的问题，请自行斟酌设置

```bash
docker run -it doris:latest /bin/bash
```

如果选择安装了 zsh
运行 容器后，在 ~/.zshrc 替换 plugins 为

```
plugins=(git zsh-autosuggestions zsh-syntax-highlighting)
```

创建目录并下载 doris

```bash
su <your user>
mkdir code && cd code
git clone https://github.com/apache/incubator-doris.git
```

## 编译

注意:

第一次编译的时候要使用如下命令

```bash
sh build.sh --clean --be --fe --ui
```

这是因为 build-env-for-0.15.0 版本镜像升级了 thrift(0.9 -> 0.13)，需要通过 --clean 命令强制使用新版本的 thrift 生成代码文件，否则会出现不兼容的代码。：

编译 Doris

```bash
sh build.sh
```

## 运行

手动创建 `meta_dir` 元数据存放位置, 默认值为 `${DORIS_HOME}/doris-meta`

```bash
mkdir meta_dir
```

启动FE

```bash
cd output/fe
sh bin/start_fe.sh --daemon
```

启动BE

```bash
cd output/be
sh bin/start_be.sh --daemon
```

mysql-client 连接

```bash
mysql -h 127.0.0.1 -P 9030 -u root
```
