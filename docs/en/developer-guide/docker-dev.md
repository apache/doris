---
{
    "title": "Doris Docker quick build development environment",
    "language": "en"
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

# Doris Docker quick build development environment

## Related detailed document navigation

- [Developing mirror compilation using Docker](https://doris.incubator.apache.org/installing/compilation.html#developing-mirror-compilation-using-docker-recommended)
- [Deploying Doris](https://doris.incubator.apache.org/installing/install-deploy.html#cluster-deployment)
- [VSCode Be Development Debugging](https://doris.incubator.apache.org/developer-guide/be-vscode-dev.html)

## Environment preparation

- Install Docker
- VSCode
    - Remote plugin

## Build image

create dockerfile

VSCode replace all by using Ctrl-d

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

# safer usage, create new user instead of using root
RUN yum install -y sudo \
        && useradd -ms /bin/bash <!!! your user !!!> && echo <!!! your user password !!!> | passwd <!!! your user !!!> --stdin \
        && usermod -a -G wheel <!!! your user !!!>

USER <!!! your user !!!>
WORKDIR /home/<!!! your user !!!>
RUN git config --global color.ui true \
        && git config --global user.email "<!!! your git email !!!>" \
        && git config --global user.name "<!!! your git username !!!>"

# install zsh and oh my zsh, easier to use on, you can remove it if you don't need it
USER root
RUN yum install -y zsh \
        && chsh -s /bin/zsh <!!! your user !!!>
USER <!!! your user !!!>
RUN wget https://github.com/robbyrussell/oh-my-zsh/raw/master/tools/install.sh -O - | zsh \
        && git clone https://github.com/zsh-users/zsh-autosuggestions ${ZSH_CUSTOM:-~/.oh-my-zsh/custom}/plugins/zsh-autosuggestions \
        && git clone https://github.com/zsh-users/zsh-syntax-highlighting.git ${ZSH_CUSTOM:-~/.oh-my-zsh/custom}/plugins/zsh-syntax-highlighting
```

run build command

```bash
docker build -t doris .
```

run image

note! [problems with mounting](../installing/compilation.md)

> See the link above: It is recommended to run the image by mounting the local Doris source code directory as a volume .....

if you are developing on windows, mounting may cause cross-filesystem access problems, please consider setting it manually

```bash
docker run -it doris:latest /bin/bash
```

if you installed zsh, replace plugins in ~/.zshrc after running the container

```
plugins=(git zsh-autosuggestions zsh-syntax-highlighting)
```

create directory and download doris

```bash
su <your user>
mkdir code && cd code
git clone https://github.com/apache/incubator-doris.git
```

## Compile

Note:

use the following command first time compiling

```bash
sh build.sh --clean --be --fe --ui
```

it is because build-env-for-0.15.0 version image upgraded thrift(0.9 -> 0.13), so you need to use --clean command to force use new version of thrift to generate code files, otherwise it will cause incompatibilities.

compile Doris

```bash
sh build.sh
```

## Run

manually create `meta_dir` metadata storage location, default value is `${DORIS_HOME}/doris-meta`

```bash
mkdir meta_dir
```

launch FE

```bash
cd output/fe
sh bin/start_fe.sh --daemon
```

launch BE

```bash
cd output/be
sh bin/start_be.sh --daemon
```

mysql-client connect

```bash
mysql -h 127.0.0.1 -P 9030 -u root
```
