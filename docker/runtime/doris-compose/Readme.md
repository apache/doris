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

# Doris compose

Use doris compose to create doris docker compose clusters.

## Requirements

### 1. Make sure you have docker permissions

 run:

```shell
docker run hello-world
```

if have problem with permission denied, then [add-docker-permission](https://docs.docker.com/engine/install/linux-postinstall/).

Make sure BuildKit configured in the machine. if not follow [docker-with-BuildKit](https://docs.docker.com/build/buildkit/).

### 2. The doris image should contains

```shell
/opt/apache-doris/{fe, be, cloud}
```

If don't create cloud cluster, the image no need to contains the cloud pkg.

If build doris use `sh build.sh --fe --be --cloud` **without do any change on their origin conf or shells**, then its `output/` satisfy with all above, then run command in doris root directory will generate such a image. If you want to pack a product that is not the `output/` directory, you can modify `Dockerfile` by yourself.

```shell
docker build -f docker/runtime/doris-compose/Dockerfile -t <image> .
```

The `<image>` is the name you want the docker image to have.

### 3. Install the dependent python library in 'docker/runtime/doris-compose/requirements.txt'

`PyYAML` of certain version not always fit other libraries' requirements. So we suggest to use a individual environment using `venv` or `conda`.

```shell
python -m pip install --user -r docker/runtime/doris-compose/requirements.txt
```

if it failed, change content of `requirements.txt` to:

```Dockerfile
pyyaml==5.3.1
docker==6.1.3
......
```

## Usage

### Notice

Each cluster will have a directory in '/tmp/doris/{cluster-name}', user can set env `LOCAL_DORIS_PATH` to change its directory.

For example, if user export `LOCAL_DORIS_PATH=/mydoris`, then the cluster's directory is '/mydoris/{cluster-name}'.

And cluster's directory will contains all its containers's logs and data, like `fe-1`, `fe-2`, `be-1`, ..., etc.

If there are multiple users run doris-compose on the same machine, suggest don't change `LOCAL_DORIS_PATH` or they should export the same `LOCAL_DORIS_PATH`.

Because when create a new cluster, doris-compose will search the local doris path, and choose a docker network which is different with this path's clusters.

So if multiple users use different `LOCAL_DORIS_PATH`, their clusters may have docker network conflict!!!

### Create a cluster or recreate its containers

```shell
python docker/runtime/doris-compose/doris-compose.py up  <cluster-name>   <image?> 
    --add-fe-num  <add-fe-num>  --add-be-num <add-be-num>
    [--fe-id <fd-id> --be-id <be-id>]
    ...
    [ --cloud ]
```

if it's a new cluster, must specific the image.

add fe/be nodes with the specific image, or update existing nodes with `--fe-id`, `--be-id`

For create a cloud cluster, steps are as below:

1. Write cloud s3 store config file, its default path is '/tmp/doris/cloud.ini'.
   It's defined in environment variable `DORIS_CLOUD_CFG_FILE`, user can change this env var to change its path.
   A Example file is locate in 'docker/runtime/doris-compose/resource/cloud.ini.example'.

2. Use doris compose up command with option `--cloud` to create a new cloud cluster.

The simplest way to create a cloud cluster:

```shell
python docker/runtime/doris-compose/doris-compose.py up  <cluster-name>  <image>  --cloud
```

It will create 1 fdb, 1 meta service server, 1 recycler, 3 fe and 3 be.

### Remove node from the cluster

```shell
python docker/runtime/doris-compose/doris-compose.py down  <cluster-name> --fe-id <fe-id>  --be-id<be-id> [--clean]  [--drop-force]
```

Down the containers and remove it from the DB.

For BE, if specific drop force, it will send dropp sql to FE, otherwise it will send decommission sql to FE.

If specific `--clean`, it will delete its data too.

### Start, stop, restart specific nodes

```shell
python docker/runtime/doris-compose/doris-compose.py start  <cluster-name>  --fe-id  <multiple fe ids>  --be-id <multiple be ids>
python docker/runtime/doris-compose/doris-compose.py restart  <cluster-name>  --fe-id  <multiple fe ids>  --be-id <multiple be ids>
```

### List doris cluster

```shell
python docker/runtime/doris-compose/doris-compose.py ls <multiple cluster names>
```

if specific cluster names, it will list all the cluster's nodes.

Otherwise it will just list summary of each clusters.

There are more options about doris-compose. Just try

```shell
python docker/runtime/doris-compose/doris-compose.py <command> -h 
```

### Generate regression custom conf file

```shell
python docker/runtime/doris-compose/doris-compose.py config <cluster-name>  <doris-root-path>  [-q]  [--connect-follow-fe]
```

Generate regression-conf-custom.groovy to connect to the specific docker cluster.

### Setup cloud multi clusters test env

steps:

1. Create a new cluster:  `python docker/runtime/doris-compose/doris-compose.py up my-cluster  my-image  --add-fe-num 2  --add-be-num 4 --cloud`
2. Generate regression-conf-custom.groovy: `python docker/runtime/doris-compose/doris-compose.py config my-cluster  <doris-root-path> --connect-follow-fe`
3. Run regression test: `bash run-regression-test.sh --run -times 1 -parallel 1 -suiteParallel 1 -d cloud/multi_cluster`

## Problem investigation

### Log

Each cluster has logs in Docker in '/tmp/doris/{cluster-name}/{node-xxx}/log/'. For each node, doris compose will also print log in '/tmp/doris/{cluster-name}/{node-xxx}/log/health.out'

### Up cluster using non-detach mode

```shell
python docker/runtime/doris-compose/doris-compose.py up ...   -no-detach
```

## Developer

Before submitting code, pls format code.

```shell
bash format-code.sh
```
