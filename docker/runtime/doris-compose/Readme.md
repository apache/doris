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

1. The doris image should contains:

```
/opt/apache-doris/{fe, be}
```

if build doris use `sh build.sh`, then its output satisfy with this, then run command in doris root

```
docker build -f docker/runtime/doris-compose/Dockerfile  .
```

will generate a image.

2. Install the dependent python library in 'docker/runtime/doris-compose/requirements.txt'


```
python -m pip install --user -r docker/runtime/doris-compose/requirements.txt
```

## Usage

### Create a cluster or recreate its containers

```
python docker/runtime/doris-compose/doris-compose.py up  <cluster-name>   <image>   --fe  <fe-num>  --be <be-num>
```

if it's a new cluster, must specific the image.

### Down a cluster

```
python docker/runtime/doris-compose/doris-compose.py down  <cluster-name> [--clean]
```

Shutdown the whold cluster. If specific `--clean`, it will delete its data too.

### Add new fe or be node

```
python docker/runtime/doris-compose/doris-compose.py add  <cluster-name> --fe <add-fe-num>  --be <add-be-num>
```

### Start or restart specific nodes


```
python docker/runtime/doris-compose/doris-compose.py start  <cluster-name>  --fe  <multiple fe ids>  --be <multiple be ids>
python docker/runtime/doris-compose/doris-compose.py restart  <cluster-name>  --fe  <multiple fe ids>  --be <multiple be ids>
```

### Stop specific nodes

```
python docker/runtime/doris-compose/doris-compose.py stop  <cluster-name>  --fe  <multiple fe ids>  --be <multiple be ids>
```

if specific `--drop`, it will send drop force command to FE.

if specific `--decommission`, for be it will send decommission command to FE, for fe, it willl send drop command to FE.

if specific `--clean`, it will clean node's all files, including doris data, configurations, and logs.

### List doris cluster

```
python docker/runtime/doris-compose/doris-compose.py ls <-a>  <multiple cluster names>
```

if specific cluster names, it will list all the cluster's nodes.

Otherwise it will just list summary of each clusters. If not specific -a, it will list only active clusters. 

If specific `-a`, it will list the unactive clusters too.

There are more options about doris-compose. Just try 

```
python docker/runtime/doris-compose/doris-compose.py  <command> -h 
```



