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
docker build -f docker/runtime/doris-compose/Dockerfile -t <image> .
```

will generate a image.

2. Install the dependent python library in 'docker/runtime/doris-compose/requirements.txt'


```
python -m pip install --user -r docker/runtime/doris-compose/requirements.txt
```

## Usage

### Create a cluster or recreate its containers

```
python docker/runtime/doris-compose/doris-compose.py up  <cluster-name>   <image?> 
    --add-fe-num  <add-fe-num>  --add-be-num <add-be-num>
    --fe-id <fd-id> --be-id <be-id>

```

if it's a new cluster, must specific the image.

add fe/be nodes with the specific image, or update existing nodes with `--fe-id`, `--be-id`

### Remove node from the cluster

```
python docker/runtime/doris-compose/doris-compose.py down  <cluster-name> --fe-id <fe-id>  --be-id<be-id> [--clean]  [--drop-force]
```

Down the containers and remove it from the DB.

For BE, if specific drop force, it will send dropp sql to FE, otherwise it will send decommission sql to FE.

If specific `--clean`, it will delete its data too.


### Start, stop, restart specific nodes


```
python docker/runtime/doris-compose/doris-compose.py start  <cluster-name>  --fe-id  <multiple fe ids>  --be-id <multiple be ids>
python docker/runtime/doris-compose/doris-compose.py restart  <cluster-name>  --fe-id  <multiple fe ids>  --be-id <multiple be ids>
```

### List doris cluster

```
python docker/runtime/doris-compose/doris-compose.py ls <multiple cluster names>
```

if specific cluster names, it will list all the cluster's nodes.

Otherwise it will just list summary of each clusters.

There are more options about doris-compose. Just try 

```
python docker/runtime/doris-compose/doris-compose.py  <command> -h 
```



