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


```
tools:
    gen_data.py: generate random data
    save_docker.sh: save the current docker state
```

## Run on Apple Silicon macOS

Install Docker Desktop and Bash 4 or newer first. The macOS system Bash
is version 3.2 and cannot run `run-thirdparties-docker.sh`.

```bash
brew install bash
```

Set `CONTAINER_UID` to a unique value containing only letters, numbers,
and hyphens. Then start the local Iceberg stack from the repository
root:

```bash
CONTAINER_UID=dorismac- /opt/homebrew/bin/bash \
    docker/thirdparties/run-thirdparties-docker.sh -c iceberg
```

On macOS, the startup script uses the native ARM64 `postgres:14` image
instead of the amd64-only `postgis/postgis:14-3.3` image. Linux keeps
using the existing PostGIS image. Other images in this stack provide
native ARM64 variants.

The separate `iceberg-rest` stack includes legacy Hadoop images that
only provide amd64 variants. Docker Desktop runs these two images with
amd64 emulation:

```bash
CONTAINER_UID=dorismac- /opt/homebrew/bin/bash \
    docker/thirdparties/run-thirdparties-docker.sh -c iceberg-rest
```
