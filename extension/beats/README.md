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

# Beats output plugin

## Compatibility

This output is developed and tested using Beats 7.3.1

## How to build

```
go build -o filebeat filebeat/filebeat.go
go build -o metricbeat metricbeat/metricbeat.go
go build -o winlogbeat winlogbeat/winlogbeat.go
go build -o packetbeat packetbeat/packetbeat.go
go build -o auditbeat auditbeat/auditbeat.go
go build -o heartbeat heartbeat/heartbeat.go
```

## How to use

See:

- https://doris.apache.org/zh-CN/docs/dev/ecosystem/beats
- https://doris.apache.org/docs/dev/ecosystem/beats
