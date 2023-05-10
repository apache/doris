---
{
"title": "Beats Doris Output Plugin",
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

# Beats output plugin

This is an output implementation of [elastic beats](https://github.com/elastic/beats) for support [Filebeat](https://github.com/elastic/beats/tree/master/filebeat), [Metricbeat](https://github.com/elastic/beats/tree/master/metricbeat), [Packetbeat](https://github.com/elastic/beats/tree/master/packetbeat), [Winlogbeat](https://github.com/elastic/beats/tree/master/winlogbeat), [Auditbeat](https://github.com/elastic/beats/tree/master/auditbeat), [Heartbeat](https://github.com/elastic/beats/tree/master/heartbeat) to [Apache Doris](https://github.com/apache/doris).

This module is used to output data to Doris for elastic beats, use the HTTP protocol to interact with the Doris FE Http interface, and import data through Doris's stream load.

[Learn more about Doris Stream Load ](../data-operate/import/import-way/stream-load-manual.md)

[Learn more about Doris](/)

## Compatibility

This output is developed and tested using Beats 7.3.1

## Install

### Download source code

```
mkdir -p $GOPATH/src/github.com/apache/
cd $GOPATH/src/github.com/apache/
git clone https://github.com/apache/doris
cd doris/extension/beats
```

### Compile

```
go build -o filebeat filebeat/filebeat.go
go build -o metricbeat metricbeat/metricbeat.go
go build -o winlogbeat winlogbeat/winlogbeat.go
go build -o packetbeat packetbeat/packetbeat.go
go build -o auditbeat auditbeat/auditbeat.go
go build -o heartbeat heartbeat/heartbeat.go
```

You will get executables in various subdirectories

## Usage

In this section, you can use the sample config file in the directory [./example/], or you can create it as follow steps.

### Configure Beat

Add following configuration to `*beat.yml`

```yml
output.doris:
  fenodes: ["http://localhost:8030"] # your doris fe address
  user: root # your doris user
  password: root # your doris password
  database: example_db # your doris database
  table: example_table # your doris table

  codec_format_string: "%{[message]}" # beat-event format expression to row data
  headers:
    column_separator: ","
```

### Start Beat

Using filebeat as an example

```
./filebeat/filebeat -c filebeat.yml -e
```

## Configurations

Connection doris configuration:

| Name           | Description                                                                                                                                 | Default     |
|----------------|---------------------------------------------------------------------------------------------------------------------------------------------|-------------|
| fenodes        | FE's HTTP interactive address eg : ["http://fe1:8030", "http://fe2:8030"]                                                                   |             |
| user           | User name, the user needs to have import permission for the doris table                                                                     |             |
| password       | Password                                                                                                                                    |             |
| database       | Database name                                                                                                                               |             |
| table          | Table name                                                                                                                                  |             |
| label_prefix   | Import the identification prefix, the final generated ID is *{label\_prefix}\_{db}\_{table}\_{time_stamp}*                                  | doris_beats |
| line_delimiter | Used to specify the newline character in the imported data, the default is \n. Combinations of multiple characters can be used as newlines. | \n          |
| headers        | Users can pass in [stream-load import parameters](../data-operate/import/import-way/stream-load-manual.md) through the headers.             |             |

Beats configuration:

| Name                | Description                                                                                                                                                                                               | Default |
|---------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------|
| codec_format_string | Set the [expression](https://www.elastic.co/guide/en/beats/filebeat/7.3/configuration-output-codec.html) of format `beat event`, and the format result will be added into http body as a row data         |         |
| codec               | Beats [output codec](https://www.elastic.co/guide/en/beats/filebeat/7.3/configuration-output-codec.html) and the format result will be added to http body as a row, Priority to use `codec_format_string` |         |
| timeout             | Set the http client connection timeout                                                                                                                                                                    |         |
| bulk_max_size       | The maximum number of events processed per batch                                                                                                                                                          | 100000  |
| max_retries         | Filebeat ignores the max_retries setting and retries indefinitely.                                                                                                                                        | 3       |
| backoff.init        | The number of seconds to wait before trying to reconnect after a network error.                                                                                                                           | 1       |
| backoff.max         | The maximum number of seconds to wait before attempting to connect after a network error.                                                                                                                 | 60      |

## Complete usage example of filebeat

### Init Doris

```sql
CREATE DATABASE example_db;

CREATE TABLE example_db.example_table (
    id BIGINT,
    name VARCHAR(100)
)
UNIQUE KEY(`id`)
DISTRIBUTED BY HASH(`id`) BUCKETS 1
PROPERTIES (
    "replication_num"="1"
);
```

### Configure Filebeat

Create `/tmp/beats/filebeat.yml` file and add following configuration:

```yml
filebeat.inputs:
  - type: log
    enabled: true
    paths:
      - /tmp/beats/example.log

output.doris:
  fenodes: ["http://localhost:8030"] # your doris fe address
  user: root # your doris user
  password: root # your doris password
  database: example_db # your doris database
  table: example_table # your doris table

  codec_format_string: "%{[message]}"
  headers:
    column_separator: ","
```

### Start Filebeat

```
./filebeat/filebeat -c /tmp/beats/filebeat.yml -e
```

### Validate Load Data

Add write data to `/tmp/beats/example.log`

```shell
echo -e "1,A\n2,B\n3,C\n4,D" >> /tmp/beats/example.log
```

Observe the filebeat log. If the error log is not printed, the import was successful. At this time, you can view the imported data in the `example_db.example_table` table

## More configure examples

### Specify columns

Make `/tmp/beats/example.log` and add following content:

```csv
1,A
2,B
```

Configure `columns`

```yml
filebeat.inputs:
  - type: log
    enabled: true
    paths:
      - /tmp/beats/example.log

output.doris:
  ...

  codec_format_string: "%{[message]}"
  headers:
    columns: "id,name"
```

### Collect json file

Make `/tmp/beats/example.json` and add following content:

```json
{"id":  1, "name": "A"}
{"id":  2, "name": "B"}
```

Configure `headers`

```yml
filebeat.inputs:
  - type: log
    enabled: true
    paths:
      - /tmp/beats/example.json

output.doris:
  ...

  codec_format_string: "%{[message]}"
  headers:
    format: json
    read_json_by_line: true
```

### Codec output fields

Make `/tmp/beats/example.log` and add following content:

```csv
1,A
2,B
```

Configure `codec_format_string`

```yml
filebeat.inputs:
  - type: log
    enabled: true
    paths:
      - /tmp/beats/example.log

output.doris:
  ...

  codec_format_string: "%{[message]},%{[@timestamp]},%{[@metadata.type]}"
  headers:
    columns: "id,name,beat_timestamp,beat_metadata_type"
```

## FAQ

### How to config batch commit size

Add following configuration to your `beat.yml`

This sample configuration forwards events to the doris  if 10000 events are available or the oldest available event has been waiting for 5s in the [mem queue](https://www.elastic.co/guide/en/beats/filebeat/7.3/configuring-internal-queue.html#configuration-internal-queue-memory):

```yml
queue.mem:
  events: 10000
  flush.min_events: 10000
  flush.timeout: 5s
```

### How to use other beats(e.g metricbeat)

Doris beats support all beats modules, see the [Install](#Install) and [Usage](#Usage)

### How to build docker image

You can package a docker image with an executable file of [Install](#Install) outputs
