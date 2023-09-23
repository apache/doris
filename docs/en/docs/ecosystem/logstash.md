---
{
    "title": "Logstash Doris Output Plugin",
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

# Doris Output Plugin

This plugin is used to output data to Doris for logstash, use the HTTP protocol to interact with the Doris FE Http interface, and import data through Doris's stream load.

[Learn more about Doris Stream Load ](../data-operate/import/import-way/stream-load-manual.md)

[Learn more about Doris](/)


## Install and compile
### 1.Download source code

### 2.compile ##
Execute under extension/logstash/ directory

`gem build logstash-output-doris.gemspec`

You will get logstash-output-doris-{version}.gem file in the same directory

### 3.Plug-in installation
copy logstash-output-doris-{version}.gem to the logstash installation directory

Executing an order

`./bin/logstash-plugin install logstash-output-doris-{version}.gem` 

Install logstash-output-doris plugin

## Configuration
### Example:

Create a new configuration file in the config directory and name it logstash-doris.conf

The specific configuration is as follows:

    output {
        doris {
            http_hosts => [ "http://fehost:8030" ]
            user => user_name
            password => password
            db => "db_name"
            table => "table_name"
            label_prefix => "label_prefix"
            column_separator => ","
        }
    }

Configuration instructions:

Connection configuration:

Configuration | Explanation
--- | ---
`http_hosts` | FE's HTTP interactive address eg : ["http://fe1:8030", "http://fe2:8030"]
`user` | User name, the user needs to have import permission for the doris table
`password` | Password
`db` | Database name
`table` | Table name
`label_prefix` | Import the identification prefix, the final generated ID is *{label\_prefix}\_{db}\_{table}\_{time_stamp}*


Load configuration:([Reference documents](../data-operate/import/import-way/stream-load-manual.md)

Configuration | Explanation
--- | ---
`column_separator` | Column separator, the default is \t
`columns` | Used to specify the correspondence between the columns in the import file and the columns in the table
`where` | The filter conditions specified by the import task
`max_filter_ratio` | The maximum tolerance rate of the import task, the default is zero tolerance
`partition` | Partition information of the table to be imported
`timeout` | timeout, the default is 600s
`strict_mode` | Strict mode, the default is false
`timezone` | Specify the time zone used for this import, the default is the East Eight District
`exec_mem_limit` | Import memory limit, default is 2GB, unit is byte

Other configuration:

Configuration | Explanation
--- | ---
`save_on_failure` | If the import fails to save locally, the default is true
`save_dir` | Local save directory, default is /tmp
`automatic_retries` | The maximum number of retries on failure, the default is 3
`batch_size` | The maximum number of events processed per batch, the default is 100000
`idle_flush_time` | Maximum interval, the default is 20 (seconds)


## Start Up
Run the command to start the doris output plugin:

`{logstash-home}/bin/logstash -f {logstash-home}/config/logstash-doris.conf --config.reload.automatic`




## Complete usage example
### 1. Compile doris-output-plugin
1> Download the ruby compressed package and go to [ruby official website](https://www.ruby-lang.org/en/downloads/) to download it. The version 2.7.1 used here

2> Compile and install, configure ruby environment variables

3> Go to the doris source extension/logstash/ directory and execute

`gem build logstash-output-doris.gemspec`

Get the file logstash-output-doris-0.1.0.gem, and the compilation is complete

### 2. Install and configure filebeat (here use filebeat as input)

1> [es official website](https://www.elastic.co/) Download the filebeat tar compression package and decompress it

2> Enter the filebeat directory and modify the configuration file filebeat.yml as follows:

	filebeat.inputs:
	- type: log
	  paths:
	    - /tmp/doris.data
	output.logstash:
	  hosts: ["localhost:5044"]

/tmp/doris.data is the doris data path

3> Start filebeat:

`./filebeat -e -c filebeat.yml -d "publish"`


### 3.Install logstash and doris-out-plugin
1> [es official website](https://www.elastic.co/) Download the logstash tar compressed package and decompress it

2> Copy the logstash-output-doris-0.1.0.gem obtained in step 1 to the logstash installation directory

3> execute

`./bin/logstash-plugin install logstash-output-doris-0.1.0.gem`

Install the plugin

4> Create a new configuration file logstash-doris.conf in the config directory as follows:

	input {
	    beats {
	        port => "5044"
	    }
	}
	
	output {
	    doris {
	        http_hosts => [ "http://127.0.0.1:8030" ]
	        user => doris
	        password => doris
	        db => "logstash_output_test"
	        table => "output"
	        label_prefix => "doris"
	        column_separator => ","
	        columns => "a,b,c,d,e"
	    }
	}

The configuration here needs to be configured according to the configuration instructions

5> Start logstash:

./bin/logstash -f ./config/logstash-doris.conf --config.reload.automatic

### 4.Test Load

Add write data to /tmp/doris.data

`echo a,b,c,d,e >> /tmp/doris.data`

Observe the logstash log. If the status of the returned response is Success, the import was successful. At this time, you can view the imported data in the logstash_output_test.output table

