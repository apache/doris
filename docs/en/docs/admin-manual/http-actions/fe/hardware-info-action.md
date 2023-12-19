---
{
    "title": "Hardware Info Action",
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

# Hardware Info Action

## Request

```
GET /rest/v1/hardware_info/fe/
```

## Description

Hardware Info Action is used to obtain the hardware information of the current FE.
    
## Path parameters

None

## Query parameters

None

## Request body

None

## Response

```
{
	"msg": "success",
	"code": 0,
	"data": {
		"VersionInfo": {
			"Git": "git://host/core@5bc28f4c36c20c7b424792df662fc988436e679e",
			"Version": "trunk",
			"BuildInfo": "cmy@192.168.1",
			"BuildTime": "Tuesday, 05 September 2019 11:07:42 CST"
		},
		"HardwareInfo": {
			"NetworkParameter": "...",
			"Processor": "...",
			"OS": "...",
			"Memory": "...",
			"FileSystem": "...",
			"NetworkInterface": "...",
			"Processes": "...",
			"Disk": "..."
		}
	},
	"count": 0
}
```

* The contents of each value in the `HardwareInfo` field are all hardware information text displayed in html format.
