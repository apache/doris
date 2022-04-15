---
{
    "title": "Upload Action",
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

# Upload Action

Upload Action currently mainly serves the front-end page of FE, and is used for users to load small test files.

## Upload load file

Used to upload a file to the FE node, which can be used to load the file later. Currently only supports uploading files up to 100MB.

### Request

```
POST /api/<namespace>/<db>/<tbl>/upload
```
    
### Path parameters

* `<namespace>`

    Namespace, currently only supports `default_cluster`
        
* `<db>`

    Specify database
    
* `<tbl>`

    Specify table

### Query parameters

* `column_separator`

    Optional, specify the column separator of the file. Default is `\t`
    
* `preview`

    Optional, if set to `true`, up to 10 rows of data rows split according to `column_separator` will be displayed in the returned result.

### Request body

The content of the file to be uploaded, the Content-type is `multipart/form-data`

### Response

```
{
	"msg": "success",
	"code": 0,
	"data": {
        "id": 1,
        "uuid": "b87824a4-f6fd-42c9-b9f1-c6d68c5964c2",
        "originFileName": "data.txt",
        "fileSize": 102400,
        "absPath": "/path/to/file/data.txt"
        "maxColNum" : 5
	},
	"count": 1
}
```

## Load the uploaded file

### Request

```
PUT /api/<namespace>/<db>/<tbl>/upload
```
    
### Path parameters

* `<namespace>`

    Namespace, currently only supports `default_cluster`
    
* `<db>`

    Specify database
    
* `<tbl>`

    Specify table

### Query parameters

* `file_id`

    Specify the load file id, which is returned by the API that uploads the file.

* `file_uuid`

    Specify the file uuid, which is returned by the API that uploads the file.
    
### Header

The options in the header are the same as those in the header in the Stream Load request.

### Request body

None

### Response

```
{
	"msg": "success",
	"code": 0,
	"data": {
		"TxnId": 7009,
		"Label": "9dbdfb0a-120b-47a2-b078-4531498727cb",
		"Status": "Success",
		"Message": "OK",
		"NumberTotalRows": 3,
		"NumberLoadedRows": 3,
		"NumberFilteredRows": 0,
		"NumberUnselectedRows": 0,
		"LoadBytes": 12,
		"LoadTimeMs": 71,
		"BeginTxnTimeMs": 0,
		"StreamLoadPutTimeMs": 1,
		"ReadDataTimeMs": 0,
		"WriteDataTimeMs": 13,
		"CommitAndPublishTimeMs": 53
	},
	"count": 1
}
```

### Example

```
PUT /api/default_cluster/db1/tbl1/upload?file_id=1&file_uuid=b87824a4-f6fd-42c9-b9f1-c6d68c5964c2
```

