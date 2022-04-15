---
{
    "title": "Upload Action",
    "language": "zh-CN"
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

Upload Action 目前主要服务于FE的前端页面，用于用户导入一些测试性质的小文件。

## 上传导入文件

用于将文件上传到FE节点，可在稍后用于导入该文件。目前仅支持上传最大100MB的文件。

### Request

```
POST /api/<namespace>/<db>/<tbl>/upload
```
    
### Path parameters

* `<namespace>`

    命名空间，目前仅支持 `default_cluster`
    
* `<db>`

    指定的数据库
    
* `<tbl>`

    指定的表

### Query parameters

* `column_separator`

    可选项，指定文件的分隔符。默认为 `\t`
    
* `preview`

    可选项，如果设置为 `true`，则返回结果中会显示最多10行根据 `column_separator` 切分好的数据行。

### Request body

要上传的文件内容，Content-type 为 `multipart/form-data`

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

## 导入已上传的文件

### Request

```
PUT /api/<namespace>/<db>/<tbl>/upload
```
    
### Path parameters

* `<namespace>`

    命名空间，目前仅支持 `default_cluster`
    
* `<db>`

    指定的数据库
    
* `<tbl>`

    指定的表

### Query parameters

* `file_id`

    指定导入的文件id，文件id由上传导入文件的API返回。

* `file_uuid`

    指定导入的文件uuid，文件uuid由上传导入文件的API返回。
    
### Header

Header 中的可选项同 Stream Load 请求中 header 的可选项。

### Request body

要上传的文件内容，Content-type 为 `multipart/form-data`

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

