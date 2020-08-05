---
{
    "title": "RESTful API Design Doc",
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

# RESTful API 设计文档

为了便于后续前端和后端代码分离，需要规范后端提供的 RESTful API 的语义和返回结果。本文档给出一个简单的设计原则，供后续修改或新增 API 时参考。

## 总体设计

### URI 设计

1. Path

    资源访问路径，一般由名词组成。如:
    
    ```
    /api/v1/load
    /api/v1/query
    ```
    
    通常 `/api/v1` 为固定开头样式，表示 api 的版本信息。
    
    Path 的设计通常要满足资源的从属关系。示例如下：
    
    ```
    /api/v1/load/<db>/stream_load/
    /api/v1/load/<db>/bulk_load/
    ```
    
    其中 `stream_load` 和 `bulk_load` 为 `load` 类型下的子类。而 `<db>` 为 Path Parameter，在 Path 中表示**在具体对象下将要展开的操作**。

2. Path parameter

    Path parameter 是在 Path 中传递的参数，通常为必须参数，
    
3. Query parameter

    在URI中问号（?）之后出现的key-value的参数，通常是可选参数。但这并不是强制的，可以根据需要灵活选择。
    
### 方法设计

RESTful API 中常见的操作方法为 POST、PUT、GET、DELETE。

1. POST

    POST 通常意味着创建一个新的资源。这个方法不强调幂等性。可以携带 JSON 格式的 Request Body。而返回结果通常也是 JSON 格式的。执行成功的 HTTP status code 通常为 201。
    
2. PUT

    PUT 通常用来对已有资源进行更新。并且通常是进行全量更新而不是部分更新。这个方法建议保证幂等性。可以携带 JSON 格式的 Request Body。而返回结果通常也是 JSON 格式的。执行成功的 HTTP status code 通常为 200。

3. DELETE

    DELETE 用于删除资源。本身建议保证幂等性。执行成功的 HTTP status code 通常为 204。
    
4. GET

    GET 用于获取资源。执行成功的 HTTP status code 通常为 202。GET 方法的条件通常通过 Query Parameter 传递。但对于一些非常复杂的查询场景，可能导致 URI 非常长。因此，有时也可以使用 POST 方法将查询条件放在 Request Body 中，来实现复杂的查询请求。


## Frontend

原则上 Frontend 的虽有 RESTful API 都应该遵循总体设计原则。但是一些现存的 API 考虑到兼容性问题和改动成本，保留已有的设计实现。

1. Http Status Code

    * 401：权限校验失败后返回。包括密码错误、权限错误等。
    * 400：Bad Request。参数缺失、参数格式错误等。
    * 500：内部错误。通常表示一些未知错误或者未正确处理的错误。任何在预期内的错误和服务端能够处理的错误，都不应该返回 500。
    * 200(201,204)：执行成功时返回。执行成功包括请求被正常处理，以及请求失败但被服务器正确处理的请求。

2. Response Body

    除了一些已经在大规模使用的 API 外，返回结构应统一使用以下格式：
    
    ```
    {
        "code": 0,
        "msg": "success",
        "data": {
        	...
        },
        "count": 0
    }
    ```
    
    该返回体只有在 Http Status Code 为 200(201, 204) 的情况下才会返回。
    
    * code：Doris 内部的错误码。0表示执行成功。其他值表示执行错误。
    * msg：如果 code 为 0，则这个字段为 success，否则，这个字段会显示详细的错误信息。
    * data：API 的具体返回结果。只有当 code 为 0 时，该字段才会被设置。否则，该字段为 null。data 字段内的具体格式暂不做要求，可以为任意 JSON 类型。但需要考虑扩展性，比如方便后续增加其他返回结果。
    * count：用于一些前端查看返回结果的大小（如行数）。当前暂不使用。

## Backend

（TODO）