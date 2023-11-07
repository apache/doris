---
{
    "title": "导入本地数据",
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

# 导入本地数据
本文档主要介绍如何从客户端导入本地的数据。

目前Doris支持两种从本地导入数据的模式:
1. [Stream Load](../import-way/stream-load-manual.md)
2. [MySql Load](../import-way/mysql-load-manual.md)

## Stream Load
Stream Load 用于将本地文件导入到 Doris 中。

不同于其他命令的提交方式，Stream Load 是通过 HTTP 协议与 Doris 进行连接交互的。

该方式中涉及 HOST:PORT 应为 HTTP 协议端口。

- BE 的 HTTP 协议端口，默认为 8040。
- FE 的 HTTP 协议端口，默认为 8030。但须保证客户端所在机器网络能够联通 BE 所在机器。

本文文档我们以 [curl](https://curl.se/docs/manpage.html) 命令为例演示如何进行数据导入。

文档最后，我们给出一个使用 Java 导入数据的代码示例

### 导入数据

Stream Load 的请求体如下：

```text
PUT /api/{db}/{table}/_stream_load
```

1. 创建一张表

   通过 `CREATE TABLE` 命令在`demo`创建一张表用于存储待导入的数据。具体的建表方式请查阅 [CREATE TABLE](../../../sql-manual/sql-reference/Data-Definition-Statements/Create/CREATE-TABLE.md) 命令手册。示例如下：

   ```sql
   CREATE TABLE IF NOT EXISTS load_local_file_test
   (
       id INT,
       age TINYINT,
       name VARCHAR(50)
   )
   unique key(id)
   DISTRIBUTED BY HASH(id) BUCKETS 3;
   ```

2. 导入数据

   执行以下 curl 命令导入本地文件：

   ```text
    curl -u user:passwd -H "label:load_local_file_test" -T /path/to/local/demo.txt http://host:port/api/demo/load_local_file_test/_stream_load
   ```

   - user:passwd 为在 Doris 中创建的用户。初始用户为 admin / root，密码初始状态下为空。
   - host:port 为 BE 的 HTTP 协议端口，默认是 8040，可以在 Doris 集群 WEB UI页面查看。
   - label: 可以在 Header 中指定 Label 唯一标识这个导入任务。

   关于 Stream Load 命令的更多高级操作，请参阅 [Stream Load](../../../sql-manual/sql-reference/Data-Manipulation-Statements/Load/STREAM-LOAD.md) 命令文档。

3. 等待导入结果

   Stream Load 命令是同步命令，返回成功即表示导入成功。如果导入数据较大，可能需要较长的等待时间。示例如下:

   ```json
   {
       "TxnId": 1003,
       "Label": "load_local_file_test",
       "Status": "Success",
       "Message": "OK",
       "NumberTotalRows": 1000000,
       "NumberLoadedRows": 1000000,
       "NumberFilteredRows": 1,
       "NumberUnselectedRows": 0,
       "LoadBytes": 40888898,
       "LoadTimeMs": 2144,
       "BeginTxnTimeMs": 1,
       "StreamLoadPutTimeMs": 2,
       "ReadDataTimeMs": 325,
       "WriteDataTimeMs": 1933,
       "CommitAndPublishTimeMs": 106,
       "ErrorURL": "http://192.168.1.1:8042/api/_load_error_log?file=__shard_0/error_log_insert_stmt_db18266d4d9b4ee5-abb00ddd64bdf005_db18266d4d9b4ee5_abb00ddd64bdf005"
   }
   ```

   - `Status` 字段状态为 `Success` 即表示导入成功。
   - 其他字段的详细介绍，请参阅 [Stream Load](../../../sql-manual/sql-reference/Data-Manipulation-Statements/Load/STREAM-LOAD.md) 命令文档。

### 导入建议

- Stream Load 只能导入本地文件。
- 建议一个导入请求的数据量控制在 1 - 2 GB 以内。如果有大量本地文件，可以分批并发提交。

### Java 代码示例

这里通过一个简单的 JAVA 示例来执行 Stream Load：

```java
package demo.doris;

import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.FileEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/*
这是一个 Doris Stream Load 示例，需要依赖
<dependency>
    <groupId>org.apache.httpcomponents</groupId>
    <artifactId>httpclient</artifactId>
    <version>4.5.13</version>
</dependency>
 */
public class DorisStreamLoader {
    //可以选择填写 FE 地址以及 FE 的 http_port，但须保证客户端和 BE 节点的连通性。
    private final static String HOST = "your_host";
    private final static int PORT = 8040;
    private final static String DATABASE = "db1";   // 要导入的数据库
    private final static String TABLE = "tbl1";     // 要导入的表
    private final static String USER = "root";      // Doris 用户名
    private final static String PASSWD = "";        // Doris 密码
    private final static String LOAD_FILE_NAME = "/path/to/1.txt"; // 要导入的本地文件路径

    private final static String loadUrl = String.format("http://%s:%s/api/%s/%s/_stream_load",
            HOST, PORT, DATABASE, TABLE);

    private final static HttpClientBuilder httpClientBuilder = HttpClients
            .custom()
            .setRedirectStrategy(new DefaultRedirectStrategy() {
                @Override
                protected boolean isRedirectable(String method) {
                    // 如果连接目标是 FE，则需要处理 307 redirect。
                    return true;
                }
            });

    public void load(File file) throws Exception {
        try (CloseableHttpClient client = httpClientBuilder.build()) {
            HttpPut put = new HttpPut(loadUrl);
            put.setHeader(HttpHeaders.EXPECT, "100-continue");
            put.setHeader(HttpHeaders.AUTHORIZATION, basicAuthHeader(USER, PASSWD));

            // 可以在 Header 中设置 stream load 相关属性，这里我们设置 label 和 column_separator。
            put.setHeader("label","label1");
            put.setHeader("column_separator",",");

            // 设置导入文件。
            // 这里也可以使用 StringEntity 来传输任意数据。
            FileEntity entity = new FileEntity(file);
            put.setEntity(entity);

            try (CloseableHttpResponse response = client.execute(put)) {
                String loadResult = "";
                if (response.getEntity() != null) {
                    loadResult = EntityUtils.toString(response.getEntity());
                }

                final int statusCode = response.getStatusLine().getStatusCode();
                if (statusCode != 200) {
                    throw new IOException(
                            String.format("Stream load failed. status: %s load result: %s", statusCode, loadResult));
                }

                System.out.println("Get load result: " + loadResult);
            }
        }
    }

    private String basicAuthHeader(String username, String password) {
        final String tobeEncode = username + ":" + password;
        byte[] encoded = Base64.encodeBase64(tobeEncode.getBytes(StandardCharsets.UTF_8));
        return "Basic " + new String(encoded);
    }

    public static void main(String[] args) throws Exception{
        DorisStreamLoader loader = new DorisStreamLoader();
        File file = new File(LOAD_FILE_NAME);
        loader.load(file);
    }
}
```



>注意：这里 http client 的版本要是4.5.13
> ```xml
><dependency>
>    <groupId>org.apache.httpcomponents</groupId>
>    <artifactId>httpclient</artifactId>
>    <version>4.5.13</version>
></dependency>
> ```

## MySql LOAD
<version since="dev">
    MySql LOAD样例
</version>

### 导入数据
1. 创建一张表

   通过 `CREATE TABLE` 命令在`demo`创建一张表用于存储待导入的数据

   ```sql
   CREATE TABLE IF NOT EXISTS load_local_file_test
   (
   id INT,
   age TINYINT,
   name VARCHAR(50)
   )
   unique key(id)
   DISTRIBUTED BY HASH(id) BUCKETS 3;
   ```

2. 导入数据
   在MySql客户端下执行以下 SQL 命令导入本地文件：

   ```sql
   LOAD DATA
   LOCAL
   INFILE '/path/to/local/demo.txt'
   INTO TABLE demo.load_local_file_test
   ```

   关于 MySQL Load 命令的更多高级操作，请参阅 [MySQL Load](../../../sql-manual/sql-reference/Data-Manipulation-Statements/Load/MYSQL-LOAD.md) 命令文档。

3. 等待导入结果

   MySql Load 命令是同步命令，返回成功即表示导入成功。如果导入数据较大，可能需要较长的等待时间。示例如下:

   ```text
   Query OK, 1 row affected (0.17 sec)
   Records: 1  Deleted: 0  Skipped: 0  Warnings: 0
   ```

   - 如果出现上述结果, 则表示导入成功。导入失败, 会抛出错误,并在客户端显示错误原因
   - 其他字段的详细介绍，请参阅 [MySQL Load](../../../sql-manual/sql-reference/Data-Manipulation-Statements/Load/MYSQL-LOAD.md) 命令文档。

### 导入建议
- MySql Load 只能导入本地文件(可以是客户端本地或者连接的FE节点本地), 而且支持CSV格式。
- 建议一个导入请求的数据量控制在 1 - 2 GB 以内。如果有大量本地文件，可以分批并发提交。
