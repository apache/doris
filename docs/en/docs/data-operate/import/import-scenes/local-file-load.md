---
{
    "title": "Import Local Data",
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

# Import Local Data
The following mainly introduces how to import local data in client.

Now Doris support two way to load data from client local file:
1. [Stream Load](../import-way/stream-load-manual.md)
2. [MySql Load](../import-way/mysql-load-manual.md)

## Stream Load

Stream Load is used to import local files into Doris.

Unlike the submission methods of other commands, Stream Load communicates with Doris through the HTTP protocol.

The HOST:PORT involved in this method should be the HTTP protocol port.

- BE's HTTP protocol port, the default is 8040.
- FE's HTTP protocol port, the default is 8030. However, it must be ensured that the network of the machine where the client is located can connect to the machine where the BE is located.

In this document, we use the [curl](https://curl.se/docs/manpage.html) command as an example to demonstrate how to import data.

At the end of the document, we give a code example of importing data using Java

### Import Data

The request body of Stream Load is as follows:

````text
PUT /api/{db}/{table}/_stream_load
````

1. Create a table

   Use the `CREATE TABLE` command to create a table in the `demo` to store the data to be imported. For the specific import method, please refer to the [CREATE TABLE](../../../sql-manual/sql-reference/Data-Definition-Statements/Create/CREATE-TABLE.md) command manual. An example is as follows:

   ```sql
   CREATE TABLE IF NOT EXISTS load_local_file_test
   (
       id INT,
       age TINYINT,
       name VARCHAR(50)
   )
   unique key(id)
   DISTRIBUTED BY HASH(id) BUCKETS 3;
   ````

2. Import data

   Execute the following curl command to import the local file:

   ````text
    curl -u user:passwd -H "label:load_local_file_test" -T /path/to/local/demo.txt http://host:port/api/demo/load_local_file_test/_stream_load
   ````

   - user:passwd is the user created in Doris. The initial user is admin/root, and the password is blank in the initial state.
   - host:port is the HTTP protocol port of BE, the default is 8040, which can be viewed on the Doris cluster WEB UI page.
   - label: Label can be specified in the Header to uniquely identify this import task.

   For more advanced operations of the Stream Load command, see [Stream Load](../../../sql-manual/sql-reference/Data-Manipulation-Statements/Load/STREAM-LOAD.md) Command documentation.

3. Wait for the import result

   The Stream Load command is a synchronous command, and a successful return indicates that the import is successful. If the imported data is large, a longer waiting time may be required. Examples are as follows:

   ````json
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
   ````

   - The status of the `Status` field is `Success`, which means the import is successful.
   - For details of other fields, please refer to the [Stream Load](../../../sql-manual/sql-reference/Data-Manipulation-Statements/Load/STREAM-LOAD.md) command documentation.

### Import Suggestion

- Stream Load can only import local files.
- It is recommended to limit the amount of data for an import request to 1 - 2 GB. If you have a large number of local files, you can submit them concurrently in batches.

### Java Code Examples

Here is a simple JAVA example to execute Stream Load:

````java
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
This is an example of Doris Stream Load, which requires dependencies
<dependency>
    <groupId>org.apache.httpcomponents</groupId>
    <artifactId>httpclient</artifactId>
    <version>4.5.13</version>
</dependency>
 */
public class DorisStreamLoader {
    //You can choose to fill in the FE address and the http_port of the FE, but the connectivity between the client and the BE node must be guaranteed.
    private final static String HOST = "your_host";
    private final static int PORT = 8040;
    private final static String DATABASE = "db1"; // database to import
    private final static String TABLE = "tbl1"; // table to import
    private final static String USER = "root"; // Doris username
    private final static String PASSWD = ""; // Doris password
    private final static String LOAD_FILE_NAME = "/path/to/1.txt"; // local file path to import

    private final static String loadUrl = String.format("http://%s:%s/api/%s/%s/_stream_load",
            HOST, PORT, DATABASE, TABLE);

    private final static HttpClientBuilder httpClientBuilder = HttpClients
            .custom()
            .setRedirectStrategy(new DefaultRedirectStrategy() {
                @Override
                protected boolean isRedirectable(String method) {
                    // If the connection target is FE, you need to handle 307 redirect.
                    return true;
                }
            });

    public void load(File file) throws Exception {
        try (CloseableHttpClient client = httpClientBuilder.build()) {
            HttpPut put = new HttpPut(loadUrl);
            put.setHeader(HttpHeaders.EXPECT, "100-continue");
            put.setHeader(HttpHeaders.AUTHORIZATION, basicAuthHeader(USER, PASSWD));

            // You can set stream load related properties in Header, here we set label and column_separator.
            put.setHeader("label","label1");
            put.setHeader("column_separator",",");

            // Set the import file.
            // StringEntity can also be used here to transfer arbitrary data.
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
````

> Note: The version of http client here is 4.5.13
> ```xml
> <dependency>
> <groupId>org.apache.httpcomponents</groupId>
> <artifactId>httpclient</artifactId>
> <version>4.5.13</version>
> </dependency>
> ```

## MySql LOAD
<version since="dev">
    Example of mysql load
</version>

### Import Data
1. Create a table

   Use the `CREATE TABLE` command to create a table in the `demo` database to store the data to be imported.

   ```sql
   CREATE TABLE IF NOT EXISTS load_local_file_test
   (
   id INT,
   age TINYINT,
   name VARCHAR(50)
   )
   unique key(id)
   DISTRIBUTED BY HASH(id) BUCKETS 3;
   ````

2. Import data
   Excute fellowing sql statmeent in the mysql client to load client local file:

   ```sql
   LOAD DATA
   LOCAL
   INFILE '/path/to/local/demo.txt'
   INTO TABLE demo.load_local_file_test
   ```

   For more advanced operations of the MySQL Load command, see [MySQL Load](../../../sql-manual/sql-reference/Data-Manipulation-Statements/Load/MYSQL-LOAD.md) Command documentation.

3. Wait for the import result

   The MySql Load command is a synchronous command, and a successful return indicates that the import is successful. If the imported data is large, a longer waiting time may be required. Examples are as follows:

   ```text
   Query OK, 1 row affected (0.17 sec)
   Records: 1  Deleted: 0  Skipped: 0  Warnings: 0
   ```

   - Load success if the client show the return rows. Otherwise sql statement will throw an exception and show the error message in client.
   - For details of other fields, please refer to the [MySQL Load](../../../sql-manual/sql-reference/Data-Manipulation-Statements/Load/MYSQL-LOAD.md) command documentation.

### Import Suggestion

   - MySql Load can only import local files(which can be client local file or fe local file) and only support csv format.
   - It is recommended to limit the amount of data for an import request to 1 - 2 GB. If you have a large number of local files, you can submit them concurrently in batches.
