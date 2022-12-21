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

# Compile and package
```
mvn clean package
```

At this time we can get a java-udf-demo.jar

# Register function

Register encryption function

There are two parameters here, one is the encrypted content, the other is the secret key, and the return value is a string

## Local file mode
```sql
CREATE FUNCTION ase_encryp(string,string) RETURNS string PROPERTIES (
   "file"="file:///path/work/doris.java.udf.demo/target/java-udf-demo.jar",
   "symbol"="org.apache.doris.udf.demo.AESEncrypt",
   "always_nullable"="true",
   "type"="JAVA_UDF"
);

```

## http method
```sql
CREATE FUNCTION ase_encryp(string,string) RETURNS string PROPERTIES (
   "file"="http://hostname/work/doris.java.udf.demo/java-udf-demo.jar",
   "symbol"="org.apache.doris.udf.demo.AESEncrypt",
   "always_nullable"="true",
   "type"="JAVA_UDF"
);

```
## Run function
```sql
mysql> select ase_encryp('zhangfeng','java_udf_function');
+----------------------------------------------+
| ase_encryp('zhangfeng', 'java_udf_function') |
+----------------------------------------------+
| 4442106BB8C98E74D19CEC0413467810             |
+----------------------------------------------+
1 row in set (0.76 sec)
```

## Decryption function

```sql
CREATE FUNCTION ase_decryp(string,string) RETURNS string PROPERTIES (
  "file"="file:///path/work/doris.java.udf.demo/target/java-udf-demo.jar",
  "symbol"="org.apache.doris.udf.demo.AESDecrypt",
  "always_nullable"="true",
  "type"="JAVA_UDF"
);

```
## http method
```sql
CREATE FUNCTION ase_decryp(string,string) RETURNS string PROPERTIES (
  "file"="http://hostname/work/doris.java.udf.demo/java-udf-demo.jar",
  "symbol"="org.apache.doris.udf.demo.AESDecrypt",
  "always_nullable"="true",
  "type"="JAVA_UDF"
);
```
## Run function

```sql
mysql> select ase_decryp('4442106BB8C98E74D19CEC0413467810','java_udf_function');
+---------------------------------------------------------------------+
| ase_decryp('4442106BB8C98E74D19CEC0413467810', 'java_udf_function') |
+---------------------------------------------------------------------+
| zhangfeng                                                           |
+---------------------------------------------------------------------+
1 row in set (0.02 sec)

```