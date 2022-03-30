---
{
    "title": "DROP FILE",
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

# DROP FILE
## Description

This statement is used to delete an uploaded file.

Grammar:

DROP FILE "file_name" [FROM database]
[properties]

Explain:
File_name: File name.
Database: A DB to which the file belongs, if not specified, uses the DB of the current session.
properties 支持以下参数:

Catalog: Yes. Classification of documents.

## example

1. Delete the file ca.pem

DROP FILE "ca.pem" properties("catalog" = "kafka");

## keyword
DROP,FILE
