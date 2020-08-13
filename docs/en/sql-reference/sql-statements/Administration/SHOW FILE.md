---
{
    "title": "SHOW FILE",
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

# SHOW FILE
## Description

This statement is used to show a file created in a database

Grammar:

SHOW FILE [FROM database];

Explain:

FileId: File ID, globally unique
DbName: The name of the database to which it belongs
Catalog: Custom Categories
FileName: File name
FileSize: File size, unit byte
MD5: Document MD5

## example

1. View uploaded files in my_database

SHOW FILE FROM my_database;

## keyword
SHOW,FILE
