---
{
    "title": "SHOW TRASH",
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

# SHOW TRASH
## description

This statement is used to view trash used capacity on some backends.

    Syntax:

        SHOW TRASH [ON "BackendHost:BackendHeartBeatPort"];

    Explain:

        1. Backend The format is BackendHost:BackendHeartBeatPort of the node. 
        2. TrashUsedCapacity Indicates that the trash data of the node occupies space. 

## example

    1. View the space occupied by trash data of all be nodes. 

        SHOW TRASH;

    2. Check the space occupied by trash data of '192.168.0.1:9050'(The specific disk information will be displayed). 

        SHOW TRASH ON "192.168.0.1:9050";

## keyword
    SHOW, TRASH

