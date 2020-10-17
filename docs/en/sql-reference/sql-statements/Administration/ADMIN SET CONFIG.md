---
{
    "title": "ADMIN SET CONFIG",
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

# ADMIN SET CONFIG
## Description

This statement is used to set the configuration items for the cluster (currently only the configuration items for setting FE are supported).
Settable configuration items can be viewed through `ADMIN SHOW FRONTEND CONFIG;` commands.

Grammar:

ADMIN SET FRONTEND CONFIG ("key" = "value");

## example

1. "disable balance" true

ADMIN SET FRONTEND CONFIG ("disable_balance" = "true");

## keyword
ADMIN,SET,CONFIG
