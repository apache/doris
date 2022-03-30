---
{
    "title": "RESTORE TABLET",
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

# RESTORE TABLET
Description

This function is used to recover the tablet data that was deleted by mistake in the trash directory.

Note: For the time being, this function only provides an HTTP interface in be service. If it is to be used,
A restore tablet API request needs to be sent to the HTTP port of the be machine for data recovery. The API format is as follows:
Method: Postal
URI: http://be_host:be_http_port/api/restore_tablet?tablet_id=xxx&schema_hash=xxx

'35;'35; example

Curl -X POST "http://hostname:8088 /api /restore" tablet? Tablet id =123456 &schema hash =1111111 "
## keyword
RESTORE,TABLET,RESTORE,TABLET
