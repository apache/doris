---
{
    "title": "Cancel Label",
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

# Cancel Label
Description
NAME:
cancel_label: cancel a transaction with label

SYNOPSIS
curl -u user:passwd -XPOST http://host:port/api/{db}/{label}/_cancel

DESCRIPTION
This command is used to cancel a transaction corresponding to a specified Label, which can be successfully cancelled during the Prepare phase.

RETURN VALUES
When the execution is complete, the relevant content of this import will be returned in Json format. Currently includes the following fields
Status: Successful cancel
Success: 成功cancel事务
20854; 2018282: 22833; 361333;
Message: Specific Failure Information

ERRORS

'35;'35; example

1. cancel testDb, testLabel20316;- 19994;
curl -u root -XPOST http://host:port/api/testDb/testLabel/_cancel

## keyword
Cancel, Label
