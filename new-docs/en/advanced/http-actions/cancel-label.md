---
{
    "title": "CANCEL LABEL",
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

# CANCEL LABEL
## description
    NAME:
        cancel_label: cancel a transaction with label
        
    SYNOPSIS
        curl -u user:passwd -XPOST http://host:port/api/{db}/_cancel?label={label}

    DESCRIPTION

        This is to cancel a transaction with specified label.

    RETURN VALUES

        Return a JSON format string:

        Status: 
            Success: cancel succeed
            Others: cancel failed
        Message: Error message if cancel failed
           
    ERRORS
    
## example

    1. Cancel the transaction with label "testLabel" on database "testDb"

        curl -u root -XPOST http://host:port/api/testDb/_cancel?label=testLabel
 
## keyword

    CANCEL, LABEL






