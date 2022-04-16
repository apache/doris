---
{
    "title": "GET LABEL STATE",
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

# GET LABEL STATE
## description
    NAME:
        get_load_state: get load's state of label
        
    SYNOPSIS
        curl -u user:passwd http://host:port/api/{db}/get_load_state?label=xxx

    DESCRIPTION

        Check the status of a transaction
        
    RETURN VALUES

        Return of JSON format string of the status of specified transaction:
        Label: The specified label.
        Status: Success or not of this request.
        Message: Error messages
        State: 
           UNKNOWN/PREPARE/COMMITTED/VISIBLE/ABORTED
        
    ERRORS
    
## example

    1. Get status of label "testLabel" on database "testDb"

        curl -u root http://host:port/api/testDb/get_load_state?label=testLabel
 
## keyword

    GET, LOAD, STATE

