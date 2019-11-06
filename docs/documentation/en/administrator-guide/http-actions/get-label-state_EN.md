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
        get_label_state: get label's state
        
    SYNOPSIS
        curl -u user:passwd http://host:port/api/{db}/{label}/_state

    DESCRIPTION

        Check the status of a transaction
        
    RETURN VALUES

        Return of JSON format string of the status of specified transaction:
        执行完毕后，会以Json格式返回这次导入的相关内容。当前包括一下字段
        Label: The specified label.
        Status: Success or not of this request.
        Message: Error messages
        State: 
           UNKNOWN/PREPARE/COMMITTED/VISIBLE/ABORTED
        
    ERRORS
    
## example

    1. Get status of label "testLabel" on database "testDb"

        curl -u root http://host:port/api/testDb/testLabel/_state
 
## keyword

    GET, LABEL, STATE

