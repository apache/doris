// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#ifndef SAMPLES_CONNECT_CPP_DORIS_CLIENT_H
#define SAMPLES_CONNECT_CPP_DORIS_CLIENT_H

#include <iostream>
#include <string>

#include <mysql/mysql.h>

using std::string;

class DorisClient {
public:
    DorisClient();
    ~DorisClient();
    // connect to doris
    bool init(const string& host, const string& user, const string& passwd,
                  const string& db_name, int port, const string& sock);
    // excute sql
    bool exec(const string& sql);
private:
    // mysql handle
    MYSQL* _client;
    // doris result
    MYSQL_RES* _result = nullptr;
    //doris result as row
    MYSQL_ROW _row;
};

#endif
