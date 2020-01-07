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

#ifndef _MYDB_H
#define _MYDB_H
#include<iostream>
#include<string>
#include<mysql/mysql.h>
using namespace std;

class MyDB
{
    public:
        MyDB();
        ~MyDB();
        bool initDB(string host,string user,string passwd,string db_name,int port,string sock); // connect to mysql
        bool exeSQL(string sql);   // excute sql
    private:
        MYSQL *mysql;
        MYSQL_RES *result;
        MYSQL_ROW row;
};

#endif
