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

/*************************************************************************
> How to use:
    1. g++ doris_client.cpp -o doris_client `mysql_config --cflags --libs`
    2. ./doris_client

> What can this demo do:
    This is a cpp demo for doris client, you can test basic function such as
    connection, CRUD of your doris. You should install mysql prior to running
    this demo.

> Supported mysql version: 5.6, 5.7, ..., 8.0
************************************************************************/

#include "doris_client.h"

#include <iostream>
#include <string>

using std::string;

DorisClient::DorisClient() {
    //init connection
    _client = mysql_init(nullptr);
    if (_client == nullptr) {
        std::cout << "Error:" << mysql_error(_client);
    }
}

DorisClient::~DorisClient() {
    //close connection
    if (_client != nullptr) {
        mysql_close(_client);
    }
}

bool DorisClient::init(const string& host, const string& user, const string& passwd,
                       const string& db_name, int port, const string& sock) {
    // create connection
    _client = mysql_real_connect(_client, host.c_str(), user.c_str(), passwd.c_str(),
                                 db_name.c_str(), port, sock.c_str(), 0);
    if (_client == nullptr) {
        std::cout << "Error: " << mysql_error(_client);
        return false;
    }
    return true;
}

bool DorisClient::exec(const string& sql) {
    if (mysql_query(_client, sql.c_str())) {
        std::cout << "Query Error: " << mysql_error(_client);
        return false;
    }
    _result = mysql_store_result(_client);
    if (_result) {
        int num_fields = mysql_num_fields(_result);
        int num_rows = mysql_num_rows(_result);
        std::cout << "Query result:" << std::endl;
        for (int i = 0; i < num_rows; i++) {
            _row = mysql_fetch_row(_result);
            if (_row == nullptr) {
                break;
            }
            for (int j = 0; j < num_fields; j++) {
                std::cout << _row[j] << "\t";
            }
            std::cout << std::endl;
        }
    } else {
        if (mysql_field_count(_client) == 0) {
            int num_rows = mysql_affected_rows(_client);
            std::cout << "Affected rows: " << num_rows << std::endl;
        } else {
            std::cout << "Get result error: " << mysql_error(_client);
            return false;
        }
    }
    return true;
}

int main() {
    // Doris connection host
    string host = "127.0.0.1";
    // Doris connection port
    int port = 9030;
    // Doris connection username
    string user = "root";
    // Doris connection password
    string password = "";
    // Local mysql sock address
    string sock_add = "/var/lib/mysql/mysql.sock";
    // database to create
    string database = "cpp_doris_db";

    // init connection
    DorisClient client;
    std::cout << "init DorisClient" << std::endl;
    client.init(host, user, password, "", port, sock_add);

    // drop database
    string sql_drop_database_if_not_exist = "drop database if exists " + database;
    std::cout << sql_drop_database_if_not_exist << std::endl;
    client.exec(sql_drop_database_if_not_exist);

    // create database
    string sql_create_database = "create database " + database;
    std::cout << sql_create_database << std::endl;
    client.exec(sql_create_database);

    DorisClient client_new;
    // connect to doris
    client_new.init(host, user, password, database, port, sock_add);
    std::cout << "init new DorisClient" << std::endl;

    // create doris table
    string sql_create_table =
            "CREATE TABLE cpp_doris_table(siteid INT,citycode SMALLINT,pv BIGINT SUM) "
            "AGGREGATE KEY(siteid, citycode) DISTRIBUTED BY HASH(siteid) BUCKETS 10 "
            "PROPERTIES(\"replication_num\" = \"1\")";
    std::cout << sql_create_table << std::endl;
    client_new.exec(sql_create_table);

    // insert into doris table
    string sql_insert = "insert into cpp_doris_table values(1, 2, 3), (4,5,6), (1,2,4)";
    std::cout << sql_insert << std::endl;
    client_new.exec(sql_insert);

    // select from doris table
    string sql_select = "select * from cpp_doris_table";
    std::cout << sql_select << std::endl;
    client_new.exec(sql_select);

    // drop database, clear env
    string drop_database = "drop database " + database;
    std::cout << drop_database << std::endl;
    client_new.exec(drop_database);
    return 0;
}
