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
> Useage:
        1. g++ doris_client.cpp -o doris_client `mysql_config --cflags --libs`
        2. ./doris_client
> Comment: Supported mysql version: 5.6, 5.7, 8.0
************************************************************************/

#include "doris_client.h"

#include <iostream>
#include <string>

using std::string;

Doris::Doris() {
    //init connection
    doris = mysql_init(NULL);
    if (doris == NULL) {
        std::cout << "Error:" << mysql_error(doris);
    }
}

Doris::~Doris() {
    //close connection
    if (doris != NULL) {
        mysql_close(doris);
    }
}

bool Doris::initDoris(const string& host, const string& user, const string& passwd,
                      const string& db_name, int port, const string& sock) {
    // create connection
    doris = mysql_real_connect(doris, host.c_str(), user.c_str(), passwd.c_str(),
            db_name.c_str(), port, sock.c_str(), 0);
    if (doris == NULL) {
        std::cout << "Error: " << mysql_error(doris);
        return false;
    }
    return true;
}

bool Doris::exeSQL(const string& sql) {
    if (mysql_query(doris, sql.c_str())) {
        std::cout << "Query Error: " << mysql_error(doris);
        return false;
    } else {
        result = mysql_store_result(doris);
        if (result) {
           int num_fields = mysql_num_fields(result);
           int num_rows = mysql_num_rows(result);
           for (int i = 0; i < num_rows; i++) {
                row = mysql_fetch_row(result);
                if (row < 0) {
                    break;
                }
                for (int j = 0; j < num_fields; j++) {
                    std::cout << row[j] << "\t";
                }
                std::cout << std::endl;
            }
        } else {
            if (mysql_field_count(doris) == 0) {
                int num_rows = mysql_affected_rows(doris);
                std::cout << "Reslut rows: " << num_rows << std::endl;
            } else {
                std::cout << "Get result error: " << mysql_error(doris);
                return false;
            }
        }
    }
    return true;
}

int main() {
    // Doris connection host
    string host = "127.0.0.1";
    // Doris connection port
    int port = 8080;
    // Doris connection username
    string user = "username";
    // Doris connection password
    string password = "password";
    // Local mysql sock address
    string sock_add = "/var/lib/mysql/mysql.sock";
    // database to create
    string database = "cpp_doris_db";

    // init connection
    Doris db;
    std::cout << "init db" << std::endl;
    db.initDoris(host, user, password, "", port, sock_add);

    // drop database
    string sql_drop_database_if_not_exist = "drop database if exists " + database;
    std::cout << sql_drop_database_if_not_exist << std::endl;
    db.exeSQL(sql_drop_database_if_not_exist);

    // create database
    string sql_create_database = "create database " + database;
    std::cout << sql_create_database << std::endl;
    db.exeSQL(sql_create_database);

    Doris db_new;
    // connect to doris
    db_new.initDoris(host, user, password, database, port, sock_add);
    std::cout << "init db_new" << std::endl;

    // create doris table
    string sql_create_table = "CREATE TABLE cpp_doris_table(siteid INT,citycode SMALLINT,pv BIGINT SUM) "\
                            "AGGREGATE KEY(siteid, citycode) DISTRIBUTED BY HASH(siteid) BUCKETS 10 "\
                            "PROPERTIES(\"replication_num\" = \"1\");";
    std::cout << sql_create_table << std::endl;
    db_new.exeSQL(sql_create_table);

    // insert into doris table
    string sql_insert = "insert into cpp_doris_table values(1, 2, 3);";
    std::cout << sql_insert << std::endl;
    db_new.exeSQL(sql_insert);

    // select from doris table
    string sql_select = "select * from cpp_doris_table;";
    std::cout << sql_select << std::endl;
    db_new.exeSQL(sql_select);

    // drop database, clear env
    string drop_database = "drop database " + database;
    std::cout << drop_database << std::endl;
    db_new.exeSQL(drop_database);
    return 0;
}
