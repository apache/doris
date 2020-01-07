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
        1. g++ MyDB.cpp -o MyDb `mysql_config --cflags --libs`
        2. ./MyDB
************************************************************************/

#include<iostream>
#include<string>
#include "MyDB.h"

using namespace std;

MyDB::MyDB()
{
    mysql=mysql_init(NULL);   //init connection
    if(mysql==NULL)
    {
        cout<<"Error:"<<mysql_error(mysql);
    }
}

MyDB::~MyDB()
{
    if(mysql!=NULL)  //close connection
    {
        mysql_close(mysql);
    }
}

bool MyDB::initDB(string host, string user, string passwd, string db_name, int port, string sock)
{
    // create connection
    mysql = mysql_real_connect(mysql, host.c_str(), user.c_str(), passwd.c_str(),
            db_name.c_str(), port, sock.c_str(), 0);
    if(mysql == NULL)
    {
        cout << "Error: " << mysql_error(mysql);
        return false;
    }
    return true;
}

bool MyDB::exeSQL(string sql)
{
    if (mysql_query(mysql,sql.c_str()))
    {
        cout << "Query Error: " << mysql_error(mysql);
        return false;
    }
    else
    {
        result = mysql_store_result(mysql);
        if (result)
        {
           int num_fields = mysql_num_fields(result);
           int num_rows = mysql_num_rows(result);
           for(int i = 0; i < num_rows; i++)
            {
                row = mysql_fetch_row(result);
                if(row < 0) break;

                for(int j = 0; j < num_fields; j++)
                {
                    cout << row[j] << "\t";
                }
                cout << endl;
            }

        }
        else
        {
            if(mysql_field_count(mysql) == 0)
            {
                int num_rows = mysql_affected_rows(mysql);
            }
            else
            {
                cout << "Get result error: " << mysql_error(mysql);
                return false;
            }
        }
    }
    return true;
}

int main()
{
    string host = "127.0.0.1";    // Doris connection host
    int port = 8080;             // Doris connection port
    string user = "username";        // Doris connection username
    string password = "password";    // Doris connection password
    string sock_add = "/var/lib/mysql/mysql.sock"; // Local mysql sock address
    string database = "cpp_doris_db";   // database to create

    // init connection
    MyDB db;
    cout << "init db" << endl;
    db.initDB(host, user, password, "", port, sock_add);

    // drop database
    string sql_drop_database_if_not_exist = "drop database if exists " + database;
    cout << sql_drop_database_if_not_exist << endl;
    db.exeSQL(sql_drop_database_if_not_exist);

    // create database
    string sql_create_database = "create database " + database;
    cout << sql_create_database << endl;
    db.exeSQL(sql_create_database);

    MyDB db_new;
    // connect to doris
    db_new.initDB(host, user, password, database, port, sock_add);
    cout << "init db_new" << endl;

    // create doris table
    string sql_create_table = "CREATE TABLE cpp_doris_table(siteid INT,citycode SMALLINT,pv BIGINT SUM) "\
                            "AGGREGATE KEY(siteid, citycode) DISTRIBUTED BY HASH(siteid) BUCKETS 10 "\
                            "PROPERTIES(\"replication_num\" = \"1\");";
    cout << sql_create_table << endl;
    db_new.exeSQL(sql_create_table);

    // insert to doris table
    string sql_insert = "insert into cpp_doris_table values(1, 2, 3);";
    cout << sql_insert << endl;
    db_new.exeSQL(sql_insert);

    // select from doris table
    string sql_select = "select * from cpp_doris_table;";
    cout << sql_select << endl;
    db_new.exeSQL(sql_select);

    // drop database, clear env
    string drop_database = "drop database " + database;
    cout << drop_database << endl;
    db_new.exeSQL(drop_database);
    return 0;
}
