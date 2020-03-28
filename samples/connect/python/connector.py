#!/usr/bin/python
# -*- coding: UTF-8 -*-

import mysql.connector

config = {
    "user": "root",
    "password": "",
    "host": "192.168.100.80",
    "port": 9030,
    "charset": "utf8"
}

# connect to doris
try:
    cnx = mysql.connector.connect(**config)
except mysql.connector.Error as err:
    print("connect to doris failed. {}".format(err))
    exit(1)
print("connect to doris successfully")

cursor = cnx.cursor()

# create database
try:
    cursor.execute("CREATE DATABASE IF NOT EXISTS db_test")
except mysql.connector.Error as err:
    print("create database failed. {}".format(err))
    exit(1)
print("create database successfully")

# set db context
try:
    cursor.execute("USE db_test")
except mysql.connector.Error as err:
    print("set db context failed. {}".format(err))
    exit(1)
print("set db context successfully")

# create table
sql = ("CREATE TABLE IF NOT EXISTS table_test(siteid INT, citycode SMALLINT, pv BIGINT SUM) " 
      "AGGREGATE KEY(siteid, citycode) "
      "DISTRIBUTED BY HASH(siteid) BUCKETS 10 "
      "PROPERTIES(\"replication_num\" = \"1\")")
try:
    cursor.execute(sql)
except mysql.connector.Error as err:
    print("create table failed. {}".format(err))
    exit(1)
print("create table successfully")

# insert data
sql = "INSERT INTO table_test values(1, 2, 3), (4, 5, 6), (1, 2, 4)"
try:
    cursor.execute(sql)
except mysql.connector.Error as err:
    print("insert data failed. {}".format(err))
    exit(1)
print("insert data successfully")

# query data
sql = "SELECT siteid, citycode, pv FROM table_test"
try:
    cursor.execute(sql)
except mysql.connector.Error as err:
    print("query data failed. {}".format(err))
    exit(1)
print("query data successfully")
print("siteid\tcitycode\tpv")
for (siteid, citycode, pv) in cursor:
    print("{}\t{}\t{}").format(siteid, citycode, pv)

# drop database
try:
    cursor.execute("DROP DATABASE IF EXISTS db_test")
except mysql.connector.Error as err:
    print("drop database failed. {}".format(err))
    exit(1)
print("drop database successfully")
