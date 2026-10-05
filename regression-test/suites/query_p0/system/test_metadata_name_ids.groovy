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

suite("test_metadata_name_ids", "p0" ) {
    

	def tableName = "internal.information_schema.metadata_name_ids"
    qt_desc """ desc  ${tableName} """


	sql """ create database if not exists demo; """
	sql """ use demo ; """ 
	
	sql """ create table if not exists test_metadata_name_ids  (
        a int ,
        b varchar(30)
    )
    DUPLICATE KEY(`a`)
    DISTRIBUTED BY HASH(`a`) BUCKETS 10
    PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
    ); 
	"""
	qt_select2 """ select CATALOG_NAME,DATABASE_NAME,TABLE_NAME from ${tableName}
		where CATALOG_NAME="internal" and DATABASE_NAME ="demo" and TABLE_NAME="test_metadata_name_ids";""" 

	sql """ drop table test_metadata_name_ids """ 

	qt_select3 """ select CATALOG_NAME,DATABASE_NAME,TABLE_NAME from ${tableName}
		where CATALOG_NAME="internal" and DATABASE_NAME ="demo" and TABLE_NAME="test_metadata_name_ids";""" 


}
