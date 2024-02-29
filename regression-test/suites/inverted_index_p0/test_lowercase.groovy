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


suite("test_lowercase"){
    // prepare test table
    def indexTblName = "lowercase_test1"

    sql "DROP TABLE IF EXISTS ${indexTblName}"
    sql """
	CREATE TABLE IF NOT EXISTS ${indexTblName}(
		`id`int(11)NULL,
		`c` text NULL,
		INDEX c_idx(`c`) USING INVERTED PROPERTIES("parser"="english") COMMENT ''
	) ENGINE=OLAP
	DUPLICATE KEY(`id`)
	COMMENT 'OLAP'
	DISTRIBUTED BY HASH(`id`) BUCKETS 1
	PROPERTIES(
 		"replication_allocation" = "tag.location.default: 1"
	);
    """

    sql "INSERT INTO $indexTblName VALUES (1, 'hello world'), (2, 'HELLO WORLD'), (3, 'Hello World');"
    qt_sql "SELECT * FROM $indexTblName WHERE c MATCH 'hello' ORDER BY id";
    qt_sql "SELECT * FROM $indexTblName WHERE c MATCH 'HELLO' ORDER BY id";
    qt_sql "SELECT * FROM $indexTblName WHERE c MATCH 'Hello' ORDER BY id";

    def indexTblName2 = "lowercase_test2"

    sql "DROP TABLE IF EXISTS ${indexTblName2}"
    sql """
	CREATE TABLE IF NOT EXISTS ${indexTblName2}(
		`id`int(11)NULL,
		`c` text NULL,
		INDEX c_idx(`c`) USING INVERTED PROPERTIES("parser"="unicode") COMMENT ''
	) ENGINE=OLAP
	DUPLICATE KEY(`id`)
	COMMENT 'OLAP'
	DISTRIBUTED BY HASH(`id`) BUCKETS 1
	PROPERTIES(
                "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql "INSERT INTO $indexTblName2 VALUES (1, 'hello 我来到北京清华大学'), (2, 'HELLO 我爱你中国'), (3, 'Hello 人民可以得到更多实惠');"
    qt_sql "SELECT * FROM $indexTblName2 WHERE c MATCH 'hello' ORDER BY id";
    qt_sql "SELECT * FROM $indexTblName2 WHERE c MATCH 'HELLO' ORDER BY id";
    qt_sql "SELECT * FROM $indexTblName2 WHERE c MATCH 'Hello' ORDER BY id";

    def indexTblName3 = "lowercase_test3"

    sql "DROP TABLE IF EXISTS ${indexTblName3}"
    sql """
	CREATE TABLE IF NOT EXISTS ${indexTblName3}(
		`id`int(11)NULL,
		`c` text NULL,
		INDEX c_idx(`c`) USING INVERTED PROPERTIES("parser"="chinese") COMMENT ''
	) ENGINE=OLAP
	DUPLICATE KEY(`id`)
	COMMENT 'OLAP'
	DISTRIBUTED BY HASH(`id`) BUCKETS 1
	PROPERTIES(
                "replication_allocation" = "tag.location.default: 1"
        );
    """

   sql "INSERT INTO $indexTblName3 VALUES (1, 'hello 我来到北京清华大学'), (2, 'HELLO 我爱你中国'), (3, 'Hello 人民可以得到更多实惠');"
   qt_sql "SELECT * FROM $indexTblName3 WHERE c MATCH 'hello' ORDER BY id";
   qt_sql "SELECT * FROM $indexTblName3 WHERE c MATCH 'HELLO' ORDER BY id";
   qt_sql "SELECT * FROM $indexTblName3 WHERE c MATCH 'Hello' ORDER BY id";

   def indexTblName4 = "lowercase_test11"

       sql "DROP TABLE IF EXISTS ${indexTblName4}"
       sql """
   	CREATE TABLE IF NOT EXISTS ${indexTblName4}(
   		`id`int(11)NULL,
   		`c` text NULL,
   		INDEX c_idx(`c`) USING INVERTED PROPERTIES("parser"="english","lower_case"="true") COMMENT ''
   	) ENGINE=OLAP
   	DUPLICATE KEY(`id`)
   	COMMENT 'OLAP'
   	DISTRIBUTED BY HASH(`id`) BUCKETS 1
   	PROPERTIES(
    		"replication_allocation" = "tag.location.default: 1"
   	);
       """

       sql "INSERT INTO $indexTblName4 VALUES (1, 'hello world'), (2, 'HELLO WORLD'), (3, 'Hello World');"
       qt_sql "SELECT * FROM $indexTblName4 WHERE c MATCH 'hello' ORDER BY id";
       qt_sql "SELECT * FROM $indexTblName4 WHERE c MATCH 'HELLO' ORDER BY id";
       qt_sql "SELECT * FROM $indexTblName4 WHERE c MATCH 'Hello' ORDER BY id";

       def indexTblName5 = "lowercase_test12"

       sql "DROP TABLE IF EXISTS ${indexTblName5}"
       sql """
   	CREATE TABLE IF NOT EXISTS ${indexTblName5}(
   		`id`int(11)NULL,
   		`c` text NULL,
   		INDEX c_idx(`c`) USING INVERTED PROPERTIES("parser"="unicode","lower_case"="true") COMMENT ''
   	) ENGINE=OLAP
   	DUPLICATE KEY(`id`)
   	COMMENT 'OLAP'
   	DISTRIBUTED BY HASH(`id`) BUCKETS 1
   	PROPERTIES(
                   "replication_allocation" = "tag.location.default: 1"
           );
       """

       sql "INSERT INTO $indexTblName5 VALUES (1, 'hello 我来到北京清华大学'), (2, 'HELLO 我爱你中国'), (3, 'Hello 人民可以得到更多实惠');"
       qt_sql "SELECT * FROM $indexTblName5 WHERE c MATCH 'hello' ORDER BY id";
       qt_sql "SELECT * FROM $indexTblName5 WHERE c MATCH 'HELLO' ORDER BY id";
       qt_sql "SELECT * FROM $indexTblName5 WHERE c MATCH 'Hello' ORDER BY id";

       def indexTblName6 = "lowercase_test13"

       sql "DROP TABLE IF EXISTS ${indexTblName6}"
       sql """
   	CREATE TABLE IF NOT EXISTS ${indexTblName6}(
   		`id`int(11)NULL,
   		`c` text NULL,
   		INDEX c_idx(`c`) USING INVERTED PROPERTIES("parser"="chinese","lower_case"="true") COMMENT ''
   	) ENGINE=OLAP
   	DUPLICATE KEY(`id`)
   	COMMENT 'OLAP'
   	DISTRIBUTED BY HASH(`id`) BUCKETS 1
   	PROPERTIES(
                   "replication_allocation" = "tag.location.default: 1"
           );
       """

      sql "INSERT INTO $indexTblName6 VALUES (1, 'hello 我来到北京清华大学'), (2, 'HELLO 我爱你中国'), (3, 'Hello 人民可以得到更多实惠');"
      qt_sql "SELECT * FROM $indexTblName6 WHERE c MATCH 'hello' ORDER BY id";
      qt_sql "SELECT * FROM $indexTblName6 WHERE c MATCH 'HELLO' ORDER BY id";
      qt_sql "SELECT * FROM $indexTblName6 WHERE c MATCH 'Hello' ORDER BY id";

      def indexTblName7 = "lowercase_test21"

       sql "DROP TABLE IF EXISTS ${indexTblName7}"
       sql """
      CREATE TABLE IF NOT EXISTS ${indexTblName7}(
      	`id`int(11)NULL,
      	`c` text NULL,
      	INDEX c_idx(`c`) USING INVERTED PROPERTIES("parser"="english","lower_case"="false") COMMENT ''
      ) ENGINE=OLAP
      DUPLICATE KEY(`id`)
      COMMENT 'OLAP'
      DISTRIBUTED BY HASH(`id`) BUCKETS 1
      PROPERTIES(
      	"replication_allocation" = "tag.location.default: 1"
      );
       """

       sql "INSERT INTO $indexTblName7 VALUES (1, 'hello world'), (2, 'HELLO WORLD'), (3, 'Hello World');"
       qt_sql "SELECT * FROM $indexTblName7 WHERE c MATCH 'hello' ORDER BY id";
       qt_sql "SELECT * FROM $indexTblName7 WHERE c MATCH 'HELLO' ORDER BY id";
       qt_sql "SELECT * FROM $indexTblName7 WHERE c MATCH 'Hello' ORDER BY id";

       def indexTblName8 = "lowercase_test22"

       sql "DROP TABLE IF EXISTS ${indexTblName8}"
       sql """
      CREATE TABLE IF NOT EXISTS ${indexTblName8}(
      	`id`int(11)NULL,
      	`c` text NULL,
      	INDEX c_idx(`c`) USING INVERTED PROPERTIES("parser"="unicode","lower_case"="false") COMMENT ''
      ) ENGINE=OLAP
      DUPLICATE KEY(`id`)
      COMMENT 'OLAP'
      DISTRIBUTED BY HASH(`id`) BUCKETS 1
      PROPERTIES(
                   "replication_allocation" = "tag.location.default: 1"
           );
       """

       sql "INSERT INTO $indexTblName8 VALUES (1, 'hello 我来到北京清华大学'), (2, 'HELLO 我爱你中国'), (3, 'Hello 人民可以得到更多实惠');"
       qt_sql "SELECT * FROM $indexTblName8 WHERE c MATCH 'hello' ORDER BY id";
       qt_sql "SELECT * FROM $indexTblName8 WHERE c MATCH 'HELLO' ORDER BY id";
       qt_sql "SELECT * FROM $indexTblName8 WHERE c MATCH 'Hello' ORDER BY id";

       def indexTblName9 = "lowercase_test23"

       sql "DROP TABLE IF EXISTS ${indexTblName9}"
       sql """
      CREATE TABLE IF NOT EXISTS ${indexTblName9}(
      	`id`int(11)NULL,
      	`c` text NULL,
      	INDEX c_idx(`c`) USING INVERTED PROPERTIES("parser"="chinese","lower_case"="false") COMMENT ''
      ) ENGINE=OLAP
      DUPLICATE KEY(`id`)
      COMMENT 'OLAP'
      DISTRIBUTED BY HASH(`id`) BUCKETS 1
      PROPERTIES(
                   "replication_allocation" = "tag.location.default: 1"
           );
       """

      sql "INSERT INTO $indexTblName9 VALUES (1, 'hello 我来到北京清华大学'), (2, 'HELLO 我爱你中国'), (3, 'Hello 人民可以得到更多实惠');"
      qt_sql "SELECT * FROM $indexTblName9 WHERE c MATCH 'hello' ORDER BY id";
      qt_sql "SELECT * FROM $indexTblName9 WHERE c MATCH 'HELLO' ORDER BY id";
      qt_sql "SELECT * FROM $indexTblName9 WHERE c MATCH 'Hello' ORDER BY id";
}
