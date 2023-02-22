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

suite("test_index_equal_select", "inverted_index_select"){
    
    def indexTbName1 = "index_equal_select"

    sql "DROP TABLE IF EXISTS ${indexTbName1}"

    // create table with different index
    sql """
            CREATE TABLE IF NOT EXISTS ${indexTbName1} (
                name varchar(50),
                age int NOT NULL,
                grade varchar(30) NOT NULL,
                registDate datetime NULL,
                studentInfo char(100),
                tearchComment string,
                selfComment text,
                fatherName varchar(50),
                matherName varchar(50),
                otherinfo varchar(100),
                INDEX name_idx(name) USING INVERTED COMMENT 'name index',
                INDEX age_idx(age) USING INVERTED COMMENT 'age index',
                INDEX grade_idx(grade) USING INVERTED PROPERTIES("parser"="none") COMMENT 'grade index',
                INDEX tearchComment_index(tearchComment) USING INVERTED PROPERTIES("parser"="english") COMMENT 'tearchComment index',
                INDEX studentInfo_index(studentInfo) USING INVERTED PROPERTIES("parser"="standard") COMMENT 'studentInfo index',
                INDEX selfComment_index(selfComment) USING INVERTED PROPERTIES("parser"="standard") COMMENT 'studentInfo index',
                INDEX fatherName_idx(fatherName) USING INVERTED PROPERTIES("parser"="standard") COMMENT ' fatherName index'
            )
            DUPLICATE KEY(`name`)
            DISTRIBUTED BY HASH(`name`) BUCKETS 10
            properties("replication_num" = "1");
    """
    // insert data
    sql """ insert into ${indexTbName1} VALUES
        ("zhang san", 10, "grade 5", "2017-10-01", "tall:120cm, weight: 35kg, hobbies: sing, dancing", "Like cultural and recreational activities", "Class activists", "zhang yi", "chen san", "buy dancing book"),
        ("zhang san yi", 11, "grade 5", "2017-10-01", "tall:120cm, weight: 35kg, hobbies: reading book", "A quiet little boy", "learn makes me happy", "zhang yi", "chen san", "buy"),
        ("li si", 9, "grade 4", "2018-10-01",  "tall:100cm, weight: 30kg, hobbies: playing ball", "A naughty boy", "i just want go outside", "li er", "wan jiu", ""),
        ("san zhang", 10, "grade 5", "2017-10-01", "tall:100cm, weight: 30kg, hobbies:", "", "", "", "", ""),
        ("li sisi", 11, "grade 6", "2016-10-01", "tall:150cm, weight: 40kg, hobbies: sing, dancing, running", "good at handiwork and beaty", "", "li ba", "li liuliu", "")
    """

    // case1: test equal
    // case1.0: test index coulume equal ‘’
    qt_sql "select * from ${indexTbName1} where name='' order by name"
    qt_sql "select * from ${indexTbName1} where grade='' order by name"
    qt_sql "select * from ${indexTbName1} where studentInfo='' order by name"
    qt_sql "select * from ${indexTbName1} where selfComment='' order by name"
    qt_sql "select * from ${indexTbName1} where tearchComment='' order by name"
    qt_sql "select * from ${indexTbName1} where fatherName='' order by name"
    // case1.1 test index colume equal
    qt_sql "select * from ${indexTbName1} where name='zhang san' order by name"
    qt_sql "select * from ${indexTbName1} where age=10 order by name"
    qt_sql "select * from ${indexTbName1} where grade='grade 5' order by name"
    qt_sql "select * from ${indexTbName1} where studentInfo='tall:120cm, weight: 35kg' order by name"
    qt_sql "select * from ${indexTbName1} where selfComment='learn makes me happy' order by name"
    qt_sql "select * from ${indexTbName1} where tearchComment='A quiet little boy' order by name"
    qt_sql "select * from ${indexTbName1} where fatherName='zhang yi' order by name"
    // case1.2 test index colume not equal
    qt_sql "select * from ${indexTbName1} where name!='zhang san' order by name"
    qt_sql "select * from ${indexTbName1} where age!=10 order by name"
    qt_sql "select * from ${indexTbName1} where grade!='grade 5' order by name"
    qt_sql "select * from ${indexTbName1} where studentInfo!='tall:120cm, weight: 35kg' order by name"
    qt_sql "select * from ${indexTbName1} where selfComment!='learn makes me happy' order by name"
    qt_sql "select * from ${indexTbName1} where tearchComment!='A quiet little boy' order by name"
    qt_sql "select * from ${indexTbName1} where fatherName!='zhang yi' order by name"
    // case1.3 test index colume and normal colume mix select
    // case1.3.0 default(simple) index and normal colume mix select
    qt_sql """
        select * from ${indexTbName1} where name='zhang san' and registDate="2017-10-01" order by name
        """
    qt_sql """
        select * from ${indexTbName1} where name!='zhang san' and registDate="2017-10-01" order by name
        """
    qt_sql """
        select * from ${indexTbName1} where name!='zhang san' and registDate!="2017-10-01" order by name
        """
    // case 1.3.1 none index and normal colume mix select
    qt_sql """
        select * from ${indexTbName1} where grade='grade 5' and registDate="2017-10-01" order by name
        """
    qt_sql """
        select * from ${indexTbName1} where grade!='grade 5' and registDate="2017-10-01" order by name
        """
    qt_sql """
        select * from ${indexTbName1} where grade!='grade 5' and registDate!="2017-10-01" order by name
        """
    // case 1.3.2 simple index and normal colume mix select
    qt_sql """
        select * from ${indexTbName1} where tearchComment="A quiet little boy" and registDate="2017-10-01" order by name
        """
    qt_sql """
        select * from ${indexTbName1} where tearchComment!="A quiet little boy" and registDate="2017-10-01" order by name
        """
    qt_sql """
        select * from ${indexTbName1} where tearchComment!="A quiet little boy" and registDate!="2017-10-01" order by name
        """
    // case 1.3.3 standard index char and normal colume mix select
     qt_sql """
        select * from ${indexTbName1} where studentInfo="tall:120cm, weight: 35kg" and registDate="2017-10-01" order by name
        """
    qt_sql """
        select * from ${indexTbName1} where studentInfo!="tall:120cm, weight: 35kg" and registDate="2017-10-01" order by name
        """
    qt_sql """
        select * from ${indexTbName1} where selfComment!="learn makes me happy" and registDate!="2017-10-01" order by name
        """
    // case 1.3.4 standard index string and normal colume mix select
    qt_sql """
        select * from ${indexTbName1} where selfComment="learn makes me happy" and registDate="2017-10-01" order by name
        """
    qt_sql """
        select * from ${indexTbName1} where selfComment!="learn makes me happy" and registDate="2017-10-01" order by name
        """
    qt_sql """
        select * from ${indexTbName1} where selfComment!="learn makes me happy" and registDate!="2017-10-01" order by name
        """
    // case 1.3.5 standard index varchar and normal colume mix select
    qt_sql """
        select * from ${indexTbName1} where fatherName='zhang yi' and registDate="2017-10-01" order by name
        """
    qt_sql """
        select * from ${indexTbName1} where fatherName!='zhang yi' and registDate="2017-10-01" order by name
        """
    qt_sql """
        select * from ${indexTbName1} where fatherName!='zhang yi' and registDate!="2017-10-01" order by name
        """
    // case1.3.6 data index and normal colume mix select
    qt_sql """
        select * from ${indexTbName1} where age=10 and registDate="2017-10-01" order by name
        """
    qt_sql """
        select * from ${indexTbName1} where age!=10 and registDate="2017-10-01" order by name
        """
    qt_sql """
        select * from ${indexTbName1} where age!=10 and registDate!="2017-10-01" order by name
        """

    // case 1.4 different colume mix select
    // case1.4.0 data index and string defalut(simple) index mix select
    qt_sql """
        select * from ${indexTbName1} where age=10 and name="zhang san" order by name
        """
    qt_sql """
        select * from ${indexTbName1} where age=10 or name="zhang san" order by name
        """
    // case1.4.1 data index and string none index mix select
    qt_sql """
        select * from ${indexTbName1} where age=10 and grade='grade 5' order by name
        """
    qt_sql """
        select * from ${indexTbName1} where age=10 or grade='grade 5' order by name
        """
    // case1.4.2 data index and string simple mix select
    qt_sql """
        select * from ${indexTbName1} where age=10 and tearchComment!="A quiet little boy" order by name
        """
    qt_sql """
        select * from ${indexTbName1} where age=10 or tearchComment!="A quiet little boy" order by name
        """
    // case1.4.3 data index and string standard mix select
    // case1.4.3.0 data index and varchar standard mix select
    qt_sql """
        select * from ${indexTbName1} where age=10 and fatherName='zhang yi' order by name
        """
    qt_sql """
        select * from ${indexTbName1} where age=10 or fatherName='zhang yi' order by name
        """
    // case1.4.3.1 data index and char standard mix select
    qt_sql """
        select * from ${indexTbName1} where age=10 and studentInfo="tall:120cm, weight: 35kg" order by name
        """
    qt_sql """
        select * from ${indexTbName1} where age=10 or studentInfo="tall:120cm, weight: 35kg" order by name
        """
    // case1.4.3.2 data index and string standard mix select
    qt_sql """
        select * from ${indexTbName1} where age=10 and selfComment="learn makes me happy" order by name
        """
    qt_sql """
        select * from ${indexTbName1} where age=10 or selfComment="learn makes me happy" order by name
        """

    // case1.4.4 none and simple index colume mix select
    qt_sql """
        select * from ${indexTbName1} where grade='grade 5' and name="zhang san" order by name
        """
    qt_sql """
        select * from ${indexTbName1} where grade='grade 5' or name="zhang san" order by name
        """
    qt_sql """
        select * from ${indexTbName1} where grade!='grade 5' and name!="zhang san" order by name
        """
    qt_sql """
        select * from ${indexTbName1} where grade!='grade 5' or name!="zhang san" order by name
        """
    // case1.4.5 none and standard index colume mix select
    qt_sql """
        select * from ${indexTbName1} where grade='grade 5' and selfComment="learn makes me happy" order by name
        """
    qt_sql """
        select * from ${indexTbName1} where grade='grade 5' or selfComment="learn makes me happy" order by name
        """
    qt_sql """
        select * from ${indexTbName1} where grade!='grade 5' and selfComment!="learn makes me happy" order by name
        """
    qt_sql """
        select * from ${indexTbName1} where grade!='grade 5' or selfComment!="learn makes me happy" order by name
        """
    // case1.4.5 simple and standard index colume mix select
    qt_sql """
        select * from ${indexTbName1} where grade='grade 5' and fatherName='zhang yi' order by name
        """
    qt_sql """
        select * from ${indexTbName1} where grade='grade 5' or fatherName='zhang yi' order by name
        """
    qt_sql """
        select * from ${indexTbName1} where grade!='grade 5' and fatherName='zhang yi' order by name
        """
    qt_sql """
        select * from ${indexTbName1} where grade!='grade 5' or fatherName='zhang yi' order by name
        """
    // case1.4.6 standard and standard index colume mix select
    qt_sql """
        select * from ${indexTbName1} where studentInfo="tall:120cm, weight: 35kg" or fatherName='zhang yi' or selfComment="learn makes me happy" order by name
        """
    qt_sql """
        select * from ${indexTbName1} where studentInfo="tall:120cm, weight: 35kg" or fatherName='zhang yi' and selfComment="learn makes me happy" order by name
        """

}
