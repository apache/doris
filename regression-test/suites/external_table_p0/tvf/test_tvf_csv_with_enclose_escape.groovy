import org.junit.Assert

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

// This suit test the `backends` tvf
suite("test_tvf_csv_with_enclose_escape", "p0") {
    List<List<Object>> backends =  sql """ select * from backends(); """
    assertTrue(backends.size() > 0)
    def be_id = backends[0][0]

    //reuse stream load case.
    def  load_case_path = context.config.dataPath + "/external_table_p0/tvf/"
    def load_cases = [
        'enclose_normal.csv',
        'enclose_with_escape.csv',
        'enclose_wrong_position.csv',
        'enclose_empty_values.csv'
    ]        

    String outFilePath="test_tvf_csv_with_enclose_escape/"
    
    for (List<Object> backend : backends) {
        def be_host = backend[1]
        for(String load_file_name : load_cases ) {
            scpFiles("root", be_host,load_case_path + load_file_name ,outFilePath,false)
        }
    }


    for(int i = 0 ;i < load_cases.size();i++) {

        String file_name = load_cases[i];

        qt_load_test  """ select * from  local (  
            "file_path" = "${outFilePath}/${file_name}",
            "format" = "csv"                           ,
            "backend_id"="${be_id}"                    ,
            "column_separator"=","                     ,
            "trim_double_quotes"="true"                , 
            "enclose"="\\""                             ,
            "escape"="\\\\"
        ) order by c1,c2,c3,c4,c5,c6;  """ 
    }
    
    def crlf_lf_path = context.config.dataPath + "/external_table_p0/tvf/"
    def file_name_end = """crlf_lf_enclose.csv""" 

    for (List<Object> backend : backends) {
        def be_host = backend[1]
        scpFiles("root", be_host,crlf_lf_path + file_name_end ,outFilePath,false)
    }

    sql """ set keep_carriage_return = true; """
    qt_csv_test_1  """ select * from  local (  
        "file_path" = "${outFilePath}/${file_name_end}",
        "format" = "csv"                           ,
        "backend_id"="${be_id}"                    ,
        "column_separator"=","                     ,
        "trim_double_quotes"="true"                , 
        "enclose"="\\""                             ,
        "escape"="\\\\"
    ) order by c1,c2,c3,c4;  """ 


    sql """ set keep_carriage_return = false; """
    qt_csv_test_2  """ select * from  local (  
        "file_path" = "${outFilePath}/${file_name_end}",
        "format" = "csv"                           ,
        "backend_id"="${be_id}"                    ,
        "column_separator"=","                     ,
        "trim_double_quotes"="true"                , 
        "enclose"="\\""                             ,
        "escape"="\\\\"
    ) order by c1,c2,c3,c4;  """ 

}
