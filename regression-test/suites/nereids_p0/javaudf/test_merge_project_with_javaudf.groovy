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
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS
// OF ANY KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

suite('test_merge_project_with_javaudf') {
    def jarPath = """${context.file.parent}/../../javaudf_p0/jars/java-udf-case-jar-with-dependencies.jar"""
    sql """
        SET runtime_filter_mode=OFF;
        SET enable_fallback_to_original_planner=false;
        SET ignore_shape_nodes='PhysicalDistribute';
        SET ignore_shape_nodes='PhysicalDistribute';
        SET detail_shape_nodes='PhysicalProject';

        drop table if exists tbl_test_merge_project_with_javaudf force;

        create table tbl_test_merge_project_with_javaudf(a int)
            distributed by hash(a) buckets 2 properties('replication_num' = '1');

        insert into tbl_test_merge_project_with_javaudf values(1), (2), (3);

        drop function if exists func_test_merge_project_with_javaudf(int);

        CREATE FUNCTION func_test_merge_project_with_javaudf(int) RETURNS int PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.IntTest",
            "type"="JAVA_UDF"
        );
    """

    def sqlStr = '''
        select k + 10, k + 20
        from (
            select func_test_merge_project_with_javaudf(a) as k
            from tbl_test_merge_project_with_javaudf
            ) t
    '''

    qt_select_shape "explain shape plan ${sqlStr}"

    // explain no exception
    explain {
        sql sqlStr
    }

    sql """
        drop function if exists func_test_merge_project_with_javaudf(int);
        drop table if exists tbl_test_merge_project_with_javaudf force;
    """
}
