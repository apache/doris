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

suite("test_pythonudf_aggregate") {
    def runtime_version = "3.8.10"

    try {
        // Test 1: Create simple aggregate function (although Python UDF is mainly for scalar functions)
        // Test using Python UDF in aggregate queries
        sql """ DROP FUNCTION IF EXISTS py_score_grade(DOUBLE); """
        sql """
        CREATE FUNCTION py_score_grade(DOUBLE) 
        RETURNS STRING 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "evaluate",
            "runtime_version" = "${runtime_version}"
        )
        AS \$\$
def evaluate(score):
    if score is None:
        return None
    if score >= 90:
        return 'A'
    elif score >= 80:
        return 'B'
    elif score >= 70:
        return 'C'
    elif score >= 60:
        return 'D'
    else:
        return 'F'
\$\$;
        """
        
        // Create test table
        sql """ DROP TABLE IF EXISTS student_scores; """
        sql """
        CREATE TABLE student_scores (
            student_id INT,
            student_name STRING,
            subject STRING,
            score DOUBLE
        ) ENGINE=OLAP 
        DUPLICATE KEY(student_id)
        DISTRIBUTED BY HASH(student_id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO student_scores VALUES
        (1, 'Alice', 'Math', 95.0),
        (1, 'Alice', 'English', 88.0),
        (1, 'Alice', 'Science', 92.0),
        (2, 'Bob', 'Math', 78.0),
        (2, 'Bob', 'English', 85.0),
        (2, 'Bob', 'Science', 80.0),
        (3, 'Charlie', 'Math', 65.0),
        (3, 'Charlie', 'English', 70.0),
        (3, 'Charlie', 'Science', 68.0),
        (4, 'David', 'Math', 55.0),
        (4, 'David', 'English', 60.0),
        (4, 'David', 'Science', 58.0);
        """
        
        // Test using UDF in SELECT
        qt_select_grades """ 
        SELECT 
            student_id,
            student_name,
            subject,
            score,
            py_score_grade(score) AS grade
        FROM student_scores
        ORDER BY student_id, subject;
        """
        
        // Test using UDF in GROUP BY
        qt_select_group_by_grade """ 
        SELECT 
            py_score_grade(score) AS grade,
            COUNT(*) AS count,
            AVG(score) AS avg_score
        FROM student_scores
        GROUP BY py_score_grade(score)
        ORDER BY grade;
        """
        
        // Test using UDF in aggregate functions
        qt_select_aggregate_with_udf """ 
        SELECT 
            student_id,
            student_name,
            AVG(score) AS avg_score,
            py_score_grade(AVG(score)) AS avg_grade
        FROM student_scores
        GROUP BY student_id, student_name
        ORDER BY student_id;
        """
        
        // Test 2: Create classification function for aggregate analysis
        sql """ DROP FUNCTION IF EXISTS py_age_group(INT); """
        sql """
        CREATE FUNCTION py_age_group(INT) 
        RETURNS STRING 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "evaluate",
            "runtime_version" = "${runtime_version}"
        )
        AS \$\$
def evaluate(age):
    if age is None:
        return None
    if age < 18:
        return 'Minor'
    elif age < 30:
        return 'Young Adult'
    elif age < 50:
        return 'Adult'
    else:
        return 'Senior'
\$\$;
        """
        
        sql """ DROP TABLE IF EXISTS users; """
        sql """
        CREATE TABLE users (
            user_id INT,
            name STRING,
            age INT,
            salary DOUBLE
        ) ENGINE=OLAP 
        DUPLICATE KEY(user_id)
        DISTRIBUTED BY HASH(user_id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO users VALUES
        (1, 'User1', 16, 0),
        (2, 'User2', 25, 50000),
        (3, 'User3', 35, 80000),
        (4, 'User4', 55, 100000),
        (5, 'User5', 28, 60000),
        (6, 'User6', 45, 90000),
        (7, 'User7', 22, 45000),
        (8, 'User8', 60, 110000);
        """
        
        qt_select_age_group_aggregate """ 
        SELECT 
            py_age_group(age) AS age_group,
            COUNT(*) AS user_count,
            AVG(salary) AS avg_salary,
            MAX(salary) AS max_salary,
            MIN(salary) AS min_salary
        FROM users
        GROUP BY py_age_group(age)
        ORDER BY age_group;
        """
        
        // Test 3: Use UDF in HAVING clause
        qt_select_having_with_udf """ 
        SELECT 
            student_id,
            student_name,
            AVG(score) AS avg_score
        FROM student_scores
        GROUP BY student_id, student_name
        HAVING py_score_grade(AVG(score)) IN ('A', 'B')
        ORDER BY student_id;
        """
        
    } finally {
        try_sql("DROP FUNCTION IF EXISTS py_score_grade(DOUBLE);")
        try_sql("DROP FUNCTION IF EXISTS py_age_group(INT);")
        try_sql("DROP TABLE IF EXISTS student_scores;")
        try_sql("DROP TABLE IF EXISTS users;")
    }
}
