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

suite("iceberg_schema_change_ddl_with_branch", "p0,external,doris,external_docker,external_docker_doris") {

    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test.")
        return
    }


    String rest_port = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalog_name = "iceberg_schema_change_ddl_with_branch"

    sql """drop catalog if exists ${catalog_name}"""
    sql """
    CREATE CATALOG ${catalog_name} PROPERTIES (
        'type'='iceberg',
        'iceberg.catalog.type'='rest',
        'uri' = 'http://${externalEnvIp}:${rest_port}',
        "s3.access_key" = "admin",
        "s3.secret_key" = "password",
        "s3.endpoint" = "http://${externalEnvIp}:${minio_port}",
        "s3.region" = "us-east-1"
    );"""

    sql """switch ${catalog_name};"""
    sql """drop database if exists iceberg_schema_change_ddl_with_branch_db force"""
    sql """create database iceberg_schema_change_ddl_with_branch_db"""
    sql """ use iceberg_schema_change_ddl_with_branch_db;""" 

    sql """ set enable_fallback_to_original_planner=false; """

    // ==================================================================================
    // Test schema change isolation between different branches and tags
    // ==================================================================================
    
    String branch_table_name = "iceberg_branch_schema_test"
    sql """ drop table if exists ${branch_table_name} """
    
    // Create a test table for branch schema change testing
    sql """
    CREATE TABLE ${branch_table_name} (
        id INT,
        name STRING,
        age INT,
        score DOUBLE
    );
    """
    
    // Step 1: Insert initial data and create branch1 & tag1
    sql """
    INSERT INTO ${branch_table_name} VALUES 
    (1, 'Alice', 25, 95.5),
    (2, 'Bob', 30, 87.2),
    (3, 'Charlie', 22, 92.8)
    """
    
    // Verify initial state on main branch
    qt_initial_state_1 """ DESC ${branch_table_name} """
    qt_initial_state_2 """ SELECT * FROM ${branch_table_name} ORDER BY id """
    
    // Create branch1 and tag1 after initial data (4 columns: id, name, age, score)
    sql """ ALTER TABLE ${branch_table_name} CREATE BRANCH branch1 """
    sql """ ALTER TABLE ${branch_table_name} CREATE TAG tag1 """
    
    // Verify branch1 and tag1 have same schema and data as main at this point
    qt_branch1_initial """ SELECT * FROM ${branch_table_name}@branch(branch1) ORDER BY id """
    qt_tag1_initial """ SELECT * FROM ${branch_table_name}@tag(tag1) ORDER BY id """
    
    // Step 2: Add email column to main branch and insert more data
    sql """ ALTER TABLE ${branch_table_name} ADD COLUMN email STRING """
    sql """ INSERT INTO ${branch_table_name} VALUES (4, 'David', 28, 89.1, 'david@example.com') """
    
    // Main branch now has 5 columns: id, name, age, score, email
    qt_main_with_email_1 """ DESC ${branch_table_name} """
    qt_main_with_email_2 """ SELECT * FROM ${branch_table_name} ORDER BY id """
    
    // Create branch2 and tag2 after adding email column
    sql """ ALTER TABLE ${branch_table_name} CREATE BRANCH branch2 """
    sql """ ALTER TABLE ${branch_table_name} CREATE TAG tag2 """
    
    // Verify branch2 and tag2 have email column
    qt_branch2_with_email """ SELECT * FROM ${branch_table_name}@branch(branch2) ORDER BY id """
    qt_tag2_with_email """ SELECT * FROM ${branch_table_name}@tag(tag2) ORDER BY id """
    
    // Step 3: Drop age column from main branch and insert more data
    sql """ ALTER TABLE ${branch_table_name} DROP COLUMN age """
    sql """ INSERT INTO ${branch_table_name} VALUES (5, 'Eve', 91.3, 'eve@example.com') """
    
    // Main branch now has 4 columns: id, name, score, email (no age)
    qt_main_no_age_1 """ DESC ${branch_table_name} """
    qt_main_no_age_2 """ SELECT * FROM ${branch_table_name} ORDER BY id """
    
    // Create branch3 and tag3 after dropping age column
    sql """ ALTER TABLE ${branch_table_name} CREATE BRANCH branch3 """
    sql """ ALTER TABLE ${branch_table_name} CREATE TAG tag3 """
    
    // Verify branch3 and tag3 don't have age column
    qt_branch3_no_age """ SELECT * FROM ${branch_table_name}@branch(branch3) ORDER BY id """
    qt_tag3_no_age """ SELECT * FROM ${branch_table_name}@tag(tag3) ORDER BY id """
    
    // Step 4: Rename score to grade in main branch and insert more data
    sql """ ALTER TABLE ${branch_table_name} RENAME COLUMN score grade """
    sql """ INSERT INTO ${branch_table_name} VALUES (6, 'Frank', 88.7, 'frank@example.com') """
    
    // Main branch now has 4 columns: id, name, grade, email (score renamed to grade)
    qt_main_with_grade_1 """ DESC ${branch_table_name} """
    qt_main_with_grade_2 """ SELECT * FROM ${branch_table_name} ORDER BY id """
    
    // Create branch4 and tag4 after renaming score to grade
    sql """ ALTER TABLE ${branch_table_name} CREATE BRANCH branch4 """
    sql """ ALTER TABLE ${branch_table_name} CREATE TAG tag4 """
    
    // Verify branch4 and tag4 have grade instead of score
    qt_branch4_with_grade """ SELECT * FROM ${branch_table_name}@branch(branch4) ORDER BY id """
    qt_tag4_with_grade """ SELECT * FROM ${branch_table_name}@tag(tag4) ORDER BY id """
    
    // Step 5: Add phone column to main branch and insert final data
    sql """ ALTER TABLE ${branch_table_name} ADD COLUMN phone STRING """
    sql """ INSERT INTO ${branch_table_name} VALUES (7, 'Grace', 93.2, 'grace@example.com', '555-0123') """
    
    // Main branch now has 5 columns: id, name, grade, email, phone
    qt_main_final_1 """ DESC ${branch_table_name} """
    qt_main_final_2 """ SELECT * FROM ${branch_table_name} ORDER BY id """
    
    // Create final branch5 and tag5
    sql """ ALTER TABLE ${branch_table_name} CREATE BRANCH branch5 """
    sql """ ALTER TABLE ${branch_table_name} CREATE TAG tag5 """
    
    // Verify branch5 and tag5 have all columns including phone
    qt_branch5_final """ SELECT * FROM ${branch_table_name}@branch(branch5) ORDER BY id """
    qt_tag5_final """ SELECT * FROM ${branch_table_name}@tag(tag5) ORDER BY id """
    
    // ==================================================================================
    // Verify schema behavior: branches get latest schema, tags keep creation-time schema
    // ==================================================================================
    
    // Test specific column queries to verify schema differences
    
    // IMPORTANT: Branches will use the LATEST schema from main branch
    // Tags will use the schema from when they were created
    
    // branch1: should use LATEST schema (same as main) - id, name, grade, email, phone
    qt_branch1_latest_schema """ SELECT * FROM ${branch_table_name}@branch(branch1) ORDER BY id """
    
    // tag1: should have ORIGINAL schema when created - id, name, age, score (no email, phone, grade)
    qt_tag1_original_schema """ SELECT * FROM ${branch_table_name}@tag(tag1) ORDER BY id """
    qt_tag1_age_score """ SELECT id, age, score FROM ${branch_table_name}@tag(tag1) ORDER BY id """
    
    // branch2: should use LATEST schema (same as main) - id, name, grade, email, phone
    qt_branch2_latest_schema """ SELECT * FROM ${branch_table_name}@branch(branch2) ORDER BY id """
    
    // tag2: should have schema when created - id, name, age, score, email (no phone, grade)
    qt_tag2_creation_schema """ SELECT * FROM ${branch_table_name}@tag(tag2) ORDER BY id """
    qt_tag2_age_email """ SELECT id, age, score, email FROM ${branch_table_name}@tag(tag2) ORDER BY id """
    
    // branch3: should use LATEST schema (same as main) - id, name, grade, email, phone
    qt_branch3_latest_schema """ SELECT * FROM ${branch_table_name}@branch(branch3) ORDER BY id """
    
    // tag3: should have schema when created - id, name, score, email (no age, phone, grade)
    qt_tag3_creation_schema """ SELECT * FROM ${branch_table_name}@tag(tag3) ORDER BY id """
    qt_tag3_score_email """ SELECT id, score, email FROM ${branch_table_name}@tag(tag3) ORDER BY id """
    
    // branch4: should use LATEST schema (same as main) - id, name, grade, email, phone
    qt_branch4_latest_schema """ SELECT * FROM ${branch_table_name}@branch(branch4) ORDER BY id """
    
    // tag4: should have schema when created - id, name, grade, email (no age, score, phone)
    qt_tag4_creation_schema """ SELECT * FROM ${branch_table_name}@tag(tag4) ORDER BY id """
    qt_tag4_grade_email """ SELECT id, grade, email FROM ${branch_table_name}@tag(tag4) ORDER BY id """
    
    // branch5: should use LATEST schema (same as main) - id, name, grade, email, phone
    qt_branch5_latest_schema """ SELECT * FROM ${branch_table_name}@branch(branch5) ORDER BY id """
    
    // tag5: should have schema when created - id, name, grade, email, phone
    qt_tag5_creation_schema """ SELECT * FROM ${branch_table_name}@tag(tag5) ORDER BY id """
    qt_tag5_all_cols """ SELECT id, grade, email, phone FROM ${branch_table_name}@tag(tag5) ORDER BY id """
    
    // ==================================================================================
    // Negative tests: verify schema behavior differences between branches and tags
    // ==================================================================================
    
    // ALL BRANCHES should have the LATEST schema (same as main)
    // So all branches should have: id, name, grade, email, phone
    
    // Verify all branches have the latest columns
    qt_all_branches_have_grade """ SELECT id, grade FROM ${branch_table_name}@branch(branch1) WHERE grade > 0 ORDER BY id """
    qt_all_branches_have_email """ SELECT id, email FROM ${branch_table_name}@branch(branch2) WHERE email IS NOT NULL ORDER BY id """
    qt_all_branches_have_phone """ SELECT id, phone FROM ${branch_table_name}@branch(branch3) WHERE phone IS NOT NULL ORDER BY id """
    
    // All branches should NOT have old columns that were dropped/renamed
    test {
        sql """ SELECT age FROM ${branch_table_name}@branch(branch1) """
        exception "Unknown column 'age'"
    }
    test {
        sql """ SELECT score FROM ${branch_table_name}@branch(branch2) """
        exception "Unknown column 'score'"
    }
    
    // TAGS should have their CREATION-TIME schema
    
    // tag1 should not have email, phone, or grade (only has age, score)
    test {
        sql """ SELECT email FROM ${branch_table_name}@tag(tag1) """
        exception "Unknown column 'email'"
    }
    test {
        sql """ SELECT phone FROM ${branch_table_name}@tag(tag1) """
        exception "Unknown column 'phone'"
    }
    test {
        sql """ SELECT grade FROM ${branch_table_name}@tag(tag1) """
        exception "Unknown column 'grade'"
    }
    
    // tag2 should not have phone or grade (has age, score, email)
    test {
        sql """ SELECT phone FROM ${branch_table_name}@tag(tag2) """
        exception "Unknown column 'phone'"
    }
    test {
        sql """ SELECT grade FROM ${branch_table_name}@tag(tag2) """
        exception "Unknown column 'grade'"
    }
    
    // tag3 should not have age, phone, or grade (has score, email)
    test {
        sql """ SELECT age FROM ${branch_table_name}@tag(tag3) """
        exception "Unknown column 'age'"
    }
    test {
        sql """ SELECT phone FROM ${branch_table_name}@tag(tag3) """
        exception "Unknown column 'phone'"
    }
    test {
        sql """ SELECT grade FROM ${branch_table_name}@tag(tag3) """
        exception "Unknown column 'grade'"
    }
    
    // tag4 should not have age, score, or phone (has grade, email)
    test {
        sql """ SELECT age FROM ${branch_table_name}@tag(tag4) """
        exception "Unknown column 'age'"
    }
    test {
        sql """ SELECT score FROM ${branch_table_name}@tag(tag4) """
        exception "Unknown column 'score'"
    }
    test {
        sql """ SELECT phone FROM ${branch_table_name}@tag(tag4) """
        exception "Unknown column 'phone'"
    }
    
    // ==================================================================================
    // Summary: Final verification showing branch vs tag schema behavior
    // ==================================================================================
    
    // Main branch has the latest schema
    qt_summary_main """ SELECT * FROM ${branch_table_name} ORDER BY id """
    
    // ALL BRANCHES use the LATEST schema (same as main)
    qt_summary_branch1 """ SELECT * FROM ${branch_table_name}@branch(branch1) ORDER BY id """
    qt_summary_branch2 """ SELECT * FROM ${branch_table_name}@branch(branch2) ORDER BY id """
    qt_summary_branch3 """ SELECT * FROM ${branch_table_name}@branch(branch3) ORDER BY id """
    qt_summary_branch4 """ SELECT * FROM ${branch_table_name}@branch(branch4) ORDER BY id """
    qt_summary_branch5 """ SELECT * FROM ${branch_table_name}@branch(branch5) ORDER BY id """
    
    // TAGS use their CREATION-TIME schema
    qt_summary_tag1 """ SELECT * FROM ${branch_table_name}@tag(tag1) ORDER BY id """      // Original: id, name, age, score
    qt_summary_tag2 """ SELECT * FROM ${branch_table_name}@tag(tag2) ORDER BY id """      // With email: id, name, age, score, email  
    qt_summary_tag3 """ SELECT * FROM ${branch_table_name}@tag(tag3) ORDER BY id """      // No age: id, name, score, email
    qt_summary_tag4 """ SELECT * FROM ${branch_table_name}@tag(tag4) ORDER BY id """      // Renamed: id, name, grade, email
    qt_summary_tag5 """ SELECT * FROM ${branch_table_name}@tag(tag5) ORDER BY id """      // Final: id, name, grade, email, phone
}
