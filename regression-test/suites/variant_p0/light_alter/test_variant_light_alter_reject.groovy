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

suite("test_variant_light_alter_reject", "p0") {
    sql "DROP TABLE IF EXISTS variant_light_alter_reject"
    sql """CREATE TABLE variant_light_alter_reject (
        id INT, v VARIANT<'a':INT, 'c':STRING> NOT NULL COMMENT '', tail INT,
        INDEX idx_a(v) USING INVERTED PROPERTIES('field_pattern'='a')
    ) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
    PROPERTIES("replication_num"="1")"""
    test {
        sql "ALTER TABLE variant_light_alter_reject MODIFY COLUMN v VARIANT<'a':INT>"
        exception "Can only append variant schema templates"
    }
    test {
        sql "ALTER TABLE variant_light_alter_reject MODIFY COLUMN v VARIANT<'c':STRING, 'a':INT>"
        exception "Can only append variant schema templates"
    }
    test {
        sql "ALTER TABLE variant_light_alter_reject MODIFY COLUMN v VARIANT<'a':BIGINT, 'c':STRING>"
        exception "Can only append variant schema templates"
    }
    test {
        sql "ALTER TABLE variant_light_alter_reject MODIFY COLUMN v VARIANT<MATCH_NAME 'a':INT, 'c':STRING>"
        exception "Can only append variant schema templates"
    }
    test {
        sql """ALTER TABLE variant_light_alter_reject MODIFY COLUMN v
            VARIANT<'a':INT, 'c':STRING, PROPERTIES("variant_max_subcolumns_count"="1")> NULL"""
        exception "Variant property changes must be isolated"
    }
    test {
        sql """ALTER TABLE variant_light_alter_reject MODIFY COLUMN v
            VARIANT<'a':INT, 'c':STRING, PROPERTIES("variant_max_subcolumns_count"="1")> NOT NULL COMMENT 'changed'"""
        exception "Variant property changes must be isolated"
    }
    test {
        sql """ALTER TABLE variant_light_alter_reject MODIFY COLUMN v
            VARIANT<'a':INT, 'c':STRING, PROPERTIES("variant_max_subcolumns_count"="1")> NOT NULL AFTER tail"""
        exception "Variant property changes must be isolated"
    }
    test {
        sql "BUILD INDEX idx_a ON variant_light_alter_reject"
        exception "because it is a variant type column"
    }
    sql "DROP TABLE IF EXISTS variant_light_reject_doc"
    sql """CREATE TABLE variant_light_reject_doc (
        id INT, v VARIANT<PROPERTIES("variant_enable_doc_mode"="true")>
    ) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
    PROPERTIES("replication_num"="1")"""
    test {
        sql """ALTER TABLE variant_light_reject_doc MODIFY COLUMN v
            VARIANT<'a':INT, PROPERTIES("variant_enable_doc_mode"="true")>"""
        exception "with doc mode, nested group or typed paths to sparse enabled"
    }
    test {
        sql """ALTER TABLE variant_light_reject_doc MODIFY COLUMN v
            VARIANT<PROPERTIES("variant_enable_doc_mode"="true", "variant_max_subcolumns_count"="10")>"""
        exception "variant_max_subcolumns_count and variant_enable_doc_mode cannot be set together"
    }
    sql "DROP TABLE IF EXISTS variant_light_reject_nested"
    sql """CREATE TABLE variant_light_reject_nested (
        id INT, v VARIANT<PROPERTIES("variant_enable_nested_group"="true", "variant_max_subcolumns_count"="0")>
    ) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
    PROPERTIES("replication_num"="1")"""
    test {
        sql """ALTER TABLE variant_light_reject_nested MODIFY COLUMN v
            VARIANT<'a':INT, PROPERTIES("variant_enable_nested_group"="true", "variant_max_subcolumns_count"="0")>"""
        exception "with doc mode, nested group or typed paths to sparse enabled"
    }
    test {
        sql """ALTER TABLE variant_light_reject_nested MODIFY COLUMN v
            VARIANT<PROPERTIES("variant_enable_nested_group"="true", "variant_max_subcolumns_count"="10")>"""
        exception "variant_enable_nested_group cannot be true when variant_max_subcolumns_count > 0"
    }
    sql "DROP TABLE IF EXISTS variant_light_reject_rowstore"
    sql """CREATE TABLE variant_light_reject_rowstore (id INT, v VARIANT)
        DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num"="1", "store_row_column"="true")"""
    test {
        sql "ALTER TABLE variant_light_reject_rowstore MODIFY COLUMN v VARIANT<'a':INT>"
        exception "Can not change variant schema templates on a row-store table"
    }
}
