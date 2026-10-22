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

// SEARCH must retain its input columns while NOT IN is expanded inside a CTE.
suite("test_search_cte_not_in", "p0") {
    // Keep the VARIANT storage mode used by the reported query.
    sql "set default_variant_enable_doc_mode = false"
    sql "DROP TABLE IF EXISTS search_cte_jobs"
    sql "DROP TABLE IF EXISTS search_cte_assignments"
    sql """
        CREATE TABLE search_cte_jobs (
            job_id INT NOT NULL,
            account_id INT,
            default_field_json VARIANT<PROPERTIES("variant_enable_typed_paths_to_sparse"="false")>,
            INDEX idx_fields(default_field_json) USING INVERTED
                PROPERTIES("parser"="unicode", "lower_case"="true", "support_phrase"="true")
        ) UNIQUE KEY(job_id)
        DISTRIBUTED BY HASH(job_id) BUCKETS 3
        PROPERTIES("replication_num"="1", "enable_unique_key_merge_on_write"="true")
    """
    sql """
        CREATE TABLE search_cte_assignments (
            id INT NOT NULL,
            jobid INT NULL,
            candidateid INT,
            accountid INT
        ) UNIQUE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 3
        PROPERTIES("replication_num"="1", "enable_unique_key_merge_on_write"="true")
    """
    sql """INSERT INTO search_cte_jobs VALUES
        (1, 352119, parse_to_variant('{"jobstatus": 1, "archived": 0, "deleted": 0, "createdon": 1781123912, "ownerid": 1311610, "minexperienceinyears": 0, "maxexperienceinyears": 0, "remote": "1", "locality": "", "city": "", "state": "", "address": "", "country": "", "collaborators": ""}')),
        (2, 352119, parse_to_variant('{"jobstatus": 1, "archived": 0, "deleted": 0, "createdon": 1781123912, "ownerid": 999, "minexperienceinyears": 0, "maxexperienceinyears": 0, "remote": "0", "locality": "", "city": "Pune", "state": "", "address": "", "country": "", "collaborators": "u3447699"}')),
        (3, 352119, parse_to_variant('{"jobstatus": 1, "archived": 0, "deleted": 0, "createdon": 1781123912, "ownerid": 999, "minexperienceinyears": 0, "maxexperienceinyears": 0, "remote": "0", "locality": "", "city": "", "state": "", "address": "", "country": "", "collaborators": "t41338"}')),
        (4, 352119, parse_to_variant('{"jobstatus": 1, "archived": 0, "deleted": 0, "createdon": 1781123912, "ownerid": 999, "minexperienceinyears": 0, "maxexperienceinyears": 0, "remote": "1", "locality": "", "city": "", "state": "", "address": "", "country": "", "collaborators": "other"}')),
        (5, 352119, parse_to_variant('{"jobstatus": 1, "archived": 0, "deleted": 0, "createdon": 1781123912, "ownerid": 1311610, "minexperienceinyears": 0, "maxexperienceinyears": 0, "remote": "1", "locality": "", "city": "", "state": "", "address": "", "country": "", "collaborators": ""}')),
        (6, 352119, parse_to_variant('{"jobstatus": 1, "archived": 0, "deleted": 1, "createdon": 1781123912, "ownerid": 1311610, "minexperienceinyears": 0, "maxexperienceinyears": 0, "remote": "1", "locality": "", "city": "", "state": "", "address": "", "country": "", "collaborators": ""}')),
        (7, 352119, parse_to_variant('{"jobstatus": 1, "archived": 1, "deleted": 0, "createdon": 1781123912, "ownerid": 1311610, "minexperienceinyears": 0, "maxexperienceinyears": 0, "remote": "1", "locality": "", "city": "", "state": "", "address": "", "country": "", "collaborators": ""}')),
        (8, 352119, parse_to_variant('{"jobstatus": 0, "archived": 0, "deleted": 0, "createdon": 1781123912, "ownerid": 1311610, "minexperienceinyears": 0, "maxexperienceinyears": 0, "remote": "1", "locality": "", "city": "", "state": "", "address": "", "country": "", "collaborators": ""}')),
        (9, 352120, parse_to_variant('{"jobstatus": 1, "archived": 0, "deleted": 0, "createdon": 1781123912, "ownerid": 1311610, "minexperienceinyears": 0, "maxexperienceinyears": 0, "remote": "1", "locality": "", "city": "", "state": "", "address": "", "country": "", "collaborators": ""}')),
        (10, 352119, parse_to_variant('{"jobstatus": 1, "archived": 0, "deleted": 0, "createdon": 1781123911, "ownerid": 1311610, "minexperienceinyears": 0, "maxexperienceinyears": 0, "remote": "1", "locality": "", "city": "", "state": "", "address": "", "country": "", "collaborators": ""}')),
        (11, 352119, parse_to_variant('{"jobstatus": 1, "archived": 0, "deleted": 0, "createdon": 1781123912, "ownerid": 1311610, "minexperienceinyears": 14, "maxexperienceinyears": 0, "remote": "1", "locality": "", "city": "", "state": "", "address": "", "country": "", "collaborators": ""}')),
        (12, 352119, parse_to_variant('{"jobstatus": 1, "archived": 0, "deleted": 0, "createdon": 1781123912, "ownerid": 1311610, "minexperienceinyears": 0, "maxexperienceinyears": 3, "remote": "1", "locality": "", "city": "", "state": "", "address": "", "country": "", "collaborators": ""}')),
        (13, 352119, parse_to_variant('{"jobstatus": 1, "archived": 0, "deleted": 0, "createdon": 1781123912, "ownerid": 1311610, "minexperienceinyears": 0, "maxexperienceinyears": 0, "remote": "0", "locality": "", "city": "Mumbai", "state": "", "address": "", "country": "US", "collaborators": ""}')),
        (14, 352119, parse_to_variant('{"jobstatus": 2, "archived": 0, "deleted": 0, "createdon": 1781123912, "ownerid": 1311610, "minexperienceinyears": 0, "maxexperienceinyears": 0, "remote": "1", "locality": "", "city": "", "state": "", "address": "", "country": "", "collaborators": ""}')),
        (15, 352119, parse_to_variant('{"jobstatus": 1, "archived": 0, "deleted": 0, "createdon": 1781123912, "ownerid": 1311610, "minexperienceinyears": 0, "maxexperienceinyears": 0, "remote": "1", "locality": "", "city": "", "state": "", "address": "", "country": "", "collaborators": ""}')),
        (16, 352119, parse_to_variant('{"jobstatus": 1, "archived": 0, "deleted": 0, "createdon": 1781123912, "ownerid": 3447699, "minexperienceinyears": 0, "maxexperienceinyears": 0, "remote": "0", "locality": "", "city": "", "state": "", "address": "Republic of India", "country": "", "collaborators": ""}'))
    """
    sql """INSERT INTO search_cte_assignments VALUES
        (1, 5, 58833859, 352119),
        (2, 1, 999, 352119),
        (3, 2, 58833859, 999)
    """
    sql "sync"

    // Keep the original query to validate FE planning. SEARCH result correctness for sparse
    // VARIANT subcolumns depends on a separate BE fix. The rows exercise
    // owner/collaborator, location, account, status, time and experience filtering.
    def query = """
WITH eligible AS (
    SELECT u.job_id
    FROM search_cte_jobs u
    WHERE u.account_id = 352119
      AND CAST(u.default_field_json['jobstatus'] AS INT) NOT IN (0, 2)
      AND CAST(u.default_field_json['archived'] AS INT) = 0
      AND CAST(u.default_field_json['deleted'] AS INT) = 0
      AND CAST(u.default_field_json['createdon'] AS INT) >= 1781123912      AND (
          CAST(u.default_field_json['ownerid'] AS INT) IN (1311610, 3447699)
          OR CAST(u.default_field_json['ownerid'] AS INT) = 3447699
          OR
           SEARCH('(default_field_json.collaborators:ANY(u3447699 t41338))')
      )      AND u.job_id NOT IN (
          SELECT jobid
          FROM search_cte_assignments
          WHERE candidateid = 58833859
            AND accountid = 352119
      )      AND (
          (
              COALESCE(
                  CAST(u.default_field_json['minexperienceinyears'] AS INT), 0
              ) = 0
              OR CAST(u.default_field_json['minexperienceinyears'] AS INT) <= 13.0
          )
          AND (
              COALESCE(
                  CAST(u.default_field_json['maxexperienceinyears'] AS INT), 0
              ) = 0
              OR CAST(u.default_field_json['maxexperienceinyears'] AS INT) >= 4.0
          )
      )      AND (
          CAST(u.default_field_json['remote'] AS STRING) = '1'          OR  SEARCH('(default_field_json.locality:ANY(Pune PN Pune "Pune City")
                    OR default_field_json.city:ANY(Pune PN Pune "Pune City")
                    OR default_field_json.state:ANY(Pune PN Maharashtra "Pune City")
                    OR default_field_json.address:ANY("Republic of India" "Pune City" Pune Maharashtra India Bharat)
                     OR default_field_json.country:ANY("Republic of India" India IN Bharat India)
                      )')
          OR CONCAT(
              COALESCE(
                  CAST(u.default_field_json['locality'] AS STRING), ''
              ),
              COALESCE(
                  CAST(u.default_field_json['city'] AS STRING), ''
              ),
              COALESCE(
                  CAST(u.default_field_json['state'] AS STRING), ''
              ),
              COALESCE(
                  CAST(u.default_field_json['address'] AS STRING), ''
              ),
              COALESCE(
                  CAST(u.default_field_json['country'] AS STRING), ''
              )
          ) = ''
      )
      )
    SELECT job_id, 0 AS w
    FROM eligible ORDER BY job_id
    """
    def checkSearchPlan = { String statement ->
        explain {
            sql(statement)
            contains "VOlapScanNode"
            contains "search_cte_jobs"
            contains "search_cte_assignments"
            contains "search("
            contains "sub path: [collaborators]"
        }
    }
    sql "set inline_cte_referenced_threshold = 1"
    checkSearchPlan(query)
    sql "set inline_cte_referenced_threshold = 0"
    checkSearchPlan(query)

    // Multiple consumers must preserve the producer's SEARCH dependencies.
    checkSearchPlan """
        WITH eligible AS (
            SELECT job_id FROM search_cte_jobs
            WHERE SEARCH('default_field_json.collaborators:ANY(u3447699 t41338)')
                AND job_id NOT IN (SELECT jobid FROM search_cte_assignments
                    WHERE candidateid = 58833859 AND accountid = 352119)
        )
        SELECT job_id FROM eligible UNION ALL SELECT job_id FROM eligible ORDER BY job_id
    """

    // A standalone SEARCH must filter only the jobs side, not the assignment side.
    checkSearchPlan """
        SELECT j.job_id FROM search_cte_jobs j JOIN search_cte_assignments a ON j.job_id = a.jobid
        WHERE SEARCH('default_field_json.collaborators:u3447699') ORDER BY j.job_id
    """

    // An OR involving both inputs cannot be evaluated as a scan filter.
    test {
        sql """EXPLAIN SELECT j.job_id FROM search_cte_jobs j JOIN search_cte_assignments a ON j.job_id = a.jobid
            WHERE SEARCH('default_field_json.collaborators:u3447699') OR a.candidateid = 999"""
        exception "search() is only supported in filters directly on an OLAP table scan"
    }
    test {
        sql """EXPLAIN SELECT * FROM (SELECT * FROM search_cte_jobs ORDER BY job_id LIMIT 2) j
            WHERE SEARCH('default_field_json.collaborators:u3447699')"""
        exception "search() is only supported in filters directly on an OLAP table scan"
    }
    test {
        sql "EXPLAIN SELECT SEARCH('default_field_json.collaborators:u3447699') FROM search_cte_jobs"
        exception "search() is only supported in WHERE filters"
    }
}
