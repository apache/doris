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

// The cases is copied from https://github.com/trinodb/trino/tree/master
// /testing/trino-product-tests/src/main/resources/sql-tests/testcases/tpcds
// and modified by Doris.


/**
 *
 * Load data use restore. We should use it as long as it works.
 *
 */
suite("load") {
    restore {
        location "s3://${getS3BucketName()}/regression_backup/clickhouse/github_events"
        ak "${getS3AK()}"
        sk "${getS3SK()}"
        endpoint "http://${getS3Endpoint()}"
        region "ap-beijing"
        repository "regression_test_github_events"
        snapshot "github_events"
        timestamp "2022-03-23-12-19-51"
        replicationNum 1
        timeout 72000
    }
    sql "sync"
    sql """ ANALYZE TABLE github_events """;
    qt_sql_select_count """ select count(*) from github_events; """
}
/**
 *
 * load data use stream load. If we cannot load data from restore, we could fallback to this.
 * Notion, it will run a few hours and maybe fail.
 *
 */
// def tableName = "github_events"
// def filePrefix = "github_events_v2_"
// def fileSuffix = ".tsv.gz"
// def delimiter = "_"
// def starSplit = 0
// def endSplit = 311
// def splitNum = 311
// def splitWide = 4
// def subSplitWide = 2
// def subSplitNumMap = [
//     99 : 10,
//     100 : 10,
//     101 : 10,
//     102 : 10,
//     103 : 10,
//     104 : 10,
//     105 : 10,
//     106 : 10,
//     107 : 10,
//     108 : 10,
//     109 : 10,
//     110 : 10,
//     111 : 10,
//     112 : 10,
//     113 : 10,
//     114 : 10,
//     115 : 10,
//     116 : 10,
//     117 : 10,
//     118 : 10,
//     119 : 10,
//     120 : 10,
//     121 : 10,
//     122 : 10,
//     123 : 10,
//     124 : 10,
//     125 : 10
// ]
// 
// if (starSplit == 0) {
//     sql """ DROP TABLE IF EXISTS ${tableName} """
//     sql new File("""${context.file.parent}/ddl/${tableName}.sql""").text
// }
// 
// for (int i = starSplit; i <= Math.min(endSplit, splitNum); i++) {
//     int subSplitNum = subSplitNumMap.get(i, 1)
//     for (int j = 0; j < subSplitNum; j++) {
// 
//         // assemble file name, file format is :
//         // no subsplit: github_events_v2_${split_number_with_4_char_wide}.tsv.gz
//         // have subsplit: github_events_v2_${split_number_with_4_char_wide}_${sub_split_number_with_2_char_wide}.tsv.gz
//         def fileName = filePrefix + "${i}".padLeft(splitWide, '0')
//         if (subSplitNum != 1) {
//             fileName = fileName + delimiter + "${j}".padLeft(subSplitWide, '0')
//         }
//         fileName = fileName + fileSuffix;
// 
//         // generate label to do failover
//         def label = UUID.randomUUID().toString()
// 
//         // do stream load
//         streamLoadWithRetry {
//             table tableName
//             retryNum = 3
//             sleepMs = 2000
// 
//             set 'label', label
//             set 'column_separator', '\t'
//             set 'compress_type', 'GZ'
//             set 'columns', 'file_time, event_type, actor_login, repo_name, created_at, updated_at, action, comment_id, body, path, position, line, ref, ref_type, creator_user_login, number, title, labels, state, locked, assignee, assignees, comments, author_association, closed_at, merged_at, merge_commit_sha, requested_reviewers, requested_teams, head_ref, head_sha, base_ref, base_sha, merged, mergeable, rebaseable, mergeable_state, merged_by, review_comments, maintainer_can_modify, commits, additions, deletions, changed_files, diff_hunk, original_position, commit_id, original_commit_id, push_size, push_distinct_size, member_login, release_tag_name, release_name, review_state'
// 
//             file """${context.config.s3Url}/regression/clickhouse/github_events/${fileName}"""
// 
//             time 0
// 
//             // stream load action will check result, include Success status, and NumberTotalRows == NumberLoadedRows
// 
//             // if declared a check callback, the default check condition will ignore.
//             // So you must check all condition
//             check { result, exception, startTime, endTime ->
//                 if (exception != null) {
//                     throw exception
//                 }
//                 log.info("Stream load result: ${result}".toString())
//                 def json = parseJson(result)
//                 assertEquals("success", json.Status.toLowerCase())
//                 assertEquals(json.NumberTotalRows, json.NumberLoadedRows)
//                 assertTrue(json.NumberLoadedRows > 0 && json.LoadBytes > 0)
//             }
//         }
//     }
// }
