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

suite("regression_test_variant", "variant_type"){
    // prepare test table
    def create_table = { table_name ->
        sql "DROP TABLE IF EXISTS ${table_name}"
        sql """
            CREATE TABLE IF NOT EXISTS ${table_name} (
                k bigint,
                v variant
            )
            DUPLICATE KEY(`k`)
            DISTRIBUTED BY RANDOM BUCKETS 5 
            properties("replication_num" = "1", "disable_auto_compaction" = "false");
        """
    }
    def verify = { table_name ->
        sql "sync"
        qt_sql """desc ${table_name}"""
        qt_sql """select count() from ${table_name}"""
    }

    def table_name = "simple_variant"
    create_table table_name
    sql """insert into ${table_name} values (1,  '[1]'),(1,  '{"a" : 1}');"""
    sql """insert into ${table_name} values (2,  '[2]'),(1,  '{"a" : [[[1]]]}');"""
    sql """insert into ${table_name} values (3,  '3'),(1,  '{"a" : 1}'), (1,  '{"a" : [1]}');"""
    sql """insert into ${table_name} values (4,  '"4"'),(1,  '{"a" : "1223"}');"""
    sql """insert into ${table_name} values (5,  '5.0'),(1,  '{"a" : [1]}');"""
    sql """insert into ${table_name} values (6,  '"[6]"'),(1,  '{"a" : ["1", 2, 1.1]}');"""
    sql """insert into ${table_name} values (7,  '7'),(1,  '{"a" : 1, "b" : {"c" : 1}}');"""
    sql """insert into ${table_name} values (8,  '8.11111'),(1,  '{"a" : 1, "b" : {"c" : [{"a" : 1}]}}');"""
    sql """insert into ${table_name} values (9,  '"9999"'),(1,  '{"a" : 1, "b" : {"c" : [{"a" : 1}]}}');"""
    sql """insert into ${table_name} values (10,  '1000000'),(1,  '{"a" : 1, "b" : {"c" : [{"a" : 1}]}}');"""
    sql """insert into ${table_name} values (11,  '[123.0]'),(1,  '{"a" : 1, "b" : {"c" : 1}}'),(1,  '{"a" : 1, "b" : 10}');"""
    sql """insert into ${table_name} values (12,  '[123.2]'),(1,  '{"a" : 1, "b" : 10}'),(1,  '{"a" : 1, "b" : {"c" : 1}}');"""
    sql """insert into ${table_name} values (13, '{"id":"25060169353","type":"PushEvent","actor":{"id":66998102,"login":"yessine09","display_login":"yessine09","gravatar_id":"","url":"https://api.github.com/users/yessine09","avatar_url":"https://avatars.githubusercontent.com/u/66998102?"},"repo":{"id":558113479,"name":"yessine09/tpAchat","url":"https://api.github.com/repos/yessine09/tpAchat"},"payload":{"push_id":11571720453,"size":1,"distinct_size":1,"ref":"refs/heads/yessine","head":"c6e9675024be85f488a40569ca2d7d5a41d632d4","before":"21d50347d2d11e43e43446584a5d2c14561bd0a2","commits":[{"sha":"c6e9675024be85f488a40569ca2d7d5a41d632d4","author":{"email":"yessine.akaichi@esprit.tn","name":"yessine09"},"message":"test commit","distinct":true,"url":"https://api.github.com/repos/yessine09/tpAchat/commits/c6e9675024be85f488a40569ca2d7d5a41d632d4"}]},"public":true,"created_at":"2022-11-07T00:00:00Z"}'), (2, '{"id":"25060169349","type":"ForkEvent","actor":{"id":104698059,"login":"irina-marzioni-tdf","display_login":"irina-marzioni-tdf","gravatar_id":"","url":"https://api.github.com/users/irina-marzioni-tdf","avatar_url":"https://avatars.githubusercontent.com/u/104698059?"},"repo":{"id":557482773,"name":"dddario-TDF-EDU/ejercicios-2022-10-19-Trabajo-en-grupo","url":"https://api.github.com/repos/dddario-TDF-EDU/ejercicios-2022-10-19-Trabajo-en-grupo"},"payload":{"forkee":{"id":562642534,"node_id":"R_kgDOIYk-Zg","name":"ejercicios-2022-10-19-Trabajo-en-grupo","full_name":"irina-marzioni-tdf/ejercicios-2022-10-19-Trabajo-en-grupo","private":false,"owner":{"login":"irina-marzioni-tdf","id":104698059,"node_id":"U_kgDOBj2Qyw","avatar_url":"https://avatars.githubusercontent.com/u/104698059?v=4","gravatar_id":"","url":"https://api.github.com/users/irina-marzioni-tdf","html_url":"https://github.com/irina-marzioni-tdf","followers_url":"https://api.github.com/users/irina-marzioni-tdf/followers","following_url":"https://api.github.com/users/irina-marzioni-tdf/following{/other_user}","gists_url":"https://api.github.com/users/irina-marzioni-tdf/gists{/gist_id}","starred_url":"https://api.github.com/users/irina-marzioni-tdf/starred{/owner}{/repo}","subscriptions_url":"https://api.github.com/users/irina-marzioni-tdf/subscriptions","organizations_url":"https://api.github.com/users/irina-marzioni-tdf/orgs","repos_url":"https://api.github.com/users/irina-marzioni-tdf/repos","events_url":"https://api.github.com/users/irina-marzioni-tdf/events{/privacy}","received_events_url":"https://api.github.com/users/irina-marzioni-tdf/received_events","type":"User","site_admin":false},"html_url":"https://github.com/irina-marzioni-tdf/ejercicios-2022-10-19-Trabajo-en-grupo","description":null,"fork":true,"url":"https://api.github.com/repos/irina-marzioni-tdf/ejercicios-2022-10-19-Trabajo-en-grupo","forks_url":"https://api.github.com/repos/irina-marzioni-tdf/ejercicios-2022-10-19-Trabajo-en-grupo/forks","keys_url":"https://api.github.com/repos/irina-marzioni-tdf/ejercicios-2022-10-19-Trabajo-en-grupo/keys{/key_id}","collaborators_url":"https://api.github.com/repos/irina-marzioni-tdf/ejercicios-2022-10-19-Trabajo-en-grupo/collaborators{/collaborator}","teams_url":"https://api.github.com/repos/irina-marzioni-tdf/ejercicios-2022-10-19-Trabajo-en-grupo/teams","hooks_url":"https://api.github.com/repos/irina-marzioni-tdf/ejercicios-2022-10-19-Trabajo-en-grupo/hooks","issue_events_url":"https://api.github.com/repos/irina-marzioni-tdf/ejercicios-2022-10-19-Trabajo-en-grupo/issues/events{/number}","events_url":"https://api.github.com/repos/irina-marzioni-tdf/ejercicios-2022-10-19-Trabajo-en-grupo/events","assignees_url":"https://api.github.com/repos/irina-marzioni-tdf/ejercicios-2022-10-19-Trabajo-en-grupo/assignees{/user}","branches_url":"https://api.github.com/repos/irina-marzioni-tdf/ejercicios-2022-10-19-Trabajo-en-grupo/branches{/branch}","tags_url":"https://api.github.com/repos/irina-marzioni-tdf/ejercicios-2022-10-19-Trabajo-en-grupo/tags","blobs_url":"https://api.github.com/repos/irina-marzioni-tdf/ejercicios-2022-10-19-Trabajo-en-grupo/git/blobs{/sha}","git_tags_url":"https://api.github.com/repos/irina-marzioni-tdf/ejercicios-2022-10-19-Trabajo-en-grupo/git/tags{/sha}","git_refs_url":"https://api.github.com/repos/irina-marzioni-tdf/ejercicios-2022-10-19-Trabajo-en-grupo/git/refs{/sha}","trees_url":"https://api.github.com/repos/irina-marzioni-tdf/ejercicios-2022-10-19-Trabajo-en-grupo/git/trees{/sha}","statuses_url":"https://api.github.com/repos/irina-marzioni-tdf/ejercicios-2022-10-19-Trabajo-en-grupo/statuses/{sha}","languages_url":"https://api.github.com/repos/irina-marzioni-tdf/ejercicios-2022-10-19-Trabajo-en-grupo/languages","stargazers_url":"https://api.github.com/repos/irina-marzioni-tdf/ejercicios-2022-10-19-Trabajo-en-grupo/stargazers","contributors_url":"https://api.github.com/repos/irina-marzioni-tdf/ejercicios-2022-10-19-Trabajo-en-grupo/contributors","subscribers_url":"https://api.github.com/repos/irina-marzioni-tdf/ejercicios-2022-10-19-Trabajo-en-grupo/subscribers","subscription_url":"https://api.github.com/repos/irina-marzioni-tdf/ejercicios-2022-10-19-Trabajo-en-grupo/subscription","commits_url":"https://api.github.com/repos/irina-marzioni-tdf/ejercicios-2022-10-19-Trabajo-en-grupo/commits{/sha}","git_commits_url":"https://api.github.com/repos/irina-marzioni-tdf/ejercicios-2022-10-19-Trabajo-en-grupo/git/commits{/sha}","comments_url":"https://api.github.com/repos/irina-marzioni-tdf/ejercicios-2022-10-19-Trabajo-en-grupo/comments{/number}","issue_comment_url":"https://api.github.com/repos/irina-marzioni-tdf/ejercicios-2022-10-19-Trabajo-en-grupo/issues/comments{/number}","contents_url":"https://api.github.com/repos/irina-marzioni-tdf/ejercicios-2022-10-19-Trabajo-en-grupo/contents/{+path}","compare_url":"https://api.github.com/repos/irina-marzioni-tdf/ejercicios-2022-10-19-Trabajo-en-grupo/compare/{base}...{head}","merges_url":"https://api.github.com/repos/irina-marzioni-tdf/ejercicios-2022-10-19-Trabajo-en-grupo/merges","archive_url":"https://api.github.com/repos/irina-marzioni-tdf/ejercicios-2022-10-19-Trabajo-en-grupo/{archive_format}{/ref}","downloads_url":"https://api.github.com/repos/irina-marzioni-tdf/ejercicios-2022-10-19-Trabajo-en-grupo/downloads","issues_url":"https://api.github.com/repos/irina-marzioni-tdf/ejercicios-2022-10-19-Trabajo-en-grupo/issues{/number}","pulls_url":"https://api.github.com/repos/irina-marzioni-tdf/ejercicios-2022-10-19-Trabajo-en-grupo/pulls{/number}","milestones_url":"https://api.github.com/repos/irina-marzioni-tdf/ejercicios-2022-10-19-Trabajo-en-grupo/milestones{/number}","notifications_url":"https://api.github.com/repos/irina-marzioni-tdf/ejercicios-2022-10-19-Trabajo-en-grupo/notifications{?since,all,participating}","labels_url":"https://api.github.com/repos/irina-marzioni-tdf/ejercicios-2022-10-19-Trabajo-en-grupo/labels{/name}","releases_url":"https://api.github.com/repos/irina-marzioni-tdf/ejercicios-2022-10-19-Trabajo-en-grupo/releases{/id}","deployments_url":"https://api.github.com/repos/irina-marzioni-tdf/ejercicios-2022-10-19-Trabajo-en-grupo/deployments","created_at":"2022-11-06T23:59:59Z","updated_at":"2022-10-25T19:15:10Z","pushed_at":"2022-10-25T19:35:33Z","git_url":"git://github.com/irina-marzioni-tdf/ejercicios-2022-10-19-Trabajo-en-grupo.git","ssh_url":"git@github.com:irina-marzioni-tdf/ejercicios-2022-10-19-Trabajo-en-grupo.git","clone_url":"https://github.com/irina-marzioni-tdf/ejercicios-2022-10-19-Trabajo-en-grupo.git","svn_url":"https://github.com/irina-marzioni-tdf/ejercicios-2022-10-19-Trabajo-en-grupo","homepage":null,"size":153,"stargazers_count":0,"watchers_count":0,"language":null,"has_issues":false,"has_projects":true,"has_downloads":true,"has_wiki":true,"has_pages":false,"forks_count":0,"mirror_url":null,"archived":false,"disabled":false,"open_issues_count":0,"license":null,"allow_forking":true,"is_template":false,"web_commit_signoff_required":false,"topics":[],"visibility":"public","forks":0,"open_issues":0,"watchers":0,"default_branch":"main","public":true}},"public":true,"created_at":"2022-11-07T00:00:00Z"}')"""
    verify table_name

    table_name = "type_conflict_resolution"
    create_table table_name
    sql """insert into ${table_name} values (1, '{"c" : "123"}');"""
    sql """insert into ${table_name} values (2, '{"c" : 123}');"""
    sql """insert into ${table_name} values (3, '{"cc" : [123]}');"""
    sql """insert into ${table_name} values (4, '{"cc" : [123.1]}');"""
    sql """insert into ${table_name} values (5, '{"ccc" : 123}');"""
    sql """insert into ${table_name} values (6, '{"ccc" : 123321}');"""
    sql """insert into ${table_name} values (7, '{"cccc" : 123}');"""
    sql """insert into ${table_name} values (8, '{"cccc" : 123.11}');"""
    sql """insert into ${table_name} values (9, '{"ccccc" : [123]}');"""
    sql """insert into ${table_name} values (10, '{"ccccc" : [123456789]}');"""
    sql """insert into ${table_name} values (11, '{"b" : 1111111111111111}');"""
    sql """insert into ${table_name} values (12, '{"b" : 1.222222}');"""
    sql """insert into ${table_name} values (13, '{"bb" : 1}');"""
    sql """insert into ${table_name} values (14, '{"bb" : 214748364711}');"""
    verify table_name
}
