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

suite("test_external_github", "p2,external,hive,external_remote,external_remote_hive") {
    Boolean ignoreP2 = true;
    if (ignoreP2) {
        logger.info("disable p2 test");
        return;
    }

    def formats = ["_parquet", "_orc"]

    def affinityByIssuesAndPRs1 = """SELECT /*+SET_VAR(exec_mem_limit=8589934592) */
            repo_name,
            count() AS prs,
            count(distinct actor_login) AS authors
        FROM github_eventsSUFFIX
        WHERE (event_type = 'PullRequestEvent') AND (action = 'opened') AND (actor_login IN
        (
            SELECT actor_login
            FROM github_eventsSUFFIX
            WHERE (event_type = 'PullRequestEvent') AND (action = 'opened') AND (repo_name IN ('yandex/ClickHouse', 'ClickHouse/ClickHouse'))
        )) AND (lower(repo_name) NOT LIKE '%clickhouse%')
        GROUP BY repo_name
        ORDER BY authors DESC, prs DESC, length(repo_name) DESC
        LIMIT 50"""
    def affinityByIssuesAndPRs2 = """SELECT /*+SET_VAR(exec_mem_limit=8589934592) */
            repo_name,
            count() AS prs,
            count(distinct actor_login) AS authors
        FROM github_eventsSUFFIX
        WHERE (event_type = 'IssuesEvent') AND (action = 'opened') AND (actor_login IN
        (
            SELECT actor_login
            FROM github_eventsSUFFIX
            WHERE (event_type = 'IssuesEvent') AND (action = 'opened') AND (repo_name IN ('yandex/ClickHouse', 'ClickHouse/ClickHouse'))
        )) AND (lower(repo_name) NOT LIKE '%clickhouse%')
        GROUP BY repo_name
        ORDER BY authors DESC, prs DESC, repo_name ASC
        LIMIT 50"""
    def countingStar1 = """SELECT /*+SET_VAR(exec_mem_limit=8589934592) */count() FROM github_eventsSUFFIX WHERE event_type = 'WatchEvent'"""
    def countingStar2 = """SELECT /*+SET_VAR(exec_mem_limit=8589934592) */action, count() FROM github_eventsSUFFIX WHERE event_type = 'WatchEvent' GROUP BY action"""
    def countingStar3 = """SELECT /*+SET_VAR(exec_mem_limit=8589934592) */count() FROM github_eventsSUFFIX WHERE event_type = 'WatchEvent' AND repo_name IN ('ClickHouse/ClickHouse', 'yandex/ClickHouse') GROUP BY action"""
    def distributionOfRepositoriesByStarCount = """SELECT /*+SET_VAR(exec_mem_limit=8589934592) */
            pow(10, floor(log10(c))) AS stars,
            count(distinct k)
        FROM
        (
            SELECT
                repo_name AS k,
                count() AS c
            FROM github_eventsSUFFIX
            WHERE event_type = 'WatchEvent'
            GROUP BY k
        ) t
        GROUP BY stars
        ORDER BY stars ASC"""
    def githubRoulette = """SELECT /*+SET_VAR(exec_mem_limit=8589934592) */repo_name FROM github_eventsSUFFIX WHERE event_type = 'WatchEvent' ORDER BY created_at LIMIT 50"""
    def howHasTheListOfTopRepositoriesChangedOverTheYears1 = """SELECT /*+SET_VAR(exec_mem_limit=8589934592) */repo_name, count() AS stars FROM github_eventsSUFFIX WHERE event_type = 'WatchEvent' AND year(created_at) = '2015' GROUP BY repo_name ORDER BY stars DESC LIMIT 50"""
    def howHasTheListOfTopRepositoriesChangedOverTheYears2 = """SELECT /*+SET_VAR(exec_mem_limit=8589934592) */repo_name, count() AS stars FROM github_eventsSUFFIX WHERE event_type = 'WatchEvent' AND year(created_at) = '2016' GROUP BY repo_name ORDER BY stars DESC LIMIT 50"""
    def howHasTheListOfTopRepositoriesChangedOverTheYears3 = """SELECT /*+SET_VAR(exec_mem_limit=8589934592) */repo_name, count() AS stars FROM github_eventsSUFFIX WHERE event_type = 'WatchEvent' AND year(created_at) = '2017' GROUP BY repo_name ORDER BY stars DESC LIMIT 50"""
    def howHasTheListOfTopRepositoriesChangedOverTheYears4 = """SELECT /*+SET_VAR(exec_mem_limit=8589934592) */repo_name, count() AS stars FROM github_eventsSUFFIX WHERE event_type = 'WatchEvent' AND year(created_at) = '2018' GROUP BY repo_name ORDER BY stars DESC LIMIT 50"""
    def howHasTheListOfTopRepositoriesChangedOverTheYears5 = """SELECT /*+SET_VAR(exec_mem_limit=8589934592) */repo_name, count() AS stars FROM github_eventsSUFFIX WHERE event_type = 'WatchEvent' AND year(created_at) = '2019' GROUP BY repo_name ORDER BY stars DESC LIMIT 50"""
    def howHasTheListOfTopRepositoriesChangedOverTheYears6 = """SELECT /*+SET_VAR(exec_mem_limit=8589934592) */repo_name, count() AS stars FROM github_eventsSUFFIX WHERE event_type = 'WatchEvent' AND year(created_at) = '2020' GROUP BY repo_name ORDER BY stars DESC LIMIT 50"""
    def howHasTheTotalNumberOfStarsChangedOverTime = """SELECT /*+SET_VAR(exec_mem_limit=8589934592) */year(created_at) AS year, count() AS stars FROM github_eventsSUFFIX WHERE event_type = 'WatchEvent' GROUP BY year ORDER BY year"""
    def issuesWithTheMostComments1 = """SELECT /*+SET_VAR(exec_mem_limit=21474836480) */count() FROM github_eventsSUFFIX WHERE event_type = 'IssueCommentEvent'"""
    def issuesWithTheMostComments2 = """SELECT /*+SET_VAR(exec_mem_limit=21474836480) */repo_name, count() FROM github_eventsSUFFIX WHERE event_type = 'IssueCommentEvent' GROUP BY repo_name ORDER BY count() DESC LIMIT 50"""
    def issuesWithTheMostComments3 = """SELECT /*+SET_VAR(exec_mem_limit=21474836480) */
            repo_name,
            comments,
            issues,
            round(comments / issues, 2) AS ratio
        FROM
        (
            SELECT
                repo_name,
                count() AS comments,
                count(distinct number) AS issues
            FROM github_eventsSUFFIX
            WHERE event_type = 'IssueCommentEvent'
            GROUP BY repo_name
        ) t
        ORDER BY comments DESC
        LIMIT 50"""
    def issuesWithTheMostComments4 = """SELECT /*+SET_VAR(exec_mem_limit=21474836480) */
            repo_name,
            number,
            count() AS comments
        FROM github_eventsSUFFIX
        WHERE event_type = 'IssueCommentEvent' AND (action = 'created')
        GROUP BY repo_name, number
        ORDER BY comments DESC, number ASC
        LIMIT 50"""
    def issuesWithTheMostComments5 = """SELECT /*+SET_VAR(exec_mem_limit=21474836480) */
            repo_name,
            number,
            count() AS comments
        FROM github_eventsSUFFIX
        WHERE event_type = 'IssueCommentEvent' AND (action = 'created') AND (number > 10)
        GROUP BY repo_name, number
        ORDER BY comments DESC, repo_name, number
        LIMIT 50"""
    def issuesWithTheMostComments7 = """SELECT /*+SET_VAR(exec_mem_limit=21474836480) */
            repo_name,
            count() AS comments,
            count(distinct actor_login) AS authors
        FROM github_eventsSUFFIX
        WHERE event_type = 'CommitCommentEvent'
        GROUP BY repo_name
        ORDER BY count() DESC
        LIMIT 50"""
    def issuesWithTheMostComments8 = """SELECT /*+SET_VAR(exec_mem_limit=21474836480) */
            concat('https://github.com/', repo_name, '/commit/', commit_id) AS URL,
            count() AS comments,
            count(distinct actor_login) AS authors
        FROM github_eventsSUFFIX
        WHERE (event_type = 'CommitCommentEvent') AND commit_id != ""
        GROUP BY
            repo_name,
            commit_id
        HAVING authors >= 10
        ORDER BY count() DESC, URL, authors
        LIMIT 50"""
    def mostForkedRepositories = """SELECT /*+SET_VAR(exec_mem_limit=8589934592) */repo_name, count() AS forks FROM github_eventsSUFFIX WHERE event_type = 'ForkEvent' GROUP BY repo_name ORDER BY forks DESC LIMIT 50"""
    def organizationsByTheNumberOfRepositories = """SELECT /*+SET_VAR(exec_mem_limit=8589934592) */
            lower(split_part(repo_name, '/', 1)) AS org,
            count(distinct repo_name) AS repos
        FROM
        (
            SELECT repo_name
            FROM github_eventsSUFFIX
            WHERE event_type = 'WatchEvent'
            GROUP BY repo_name
            HAVING count() >= 10
        ) t
        GROUP BY org
        ORDER BY repos DESC, org ASC
        LIMIT 50"""
    def organizationsByTheNumberOfStars = """SELECT /*+SET_VAR(exec_mem_limit=8589934592) */
            lower(split_part(repo_name, '/', 1)) AS org,
            count() AS stars
        FROM github_eventsSUFFIX
        WHERE event_type = 'WatchEvent'
        GROUP BY org
        ORDER BY stars DESC
        LIMIT 50"""
    def proportionsBetweenStarsAndForks1 = """SELECT /*+SET_VAR(exec_mem_limit=8589934592) */
            repo_name,
            sum(fork) AS forks,
            sum(star) AS stars,
            round(sum(star) / sum(fork), 3) AS ratio
        FROM
        (
            SELECT
                repo_name,
                CASE WHEN event_type = 'ForkEvent' THEN 1 ELSE 0 END AS fork,
                CASE WHEN event_type = 'WatchEvent' THEN 1 ELSE 0 END AS star
            FROM github_eventsSUFFIX
            WHERE event_type IN ('ForkEvent', 'WatchEvent')
        ) t
        GROUP BY repo_name
        ORDER BY forks DESC
        LIMIT 50"""
    def proportionsBetweenStarsAndForks2 = """SELECT /*+SET_VAR(exec_mem_limit=8589934592) */
            repo_name,
            sum(fork) AS forks,
            sum(star) AS stars,
            round(sum(star) / sum(fork), 3) AS ratio
        FROM
        (
            SELECT
                repo_name,
                CASE WHEN event_type = 'ForkEvent' THEN 1 ELSE 0 END AS fork,
                CASE WHEN event_type = 'WatchEvent' THEN 1 ELSE 0 END AS star
            FROM github_eventsSUFFIX
            WHERE event_type IN ('ForkEvent', 'WatchEvent')
        ) t
        GROUP BY repo_name
        HAVING (stars > 100) AND (forks > 100)
        ORDER BY ratio DESC, repo_name
        LIMIT 50"""
    def proportionsBetweenStarsAndForks3 = """SELECT /*+SET_VAR(exec_mem_limit=8589934592) */
            repo_name,
            sum(fork) AS forks,
            sum(star) AS stars,
            round(sum(fork) / sum(star), 2) AS ratio
        FROM
        (
            SELECT
                repo_name,
                CASE WHEN event_type = 'ForkEvent' THEN 1 ELSE 0 END AS fork,
                CASE WHEN event_type = 'WatchEvent' THEN 1 ELSE 0 END AS star
            FROM github_eventsSUFFIX
            WHERE event_type IN ('ForkEvent', 'WatchEvent')
        ) t
        GROUP BY repo_name
        HAVING (stars > 100) AND (forks > 100)
        ORDER BY ratio DESC
        LIMIT 50"""
    def proportionsBetweenStarsAndForks4 = """SELECT /*+SET_VAR(exec_mem_limit=8589934592) */
            sum(fork) AS forks,
            sum(star) AS stars,
            round(sum(star) / sum(fork), 2) AS ratio
        FROM
        (
            SELECT
                repo_name,
                CASE WHEN event_type = 'ForkEvent' THEN 1 ELSE 0 END AS fork,
                CASE WHEN event_type = 'WatchEvent' THEN 1 ELSE 0 END AS star
            FROM github_eventsSUFFIX
            WHERE event_type IN ('ForkEvent', 'WatchEvent')
        ) t"""
    def proportionsBetweenStarsAndForks5 = """SELECT /*+SET_VAR(exec_mem_limit=8589934592) */
            sum(forks) AS forks,
            sum(stars) AS stars,
            round(sum(stars) / sum(forks), 2) AS ratio
        FROM
        (
            SELECT
                sum(fork) AS forks,
                sum(star) AS stars
            FROM
            (
                SELECT
                    repo_name,
                    CASE WHEN event_type = 'ForkEvent' THEN 1 ELSE 0 END AS fork,
                    CASE WHEN event_type = 'WatchEvent' THEN 1 ELSE 0 END AS star
                FROM github_eventsSUFFIX
                WHERE event_type IN ('ForkEvent', 'WatchEvent')
            ) t
            GROUP BY repo_name
            HAVING stars > 100
        ) t2"""
    def repositoriesByAmountOfModifiedCode = """SELECT /*+SET_VAR(exec_mem_limit=8589934592) */
            repo_name,
            count() AS prs,
            count(distinct actor_login) AS authors,
            sum(additions) AS adds,
            sum(deletions) AS dels
        FROM github_eventsSUFFIX
        WHERE (event_type = 'PullRequestEvent') AND (action = 'opened') AND (additions < 10000) AND (deletions < 10000)
        GROUP BY repo_name
        HAVING (adds / dels) < 10
        ORDER BY adds + dels DESC
        LIMIT 50"""
    def repositoriesByTheNumberOfPushes = """SELECT /*+SET_VAR(exec_mem_limit=8589934592) */
            repo_name,
            count() AS pushes,
            count(distinct actor_login) AS authors
        FROM github_eventsSUFFIX
        WHERE (event_type = 'PushEvent') AND (repo_name IN
        (
            SELECT repo_name
            FROM github_eventsSUFFIX
            WHERE event_type = 'WatchEvent'
            GROUP BY repo_name
            ORDER BY count() DESC
            LIMIT 10000
        ))
        GROUP BY repo_name
        ORDER BY count() DESC
        LIMIT 50"""
    def repositoriesWithClickhouse_related_comments1 = """SELECT /*+SET_VAR(exec_mem_limit=8589934592, parallel_fragment_exec_instance_num=8, parallel_pipeline_task_num=8, query_timeout=600) */repo_name, count() FROM github_eventsSUFFIX WHERE lower(body) LIKE '%clickhouse%' GROUP BY repo_name ORDER BY count() DESC, repo_name ASC LIMIT 50"""
    def repositoriesWithClickhouse_related_comments2 = """SELECT /*+SET_VAR(exec_mem_limit=8589934592, parallel_fragment_exec_instance_num=8, parallel_pipeline_task_num=8, query_timeout=600) */
            repo_name,
            sum(num_star) AS num_stars,
            sum(num_comment) AS num_comments
        FROM
        (
            SELECT
                repo_name,
                CASE WHEN event_type = 'WatchEvent' THEN 1 ELSE 0 END AS num_star,
                CASE WHEN lower(body) LIKE '%clickhouse%' THEN 1 ELSE 0 END AS num_comment
            FROM github_eventsSUFFIX
            WHERE (lower(body) LIKE '%clickhouse%') OR (event_type = 'WatchEvent')
        ) t
        GROUP BY repo_name
        HAVING num_comments > 0
        ORDER BY num_stars DESC,num_comments DESC,repo_name ASC
        LIMIT 50"""
    def repositoriesWithDoris_related_comments1 = """SELECT /*+SET_VAR(exec_mem_limit=8589934592, parallel_fragment_exec_instance_num=8, parallel_fragment_exec_instance_num=8, query_timeout=600) */repo_name, count() FROM github_eventsSUFFIX WHERE lower(body) LIKE '%doris%' GROUP BY repo_name ORDER BY count() DESC, repo_name ASC LIMIT 50"""
    def repositoriesWithDoris_related_comments2 = """SELECT /*+SET_VAR(exec_mem_limit=8589934592, parallel_fragment_exec_instance_num=8, parallel_fragment_exec_instance_num=8, query_timeout=600) */
            repo_name,
            sum(num_star) AS num_stars,
            sum(num_comment) AS num_comments
        FROM
        (
            SELECT
                repo_name,
                CASE WHEN event_type = 'WatchEvent' THEN 1 ELSE 0 END AS num_star,
                CASE WHEN lower(body) LIKE '%doris%' THEN 1 ELSE 0 END AS num_comment
            FROM github_eventsSUFFIX
            WHERE (lower(body) LIKE '%doris%') OR (event_type = 'WatchEvent')
        ) t
        GROUP BY repo_name
        HAVING num_comments > 0
        ORDER BY num_stars DESC,num_comments DESC,repo_name ASC
        LIMIT 50"""
    def repositoriesWithTheHighestGrowthYoY = """SELECT /*+SET_VAR(exec_mem_limit=8589934592, query_timeout=600) */
            repo_name,
            sum(created_at_2020) AS stars2020,
            sum(created_at_2019) AS stars2019,
            round(sum(created_at_2020) / sum(created_at_2019), 3) AS yoy,
            min(created_at) AS first_seen
        FROM
        (
            SELECT
                repo_name,
                CASE year(created_at) WHEN 2020 THEN 1 ELSE 0 END AS created_at_2020,
                CASE year(created_at) WHEN 2019 THEN 1 ELSE 0 END AS created_at_2019,
                created_at
            FROM github_eventsSUFFIX
            WHERE event_type = 'WatchEvent'
        ) t
        GROUP BY repo_name
        HAVING (min(created_at) <= '2019-01-01 00:00:00') AND (stars2019 >= 1000)
        ORDER BY yoy DESC, repo_name
        LIMIT 50"""
    def repositoriesWithTheMaximumAmountOfIssues1 = """SELECT /*+SET_VAR(exec_mem_limit=8589934592, query_timeout=600) */repo_name, count() AS c, count(distinct actor_login) AS u FROM github_eventsSUFFIX WHERE event_type = 'IssuesEvent' AND action = 'opened' GROUP BY repo_name ORDER BY c DESC, repo_name LIMIT 50"""
    def repositoriesWithTheMaximumAmountOfIssues2 = """SELECT /*+SET_VAR(exec_mem_limit=8589934592, query_timeout=600) */
            repo_name,
            sum(issue_created) AS c,
            count(distinct actor_login) AS u,
            sum(star) AS stars
        FROM
        (
            SELECT
                repo_name,
                CASE WHEN (event_type = 'IssuesEvent') AND (action = 'opened') THEN 1 ELSE 0 END AS issue_created,
                CASE WHEN event_type = 'WatchEvent' THEN 1 ELSE 0 END AS star,
                CASE WHEN (event_type = 'IssuesEvent') AND (action = 'opened') THEN actor_login ELSE NULL END AS actor_login
            FROM github_eventsSUFFIX
            WHERE event_type IN ('IssuesEvent', 'WatchEvent')
        ) t
        GROUP BY repo_name
        ORDER BY c DESC, repo_name
        LIMIT 50"""
    def repositoriesWithTheMaximumAmountOfIssues3 = """SELECT /*+SET_VAR(exec_mem_limit=8589934592, query_timeout=600) */
            repo_name,
            sum(issue_created) AS c,
            count(distinct actor_login) AS u,
            sum(star) AS stars
        FROM
        (
            SELECT
                repo_name,
                CASE WHEN (event_type = 'IssuesEvent') AND (action = 'opened') THEN 1 ELSE 0 END AS issue_created,
                CASE WHEN event_type = 'WatchEvent' THEN 1 ELSE 0 END AS star,
                CASE WHEN (event_type = 'IssuesEvent') AND (action = 'opened') THEN actor_login ELSE NULL END AS actor_login
            FROM github_eventsSUFFIX
            WHERE event_type IN ('IssuesEvent', 'WatchEvent')
        ) t
        GROUP BY repo_name
        HAVING stars >= 1000
        ORDER BY c DESC
        LIMIT 50"""
    def repositoriesWithTheMaximumAmountOfIssues4 = """SELECT /*+SET_VAR(exec_mem_limit=8589934592, query_timeout=600) */
            repo_name,
            sum(issue_created) AS c,
            count(distinct actor_login) AS u,
            sum(star) AS stars
        FROM
        (
            SELECT
                repo_name,
                CASE WHEN (event_type = 'IssuesEvent') AND (action = 'opened') THEN 1 ELSE 0 END AS issue_created,
                CASE WHEN event_type = 'WatchEvent' THEN 1 ELSE 0 END AS star,
                CASE WHEN (event_type = 'IssuesEvent') AND (action = 'opened') THEN actor_login ELSE NULL END AS actor_login
            FROM github_eventsSUFFIX
            WHERE event_type IN ('IssuesEvent', 'WatchEvent')
        ) t
        GROUP BY repo_name
        ORDER BY u DESC
        LIMIT 50"""
    def repositoriesWithTheMaximumAmountOfPullRequests1 = """SELECT /*+SET_VAR(exec_mem_limit=8589934592, query_timeout=600) */repo_name, count(), count(distinct actor_login) FROM github_eventsSUFFIX WHERE event_type = 'PullRequestEvent' AND action = 'opened' GROUP BY repo_name ORDER BY count() DESC LIMIT 50"""
    def repositoriesWithTheMaximumAmountOfPullRequests2 = """SELECT /*+SET_VAR(exec_mem_limit=8589934592, query_timeout=600) */repo_name, count(), count(distinct actor_login) AS u FROM github_eventsSUFFIX WHERE event_type = 'PullRequestEvent' AND action = 'opened' GROUP BY repo_name ORDER BY u DESC, 2 DESC LIMIT 50"""
    def repositoriesWithTheMaximumNumberOfAcceptedInvitations = """SELECT /*+SET_VAR(exec_mem_limit=8589934592, query_timeout=600) */
            repo_name,
            sum(invitation) AS invitations,
            sum(star) AS stars
        FROM
        (
            SELECT
                repo_name,
                CASE WHEN event_type = 'MemberEvent' THEN 1 ELSE 0 END AS invitation,
                CASE WHEN event_type = 'WatchEvent' THEN 1 ELSE 0 END AS star
            FROM github_eventsSUFFIX
            WHERE event_type IN ('MemberEvent', 'WatchEvent')
        ) t
        GROUP BY repo_name
        HAVING stars >= 100
        ORDER BY invitations DESC, stars DESC
        LIMIT 50"""
    def repositoriesWithTheWorstStagnation_order = """SELECT /*+SET_VAR(exec_mem_limit=8589934592, query_timeout=600) */
            repo_name,
            sum(created_at_2020) AS stars2020,
            sum(created_at_2019) AS stars2019,
            round(sum(created_at_2020) / sum(created_at_2019), 3) AS yoy,
            min(created_at) AS first_seen
        FROM
        (
            SELECT
                repo_name,
                CASE year(created_at) WHEN 2020 THEN 1 ELSE 0 END AS created_at_2020,
                CASE year(created_at) WHEN 2019 THEN 1 ELSE 0 END AS created_at_2019,
                created_at
            FROM github_eventsSUFFIX
            WHERE event_type = 'WatchEvent'
        ) t
        GROUP BY repo_name
        HAVING (min(created_at) <= '2019-01-01 00:00:00') AND (max(created_at) >= '2020-06-01 00:00:00') AND (stars2019 >= 1000)
        ORDER BY yoy,repo_name
        LIMIT 50"""
    def repositoryAffinityList1 = """SELECT /*+SET_VAR(exec_mem_limit=8589934592, query_timeout=600) */
          repo_name,
          count() AS stars
        FROM github_eventsSUFFIX
        WHERE (event_type = 'WatchEvent') AND (actor_login IN
        (
            SELECT actor_login
            FROM github_eventsSUFFIX
            WHERE (event_type = 'WatchEvent') AND (repo_name IN ('ClickHouse/ClickHouse', 'yandex/ClickHouse'))
        )) AND (repo_name NOT IN ('ClickHouse/ClickHouse', 'yandex/ClickHouse'))
        GROUP BY repo_name
        ORDER BY stars DESC, repo_name asc
        LIMIT 50"""
    def starsFromHeavyGithubUsers1 = """SELECT /*+SET_VAR(exec_mem_limit=21474836480, query_timeout=600) */
            repo_name,
            count()
        FROM github_eventsSUFFIX
        WHERE (event_type = 'WatchEvent') AND (actor_login IN
        (
            SELECT actor_login
            FROM github_eventsSUFFIX
            WHERE (event_type = 'PullRequestEvent') AND (action = 'opened')
        ))
        GROUP BY repo_name
        ORDER BY count() DESC
        LIMIT 50"""
    def starsFromHeavyGithubUsers2 = """SELECT /*+SET_VAR(exec_mem_limit=21474836480, query_timeout=600) */
            repo_name,
            count()
        FROM github_eventsSUFFIX
        WHERE (event_type = 'WatchEvent') AND (actor_login IN
        (
            SELECT actor_login
            FROM github_eventsSUFFIX
            WHERE (event_type = 'PullRequestEvent') AND (action = 'opened')
            GROUP BY actor_login
            HAVING count() >= 10
        ))
        GROUP BY repo_name
        ORDER BY count() DESC
        LIMIT 50"""
    def theLongestRepositoryNames1 = """SELECT /*+SET_VAR(exec_mem_limit=8589934592, query_timeout=600) */count(), repo_name FROM github_eventsSUFFIX WHERE event_type = 'WatchEvent' GROUP BY repo_name ORDER BY length(repo_name) DESC, repo_name LIMIT 50"""
    def theLongestRepositoryNames2 = """SELECT /*+SET_VAR(exec_mem_limit=8589934592, query_timeout=600) */repo_name, count() FROM github_eventsSUFFIX WHERE event_type = 'WatchEvent' AND repo_name LIKE '%_/_%' GROUP BY repo_name ORDER BY length(repo_name) ASC, repo_name LIMIT 50"""
    def theMostToughCodeReviews = """SELECT /*+SET_VAR(exec_mem_limit=8589934592, query_timeout=600) */
            concat('https://github.com/', repo_name, '/pull/', CAST(number AS STRING)) AS URL,
            count(distinct actor_login) AS authors
        FROM github_eventsSUFFIX
        WHERE (event_type = 'PullRequestReviewCommentEvent') AND (action = 'created')
        GROUP BY
            repo_name,
            number
        ORDER BY authors DESC, URL ASC
        LIMIT 50"""
    def theTotalNumberOfUsersOnGithub1 = """SELECT /*+SET_VAR(exec_mem_limit=8589934592, query_timeout=600) */count(distinct actor_login) FROM github_eventsSUFFIX"""
    def theTotalNumberOfUsersOnGithub2 = """SELECT /*+SET_VAR(exec_mem_limit=8589934592, query_timeout=600) */count(distinct actor_login) FROM github_eventsSUFFIX WHERE event_type = 'WatchEvent'"""
    def theTotalNumberOfUsersOnGithub3 = """SELECT /*+SET_VAR(exec_mem_limit=8589934592, query_timeout=600) */count(distinct actor_login) FROM github_eventsSUFFIX WHERE event_type = 'PushEvent'"""
    def theTotalNumberOfUsersOnGithub4 = """SELECT /*+SET_VAR(exec_mem_limit=8589934592, query_timeout=600) */count(distinct actor_login) FROM github_eventsSUFFIX WHERE event_type = 'PullRequestEvent' AND action = 'opened'"""
    def topRepositoriesByStars = """SELECT /*+SET_VAR(exec_mem_limit=8589934592, query_timeout=600) */repo_name, count() AS stars FROM github_eventsSUFFIX WHERE event_type = 'WatchEvent' GROUP BY repo_name ORDER BY stars DESC LIMIT 50"""
    def whatIsTheBestDayOfTheWeekToCatchAStar = """SELECT /*+SET_VAR(exec_mem_limit=8589934592, query_timeout=600) */dayofweek(created_at) AS day, count() AS stars FROM github_eventsSUFFIX WHERE event_type = 'WatchEvent' GROUP BY day ORDER BY day"""
    def whoAreAllThosePeopleGivingStars1 = """SELECT /*+SET_VAR(exec_mem_limit=8589934592, query_timeout=600) */actor_login, count() AS stars FROM github_eventsSUFFIX WHERE event_type = 'WatchEvent' GROUP BY actor_login ORDER BY stars DESC LIMIT 50"""
    def whoAreAllThosePeopleGivingStars2 = """SELECT /*+SET_VAR(exec_mem_limit=8589934592, query_timeout=600) */actor_login, count() AS stars FROM github_eventsSUFFIX WHERE event_type = 'WatchEvent' AND actor_login = 'alexey-milovidov' GROUP BY actor_login ORDER BY stars DESC LIMIT 50"""
    def whoAreAllThosePeopleGivingStars3 = """SELECT /*+SET_VAR(exec_mem_limit=8589934592, query_timeout=600) */
            repo_name,
            count() AS stars
        FROM github_eventsSUFFIX
        WHERE (event_type = 'WatchEvent') AND (repo_name IN
        (
            SELECT repo_name
            FROM github_eventsSUFFIX
            WHERE (event_type = 'WatchEvent') AND (actor_login = 'alexey-milovidov')
        ))
        GROUP BY repo_name
        ORDER BY stars DESC
        LIMIT 50"""

    def detail_test1 = """SELECT /*+SET_VAR(exec_mem_limit=8589934592, query_timeout=600) */commit_id from github_eventsSUFFIX WHERE event_type = 'CommitCommentEvent' AND commit_id != "" order by commit_id asc limit 10"""
    def detail_test2 = """SELECT /*+SET_VAR(exec_mem_limit=8589934592, query_timeout=600) */commit_id from github_eventsSUFFIX WHERE event_type = 'CommitCommentEvent' AND commit_id != "" order by commit_id desc limit 10"""
    def detail_test3 = """SELECT /*+SET_VAR(exec_mem_limit=8589934592, query_timeout=600) */commit_id from github_eventsSUFFIX WHERE event_type = 'CommitCommentEvent' AND commit_id is null order by commit_id limit 10"""
    def detail_test4 = """SELECT /*+SET_VAR(exec_mem_limit=8589934592, query_timeout=600) */commit_id from github_eventsSUFFIX WHERE event_type = 'CommitCommentEvent' AND commit_id is null AND commit_id != "" order by commit_id limit 10"""

    String enabled = context.config.otherConfigs.get("enableExternalHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String extHiveHmsHost = context.config.otherConfigs.get("extHiveHmsHost")
        String extHiveHmsPort = context.config.otherConfigs.get("extHiveHmsPort")
        String catalog_name = "external_yandex"

        sql """drop catalog if exists ${catalog_name};"""

        sql """
            create catalog if not exists ${catalog_name} properties (
                'type'='hms',
                'hive.metastore.uris' = 'thrift://${extHiveHmsHost}:${extHiveHmsPort}'
            );
        """
        logger.info("catalog " + catalog_name + " created")

        sql """switch ${catalog_name};"""

        logger.info("switched to catalog " + catalog_name)

        sql """use multi_catalog;"""

        logger.info("use multi_catalog")

        for (String format in formats) {
            logger.info("Process format " + format)
            qt_01 affinityByIssuesAndPRs1.replace("SUFFIX", format)
            qt_02 affinityByIssuesAndPRs2.replace("SUFFIX", format)
            qt_03 countingStar1.replace("SUFFIX", format)
            qt_04 countingStar2.replace("SUFFIX", format)
            qt_05 countingStar3.replace("SUFFIX", format)
            qt_06 distributionOfRepositoriesByStarCount.replace("SUFFIX", format)
            qt_07 githubRoulette.replace("SUFFIX", format)
            qt_08 howHasTheListOfTopRepositoriesChangedOverTheYears1.replace("SUFFIX", format)
            qt_09 howHasTheListOfTopRepositoriesChangedOverTheYears2.replace("SUFFIX", format)
            qt_10 howHasTheListOfTopRepositoriesChangedOverTheYears3.replace("SUFFIX", format)
            qt_11 howHasTheListOfTopRepositoriesChangedOverTheYears4.replace("SUFFIX", format)
            qt_12 howHasTheListOfTopRepositoriesChangedOverTheYears5.replace("SUFFIX", format)
            qt_13 howHasTheListOfTopRepositoriesChangedOverTheYears6.replace("SUFFIX", format)
            qt_14 howHasTheTotalNumberOfStarsChangedOverTime.replace("SUFFIX", format)
            qt_15 issuesWithTheMostComments1.replace("SUFFIX", format)
            qt_16 issuesWithTheMostComments2.replace("SUFFIX", format)
            qt_17 issuesWithTheMostComments3.replace("SUFFIX", format)
            qt_18 issuesWithTheMostComments4.replace("SUFFIX", format)
            qt_19 issuesWithTheMostComments5.replace("SUFFIX", format)
            qt_20 issuesWithTheMostComments7.replace("SUFFIX", format)
            qt_21 issuesWithTheMostComments8.replace("SUFFIX", format)
            qt_22 mostForkedRepositories.replace("SUFFIX", format)
            qt_23 organizationsByTheNumberOfRepositories.replace("SUFFIX", format)
            qt_24 organizationsByTheNumberOfStars.replace("SUFFIX", format)
            qt_25 proportionsBetweenStarsAndForks1.replace("SUFFIX", format)
            qt_26 proportionsBetweenStarsAndForks2.replace("SUFFIX", format)
            qt_27 proportionsBetweenStarsAndForks3.replace("SUFFIX", format)
            qt_28 proportionsBetweenStarsAndForks4.replace("SUFFIX", format)
            qt_29 proportionsBetweenStarsAndForks5.replace("SUFFIX", format)
            qt_30 repositoriesByAmountOfModifiedCode.replace("SUFFIX", format)
            qt_31 repositoriesByTheNumberOfPushes.replace("SUFFIX", format)
            qt_32 repositoriesWithClickhouse_related_comments1.replace("SUFFIX", format)
            qt_33 repositoriesWithClickhouse_related_comments2.replace("SUFFIX", format)
            qt_34 repositoriesWithDoris_related_comments1.replace("SUFFIX", format)
            qt_35 repositoriesWithDoris_related_comments2.replace("SUFFIX", format)
            qt_36 repositoriesWithTheHighestGrowthYoY.replace("SUFFIX", format)
            qt_37 repositoriesWithTheMaximumAmountOfIssues1.replace("SUFFIX", format)
            qt_38 repositoriesWithTheMaximumAmountOfIssues2.replace("SUFFIX", format)
            qt_39 repositoriesWithTheMaximumAmountOfIssues3.replace("SUFFIX", format)
            qt_40 repositoriesWithTheMaximumAmountOfIssues4.replace("SUFFIX", format)
            qt_41 repositoriesWithTheMaximumAmountOfPullRequests1.replace("SUFFIX", format)
            qt_42 repositoriesWithTheMaximumAmountOfPullRequests2.replace("SUFFIX", format)
            qt_43 repositoriesWithTheMaximumNumberOfAcceptedInvitations.replace("SUFFIX", format)
            qt_44 repositoriesWithTheWorstStagnation_order.replace("SUFFIX", format)
            qt_45 repositoryAffinityList1.replace("SUFFIX", format)
            qt_46 starsFromHeavyGithubUsers1.replace("SUFFIX", format)
            qt_47 starsFromHeavyGithubUsers2.replace("SUFFIX", format)
            qt_48 theLongestRepositoryNames1.replace("SUFFIX", format)
            qt_49 theLongestRepositoryNames2.replace("SUFFIX", format)
            qt_50 theMostToughCodeReviews.replace("SUFFIX", format)
            qt_51 theTotalNumberOfUsersOnGithub1.replace("SUFFIX", format)
            qt_52 theTotalNumberOfUsersOnGithub2.replace("SUFFIX", format)
            qt_53 theTotalNumberOfUsersOnGithub3.replace("SUFFIX", format)
            qt_54 theTotalNumberOfUsersOnGithub4.replace("SUFFIX", format)
            qt_55 topRepositoriesByStars.replace("SUFFIX", format)
            qt_56 whatIsTheBestDayOfTheWeekToCatchAStar.replace("SUFFIX", format)
            qt_57 whoAreAllThosePeopleGivingStars1.replace("SUFFIX", format)
            qt_58 whoAreAllThosePeopleGivingStars2.replace("SUFFIX", format)
            qt_59 whoAreAllThosePeopleGivingStars3.replace("SUFFIX", format)
            qt_60 detail_test1.replace("SUFFIX", format)
            qt_61 detail_test2.replace("SUFFIX", format)
            qt_62 detail_test3.replace("SUFFIX", format)
            qt_63 detail_test4.replace("SUFFIX", format)
        }
    }
}

