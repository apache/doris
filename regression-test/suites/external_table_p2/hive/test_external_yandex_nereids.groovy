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

suite("test_external_yandex_nereids", "p2,external,hive,external_remote,external_remote_hive") {
    Boolean ignoreP2 = true;
    if (ignoreP2) {
        logger.info("disable p2 test");
        return;
    }

    def formats = ["_parquet"]
    def duplicateAggregationKeys = "SELECT URL, EventDate, max(URL) FROM hitsSUFFIX WHERE CounterID = 1704509 AND UserID = 4322253409885123546 GROUP BY URL, EventDate, EventDate ORDER BY URL, EventDate;"
    def like1 = """SELECT count() FROM hitsSUFFIX WHERE URL LIKE '%/avtomobili_s_probegom/_%__%__%__%';"""
    def like2 = """SELECT count() FROM hitsSUFFIX WHERE URL LIKE '/avtomobili_s_probegom/_%__%__%__%';"""
    def like3 = """SELECT count() FROM hitsSUFFIX WHERE URL LIKE '%_/avtomobili_s_probegom/_%__%__%__%';"""
    def like4 = """SELECT count() FROM hitsSUFFIX WHERE URL LIKE '%avtomobili%';"""
    def loyalty = """SELECT loyalty, count() AS c
        FROM
        (
            SELECT UserID, CAST(((if(yandex > google, yandex / (yandex + google), 0 - google / (yandex + google))) * 10) AS TINYINT) AS loyalty
            FROM
            (
                SELECT UserID, sum(if(SearchEngineID = 2, 1, 0)) AS yandex, sum(if(SearchEngineID = 3, 1, 0)) AS google
                FROM hitsSUFFIX
                WHERE SearchEngineID = 2 OR SearchEngineID = 3 GROUP BY UserID HAVING yandex + google > 10
            ) t1
        ) t2
        GROUP BY loyalty
        ORDER BY loyalty;"""
    def maxStringIf = """SELECT CounterID, count(), max(if(SearchPhrase != "", SearchPhrase, "")) FROM hitsSUFFIX GROUP BY CounterID ORDER BY count() DESC LIMIT 20;"""
    def minMax = """SELECT CounterID, min(WatchID), max(WatchID) FROM hitsSUFFIX GROUP BY CounterID ORDER BY count() DESC LIMIT 20;"""
    def monotonicEvaluationSegfault = """SELECT max(0) FROM visitsSUFFIX WHERE (CAST(CAST(StartDate AS DATETIME) AS INT)) > 1000000000;"""
    def subqueryInWhere = """SELECT count() FROM hitsSUFFIX WHERE UserID IN (SELECT UserID FROM hitsSUFFIX WHERE CounterID = 800784);"""
    def where01 = """SELECT CounterID, count(distinct UserID) FROM hitsSUFFIX WHERE 0 != 0 GROUP BY CounterID;"""
    def where02 = """SELECT CounterID, count(distinct UserID) FROM hitsSUFFIX WHERE CAST(0 AS BOOLEAN) AND CounterID = 1704509 GROUP BY CounterID;"""

    String enabled = context.config.otherConfigs.get("enableExternalHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String extHiveHmsHost = context.config.otherConfigs.get("extHiveHmsHost")
        String extHiveHmsPort = context.config.otherConfigs.get("extHiveHmsPort")
        String catalog_name = "external_yandex_nereids"

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
        sql """set enable_nereids_planner=true"""
        sql """set enable_fallback_to_original_planner=false"""

        for (String format in formats) {
            logger.info("Process format " + format)
            qt_01 duplicateAggregationKeys.replace("SUFFIX", format)
            qt_02 like1.replace("SUFFIX", format)
            qt_03 like2.replace("SUFFIX", format)
            qt_04 like3.replace("SUFFIX", format)
            qt_05 like4.replace("SUFFIX", format)
            qt_06 loyalty.replace("SUFFIX", format)
            qt_07 maxStringIf.replace("SUFFIX", format)
            qt_08 minMax.replace("SUFFIX", format)
            qt_09 monotonicEvaluationSegfault.replace("SUFFIX", format)
            qt_10 subqueryInWhere.replace("SUFFIX", format)
            qt_11 where01.replace("SUFFIX", format)
            qt_12 where02.replace("SUFFIX", format)
        }
    }
}

