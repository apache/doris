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

import groovy.json.JsonSlurper

// Focus: different metrics (l2 vs inner_product) and predicate directions (< vs >)
// Expectation:
//  - l2_distance: index returns distance on < (or <=) range; not on > (or >=)
//  - inner_product: index returns distance on > (or >=) range; not on < (or <=)
// We infer index-only read by comparing ScanBytes with and without selecting/using distance.

def getProfileList = {
    def dst = 'http://' + context.config.feHttpAddress
    def conn = new URL(dst + "/rest/v1/query_profile").openConnection()
    conn.setRequestMethod("GET")
    def encoding = Base64.getEncoder().encodeToString((context.config.feHttpUser + ":" +
            (context.config.feHttpPassword == null ? "" : context.config.feHttpPassword)).getBytes("UTF-8"))
    conn.setRequestProperty("Authorization", "Basic ${encoding}")
    return conn.getInputStream().getText()
}

def getProfile = { id ->
    def dst = 'http://' + context.config.feHttpAddress
    def conn = new URL(dst + "/api/profile/text/?query_id=$id").openConnection()
    conn.setRequestMethod("GET")
    def encoding = Base64.getEncoder().encodeToString((context.config.feHttpUser + ":" +
            (context.config.feHttpPassword == null ? "" : context.config.feHttpPassword)).getBytes("UTF-8"))
    conn.setRequestProperty("Authorization", "Basic ${encoding}")
    return conn.getInputStream().getText()
}

// Note: define getProfileWithToken inside suite to use suite-level assertTrue

def extractScanBytesValue = { String profileText ->
    def lines = profileText.split("\n")
    for (def line : lines) {
        if (line.contains("ScanBytes:")) {
            def m = (line =~ /ScanBytes:\s*([0-9]+(?:\.[0-9]+)?)\s*[A-Za-z]+/)
            if (m.find()) {
                return m.group(1)
            }
        }
    }
    return null
}

suite("ann_index_only_scan_metric_direction") {
    def getProfileWithToken = { token ->
        String profileId = ""
        int attempts = 0
        while (attempts < 10 && (profileId == null || profileId == "")) {
            List profileData = new JsonSlurper().parseText(getProfileList()).data.rows
            for (def profileItem in profileData) {
                if (profileItem["Sql Statement"].toString().contains(token)) {
                    profileId = profileItem["Profile ID"].toString()
                    break
                }
            }
            if (profileId == null || profileId == "") {
                Thread.sleep(300)
            }
            attempts++
        }
        assertTrue(profileId != null && profileId != "")
        Thread.sleep(800)
        return getProfile(profileId).toString()
    }
    // session vars
    sql "unset variable all;"
    sql "set profile_level=2;"
    sql "set enable_profile=true;"
    sql "set experimental_topn_lazy_materialization_threshold=0;"
    sql "set experimental_enable_virtual_slot_for_cse=true;"
    sql "set enable_no_need_read_data_opt=true;"
    sql "set parallel_pipeline_task_num=1;" // make execution more deterministic for test

    // l2 table
    sql "drop table if exists ann_md_l2"
    sql """
        create table ann_md_l2 (
            id int not null,
            embedding array<float> not null,
            comment string not null,
            value int null,
            index ann_embedding(`embedding`) using ann properties(
                "index_type"="hnsw",
                "metric_type"="l2_distance",
                "dim"="8"
            )
        ) duplicate key(id)
        distributed by hash(id) buckets 1
        properties("replication_num"="1");
    """

    // inner product table
    sql "drop table if exists ann_md_ip"
    sql """
        create table ann_md_ip (
            id int not null,
            embedding array<float> not null,
            comment string not null,
            value int null,
            index ann_embedding(`embedding`) using ann properties(
                "index_type"="hnsw",
                "metric_type"="inner_product",
                "dim"="8"
            )
        ) duplicate key(id)
        distributed by hash(id) buckets 1
        properties("replication_num"="1");
    """

    def rows = """
        (0, [39.906116, 10.495334, 54.08394, 88.67262, 55.243687, 10.162686, 36.335983, 38.684258], "A", 100),
        (1, [62.759315, 97.15586, 25.832521, 39.604908, 88.76715, 72.64085, 9.688437, 17.721428], "B", 101),
        (2, [15.447449, 59.7771, 65.54516, 12.973712, 99.685135, 72.080734, 85.71118, 99.35976], "C", 102),
        (3, [72.26747, 46.42257, 32.368374, 80.50209, 5.777631, 98.803314, 7.0915947, 68.62693], "D", 103),
        (4, [22.098177, 74.10027, 63.634556, 4.710955, 12.405106, 79.39356, 63.014366, 68.67834], "E", 104),
        (5, [27.53003, 72.1106, 50.891026, 38.459953, 68.30715, 20.610682, 94.806274, 45.181377], "F", 105),
        (6, [77.73215, 64.42907, 71.50025, 43.85641, 94.42648, 50.04773, 65.12575, 68.58207], "G", 106),
        (7, [2.1537063, 82.667885, 16.171143, 71.126656, 5.335274, 40.286068, 11.943586, 3.69409], "H", 107),
        (8, [54.435013, 56.800594, 59.335514, 55.829235, 85.46627, 33.388138, 11.076194, 20.480877], "I", 108),
        (9, [76.197945, 60.623528, 84.229805, 31.652937, 71.82595, 48.04684, 71.29212, 30.282396], "J", 109)
    """
    sql "insert into ann_md_l2 values ${rows};"
    sql "insert into ann_md_ip values ${rows};"

    // Common probe vector
    def v = "[26.360261917114258,7.05784273147583,32.361351013183594,86.39714050292969,58.79527282714844,27.189321517944336,99.38946533203125,80.19270324707031]"

    // L2: < threshold -> expect index returns distance; projecting distance should NOT increase ScanBytes
    def t1 = UUID.randomUUID().toString()
    sql """
        select id, "${t1}" from ann_md_l2
        where l2_distance_approximate(embedding, ${v}) < 160.0
        order by id limit 20;
    """
    def t2 = UUID.randomUUID().toString()
    sql """
        select id, "${t2}", l2_distance_approximate(embedding, ${v}) as dist
        from ann_md_l2
        where l2_distance_approximate(embedding, ${v}) < 160.0
        order by id limit 20;
    """
    def p1 = getProfileWithToken(t1)
    def p2 = getProfileWithToken(t2)
    def s1 = extractScanBytesValue(p1)
    def s2 = extractScanBytesValue(p2)
    logger.info("L2 < threshold ScanBytes: no-proj=${s1}, with-dist=${s2}")
    assertTrue(s1 == s2)

    // L2: > threshold -> index doesn't return distance; projecting distance SHOULD increase ScanBytes
    def t3 = UUID.randomUUID().toString()
    sql """
        select id, "${t3}" from ann_md_l2
        where l2_distance_approximate(embedding, ${v}) > 120.0
        order by id limit 20;
    """
    def t4 = UUID.randomUUID().toString()
    sql """
        select id, "${t4}", l2_distance_approximate(embedding, ${v}) as dist
        from ann_md_l2
        where l2_distance_approximate(embedding, ${v}) > 120.0
        order by id limit 20;
    """
    def p3 = getProfileWithToken(t3)
    def p4 = getProfileWithToken(t4)
    def s3 = extractScanBytesValue(p3)
    def s4 = extractScanBytesValue(p4)
    logger.info("L2 > threshold ScanBytes: no-proj=${s3}, with-dist=${s4}")
    assertTrue(s3 != s4)

    // Inner Product: > threshold -> expect index returns distance
    def t5 = UUID.randomUUID().toString()
    sql """
        select id, "${t5}" from ann_md_ip
        where inner_product_approximate(embedding, ${v}) > 1000.0
        order by id limit 20;
    """
    def t6 = UUID.randomUUID().toString()
    sql """
        select id, "${t6}", inner_product_approximate(embedding, ${v}) as score
        from ann_md_ip
        where inner_product_approximate(embedding, ${v}) > 1000.0
        order by id limit 20;
    """
    def p5 = getProfileWithToken(t5)
    def p6 = getProfileWithToken(t6)
    def s5 = extractScanBytesValue(p5)
    def s6 = extractScanBytesValue(p6)
    logger.info("IP > threshold ScanBytes: no-proj=${s5}, with-score=${s6}")
    assertTrue(s5 == s6)

    // Inner Product: < threshold -> expect index doesn't return distance; projecting distance increases ScanBytes
    def t7 = UUID.randomUUID().toString()
    sql """
        select id, "${t7}" from ann_md_ip
        where inner_product_approximate(embedding, ${v}) < 16175.99
        order by id limit 20;
    """
    def t8 = UUID.randomUUID().toString()
    sql """
        select id, "${t8}", inner_product_approximate(embedding, ${v}) as score
        from ann_md_ip
        where inner_product_approximate(embedding, ${v}) < 16175.99
        order by id limit 20;
    """
    def p7 = getProfileWithToken(t7)
    def p8 = getProfileWithToken(t8)
    def s7 = extractScanBytesValue(p7)
    def s8 = extractScanBytesValue(p8)
    logger.info("IP < threshold ScanBytes: no-proj=${s7}, with-score=${s8}")
    assertTrue(s7 != s8)
}
