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

suite("ann_with_fulltext") {
    sql "drop table if exists ann_with_fulltext"
    sql "set profile_level=2;"
    sql "set enable_common_expr_pushdown=true;"
    
    sql """
        create table ann_with_fulltext (
            id int not null,
            embedding array<float> not null,
            comment String not null,
            value int null,
            INDEX idx_comment(`comment`) USING INVERTED PROPERTIES("parser" = "english") COMMENT 'inverted index for comment',
            INDEX ann_embedding(`embedding`) USING ANN PROPERTIES("index_type"="hnsw","metric_type"="l2_distance","dim"="8")
        ) duplicate key (`id`) 
        distributed by hash(`id`) buckets 1
        properties("replication_num"="1");
    """

    sql """
        INSERT INTO ann_with_fulltext (id, embedding, comment, value) VALUES
            (0, [39.906116, 10.495334, 54.08394, 88.67262, 55.243687, 10.162686, 36.335983, 38.684258], "This example illustrates how subtle differences can influence perception. It's more about interpretation than right or wrong.", 100),
            (1, [62.759315, 97.15586, 25.832521, 39.604908, 88.76715, 72.64085, 9.688437, 17.721428], "Thanks for all the comments, good and bad. They help us refine our test. Keep in mind that we're attempting to figure you out in 40 pairs of pictures. We did this so that lots of people could take it, just to introduce the idea.<p>A <i>real</i> test would have more like 200 pairs, which is what the YC founders took when we assessed their attributes in the first place.", 101),
            (2, [15.447449, 59.7771, 65.54516, 12.973712, 99.685135, 72.080734, 85.71118, 99.35976], "At a glance, these might seem obvious, but there’s nuance in every choice. Don’t rush.", 102),
            (3, [72.26747, 46.42257, 32.368374, 80.50209, 5.777631, 98.803314, 7.0915947, 68.62693], "We're testing how consistent your judgments are over a range of visual impressions. There's no single 'correct' answer.", 103),
            (4, [22.098177, 74.10027, 63.634556, 4.710955, 12.405106, 79.39356, 63.014366, 68.67834], "Some pairs are meant to be tricky. Your intuition is part of what we're analyzing.", 104),
            (5, [27.53003, 72.1106, 50.891026, 38.459953, 68.30715, 20.610682, 94.806274, 45.181377], "This data will help us identify patterns in how people perceive attributes such as trustworthiness or confidence.", 105),
            (6, [77.73215, 64.42907, 71.50025, 43.85641, 94.42648, 50.04773, 65.12575, 68.58207], "Sometimes people see entirely different things in the same image. That's part of the exploration.", 106),
            (7, [2.1537063, 82.667885, 16.171143, 71.126656, 5.335274, 40.286068, 11.943586, 3.69409], "Don't worry if you’re unsure. The ambiguity is intentional — that’s what makes this interesting.", 107),
            (8, [54.435013, 56.800594, 59.335514, 55.829235, 85.46627, 33.388138, 11.076194, 20.480877], "Your reactions help us understand which features people subconsciously favor or avoid.", 108),
            (9, [76.197945, 60.623528, 84.229805, 31.652937, 71.82595, 48.04684, 71.29212, 30.282396], "This task isn’t about right answers, but about consistency in your judgments over time.", 109);
    """
    qt_q0 """
        select * from ann_with_fulltext ORDER BY id;
    """

    qt_q1 """
        select * from ann_with_fulltext where comment match_any "illustrates comments answers" and l2_distance_approximate(embedding, [26.360261917114258,7.05784273147583,32.361351013183594,86.39714050292969,58.79527282714844,27.189321517944336,99.38946533203125,80.19270324707031]) < 105.66439056396484
    """

    qt_q2 """
        select *, l2_distance_approximate(embedding, [26.360261917114258,7.05784273147583,32.361351013183594,86.39714050292969,58.79527282714844,27.189321517944336,99.38946533203125,80.19270324707031]) as dist from ann_with_fulltext where comment match_any "illustrates comments answers"  order by dist limit 2
    """
    // This case should error because score() + another ordering expression is not allowed for TopN push down
    test {
        sql "select score() as score, l2_distance_approximate(embedding, [26.360261917114258,7.05784273147583,32.361351013183594,86.39714050292969,58.79527282714844,27.189321517944336,99.38946533203125,80.19270324707031]) as dist from ann_with_fulltext where comment match_any \"illustrates comments answers\"  order by score, dist limit 2"
        exception "TopN must have exactly one ordering expression for score() push down optimization"
    }
}