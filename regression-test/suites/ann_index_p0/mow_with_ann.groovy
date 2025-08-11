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

suite("mow_with_ann") {
	sql "set profile_level=2;"
	sql "set enable_common_expr_pushdown=true;"

	// 1) MOW table with ANN index only
	sql "drop table if exists t_mow_ann_only"
	sql """
		create table t_mow_ann_only (
			id int not null,
			embedding array<float> not null,
			comment string not null,
			value int null,
			INDEX ann_embedding(`embedding`) USING ANN PROPERTIES(
				"index_type"="hnsw",
				"metric_type"="l2_distance",
				"dim"="8"
			)
		) unique key(`id`)
		distributed by hash(`id`) buckets 1
		properties(
			"replication_num"="1",
			"enable_unique_key_merge_on_write" = "true"
		);
	"""

	sql """
		INSERT INTO t_mow_ann_only (id, embedding, comment, value) VALUES
			(0, [39.906116, 10.495334, 54.08394, 88.67262, 55.243687, 10.162686, 36.335983, 38.684258], "This example illustrates how subtle differences can influence perception. It's more about interpretation than right or wrong.", 100),
			(1, [62.759315, 97.15586, 25.832521, 39.604908, 88.76715, 72.64085, 9.688437, 17.721428], "Thanks for all the comments, good and bad. They help us refine our test.", 101),
			(2, [15.447449, 59.7771, 65.54516, 12.973712, 99.685135, 72.080734, 85.71118, 99.35976], "At a glance, these might seem obvious, but there’s nuance in every choice. Don’t rush.", 102),
			(3, [72.26747, 46.42257, 32.368374, 80.50209, 5.777631, 98.803314, 7.0915947, 68.62693], "We're testing how consistent your judgments are over a range of visual impressions.", 103),
			(4, [22.098177, 74.10027, 63.634556, 4.710955, 12.405106, 79.39356, 63.014366, 68.67834], "Some pairs are meant to be tricky. Your intuition is part of what we're analyzing.", 104),
			(5, [27.53003, 72.1106, 50.891026, 38.459953, 68.30715, 20.610682, 94.806274, 45.181377], "This data will help us identify patterns in how people perceive attributes.", 105),
			(6, [77.73215, 64.42907, 71.50025, 43.85641, 94.42648, 50.04773, 65.12575, 68.58207], "Sometimes people see entirely different things in the same image.", 106),
			(7, [2.1537063, 82.667885, 16.171143, 71.126656, 5.335274, 40.286068, 11.943586, 3.69409], "Don't worry if you’re unsure. The ambiguity is intentional.", 107),
			(8, [54.435013, 56.800594, 59.335514, 55.829235, 85.46627, 33.388138, 11.076194, 20.480877], "Your reactions help us understand which features people subconsciously favor.", 108),
			(9, [76.197945, 60.623528, 84.229805, 31.652937, 71.82595, 48.04684, 71.29212, 30.282396], "This task isn’t about right answers, but about consistency.", 109);
	"""

	// ANN top-N on MOW
	qt_ann_only_topn """
		select id,
			   l2_distance_approximate(embedding, [26.360261917114258,7.05784273147583,32.361351013183594,86.39714050292969,58.79527282714844,27.189321517944336,99.38946533203125,80.19270324707031]) as dist
		from t_mow_ann_only
		order by dist
		limit 3;
	"""

	// ANN range filter on MOW
	qt_ann_only_range """
		select count(*)
		from t_mow_ann_only
		where l2_distance_approximate(embedding, [26.360261917114258,7.05784273147583,32.361351013183594,86.39714050292969,58.79527282714844,27.189321517944336,99.38946533203125,80.19270324707031]) < 106.0;
	"""

	// 2) MOW table with ANN + Inverted index
	sql "drop table if exists t_mow_ann_fulltext"
	sql """
		create table t_mow_ann_fulltext (
			id int not null,
			embedding array<float> not null,
			comment String not null,
			value int null,
			INDEX idx_comment(`comment`) USING INVERTED PROPERTIES("parser" = "english") COMMENT 'inverted index for comment',
			INDEX ann_embedding(`embedding`) USING ANN PROPERTIES(
				"index_type"="hnsw",
				"metric_type"="l2_distance",
				"dim"="8"
			)
		) unique key (`id`) 
		distributed by hash(`id`) buckets 1
		properties(
			"replication_num"="1",
			"enable_unique_key_merge_on_write" = "true"
		);
	"""

	sql """
		INSERT INTO t_mow_ann_fulltext (id, embedding, comment, value) VALUES
			(0, [39.906116, 10.495334, 54.08394, 88.67262, 55.243687, 10.162686, 36.335983, 38.684258], "This example illustrates how subtle differences can influence perception. It's more about interpretation than right or wrong.", 100),
			(1, [62.759315, 97.15586, 25.832521, 39.604908, 88.76715, 72.64085, 9.688437, 17.721428], "Thanks for all the comments, good and bad. They help us refine our test.", 101),
			(2, [15.447449, 59.7771, 65.54516, 12.973712, 99.685135, 72.080734, 85.71118, 99.35976], "At a glance, these might seem obvious, but there’s nuance in every choice. Don’t rush.", 102),
			(3, [72.26747, 46.42257, 32.368374, 80.50209, 5.777631, 98.803314, 7.0915947, 68.62693], "We're testing how consistent your judgments are over a range of visual impressions.", 103),
			(4, [22.098177, 74.10027, 63.634556, 4.710955, 12.405106, 79.39356, 63.014366, 68.67834], "Some pairs are meant to be tricky. Your intuition is part of what we're analyzing.", 104),
			(5, [27.53003, 72.1106, 50.891026, 38.459953, 68.30715, 20.610682, 94.806274, 45.181377], "This data will help us identify patterns in how people perceive attributes.", 105),
			(6, [77.73215, 64.42907, 71.50025, 43.85641, 94.42648, 50.04773, 65.12575, 68.58207], "Sometimes people see entirely different things in the same image.", 106),
			(7, [2.1537063, 82.667885, 16.171143, 71.126656, 5.335274, 40.286068, 11.943586, 3.69409], "Don't worry if you’re unsure. The ambiguity is intentional.", 107),
			(8, [54.435013, 56.800594, 59.335514, 55.829235, 85.46627, 33.388138, 11.076194, 20.480877], "Your reactions help us understand which features people subconsciously favor.", 108),
			(9, [76.197945, 60.623528, 84.229805, 31.652937, 71.82595, 48.04684, 71.29212, 30.282396], "This task isn’t about right answers, but about consistency.", 109);
	"""

	// Inverted-only query with score
	qt_inv_only_score """
		select id, score() as sc
		from t_mow_ann_fulltext
		where comment match_any "illustrates comments answers"
		order by sc desc
		limit 3;
	"""

	// ANN query with a fulltext filter
	qt_ann_with_filter """
		select id,
			   l2_distance_approximate(embedding, [26.360261917114258,7.05784273147583,32.361351013183594,86.39714050292969,58.79527282714844,27.189321517944336,99.38946533203125,80.19270324707031]) as dist
		from t_mow_ann_fulltext
		where comment match_any "illustrates comments answers"
		order by dist
		limit 3;
	"""

	// Hybrid search: combine BM25 score() and ANN distance into a single ranking metric
	// Use a simple combination: 0.7 * score() + 0.3 * (1 / (1 + dist))
	qt_hybrid_search """
		select id, sc, dist, src from (
		    -- TopN by BM25 score (requires MATCH in WHERE, one ORDER BY expr, and LIMIT)
		    select id,
		           score() as sc,
		           cast(null as double) as dist,
		           'score' as src
		    from t_mow_ann_fulltext
		    where comment match_any "illustrates comments answers"
		    order by sc desc
		    limit 3
		) s
		union all
		select id, sc, dist, src from (
		    -- TopN by ANN distance (ascending)
		    select id,
		           cast(null as double) as sc,
		           l2_distance_approximate(embedding, [26.360261917114258,7.05784273147583,32.361351013183594,86.39714050292969,58.79527282714844,27.189321517944336,99.38946533203125,80.19270324707031]) as dist,
		           'ann' as src
		    from t_mow_ann_fulltext
		    order by dist asc
		    limit 3
		) a
		-- Deterministic combined order: source first, then by respective metric
		order by src, coalesce(sc, 0) desc, coalesce(dist, 1e38) asc;
	"""
}