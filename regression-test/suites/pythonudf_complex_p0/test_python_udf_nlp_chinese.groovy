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

suite("test_python_udf_nlp_chinese") {
    // Test Chinese NLP processing using jieba library
    // Dependencies: jieba

    def pyPath = """${context.file.parent}/py_udf_complex_scripts/py_udf_complex.zip"""
    scp_udf_file_to_all_be(pyPath)
    def runtime_version = "3.8.10"
    log.info("Python zip path: ${pyPath}".toString())

    try {
        // ========================================
        // Test 1: Chinese Word Segmentation (Accurate Mode)
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_chinese_segment(STRING); """
        sql """
        CREATE FUNCTION py_chinese_segment(STRING)
        RETURNS STRING
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "nlp_chinese.chinese_word_segment",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """

        sql """ DROP TABLE IF EXISTS chinese_text_test; """
        sql """
        CREATE TABLE chinese_text_test (
            id INT,
            content STRING
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """

        sql """
        INSERT INTO chinese_text_test VALUES
        (1, '我爱北京天安门'),
        (2, '今天天气真不错，适合出去玩'),
        (3, '机器学习和深度学习是人工智能的重要分支'),
        (4, '阿里巴巴集团是一家中国互联网公司'),
        (5, NULL);
        """

        qt_chinese_segment """
            SELECT id, content, py_chinese_segment(content) AS segmented
            FROM chinese_text_test
            ORDER BY id;
        """

        // ========================================
        // Test 2: Chinese Word Segmentation (JSON Array Output)
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_chinese_segment_json(STRING); """
        sql """
        CREATE FUNCTION py_chinese_segment_json(STRING)
        RETURNS STRING
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "nlp_chinese.chinese_word_segment_json",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """

        qt_chinese_segment_json """
            SELECT id, py_chinese_segment_json(content) AS words_json
            FROM chinese_text_test
            WHERE id <= 3
            ORDER BY id;
        """

        // ========================================
        // Test 3: Chinese Word Count
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_chinese_word_count(STRING); """
        sql """
        CREATE FUNCTION py_chinese_word_count(STRING)
        RETURNS INT
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "nlp_chinese.chinese_word_count",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """

        qt_chinese_word_count """
            SELECT id, content, py_chinese_word_count(content) AS word_count
            FROM chinese_text_test
            ORDER BY id;
        """

        // ========================================
        // Test 4: Keyword Extraction (TF-IDF)
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_extract_keywords(STRING, INT); """
        sql """
        CREATE FUNCTION py_extract_keywords(STRING, INT)
        RETURNS STRING
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "nlp_chinese.extract_keywords_tfidf",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """

        sql """ DROP TABLE IF EXISTS chinese_article_test; """
        sql """
        CREATE TABLE chinese_article_test (
            id INT,
            article STRING
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """

        sql """
        INSERT INTO chinese_article_test VALUES
        (1, '人工智能是计算机科学的一个分支，它企图了解智能的实质，并生产出一种新的能以人类智能相似的方式做出反应的智能机器。人工智能是研究、开发用于模拟、延伸和扩展人的智能的理论、方法、技术及应用系统的一门新的技术科学。'),
        (2, '机器学习是人工智能的一个重要分支。机器学习是一门研究如何让计算机从数据中自动学习规律的技术。深度学习是机器学习的一个子领域，它使用多层神经网络来学习数据的表示。');
        """

        qt_extract_keywords """
            SELECT id, py_extract_keywords(article, 5) AS top_keywords
            FROM chinese_article_test
            ORDER BY id;
        """

        // ========================================
        // Test 5: Part-of-Speech Tagging
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_pos_tagging(STRING); """
        sql """
        CREATE FUNCTION py_pos_tagging(STRING)
        RETURNS STRING
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "nlp_chinese.pos_tagging",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """

        qt_pos_tagging """
            SELECT id, content, py_pos_tagging(content) AS pos_tags
            FROM chinese_text_test
            WHERE id = 1
            ORDER BY id;
        """

        // ========================================
        // Test 6: Extract Nouns
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_extract_nouns(STRING); """
        sql """
        CREATE FUNCTION py_extract_nouns(STRING)
        RETURNS STRING
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "nlp_chinese.extract_nouns",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """

        qt_extract_nouns """
            SELECT id, content, py_extract_nouns(content) AS nouns
            FROM chinese_text_test
            WHERE id <= 4
            ORDER BY id;
        """

        // ========================================
        // Test 7: Extract Verbs
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_extract_verbs(STRING); """
        sql """
        CREATE FUNCTION py_extract_verbs(STRING)
        RETURNS STRING
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "nlp_chinese.extract_verbs",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """

        qt_extract_verbs """
            SELECT id, content, py_extract_verbs(content) AS verbs
            FROM chinese_text_test
            WHERE id <= 4
            ORDER BY id;
        """

        // ========================================
        // Test 8: Named Entity Recognition
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_extract_entities(STRING); """
        sql """
        CREATE FUNCTION py_extract_entities(STRING)
        RETURNS STRING
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "nlp_chinese.extract_all_entities",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """

        sql """ DROP TABLE IF EXISTS chinese_ner_test; """
        sql """
        CREATE TABLE chinese_ner_test (
            id INT,
            text STRING
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """

        sql """
        INSERT INTO chinese_ner_test VALUES
        (1, '张三和李四昨天去了北京参加阿里巴巴的会议'),
        (2, '马云创立了阿里巴巴集团，总部位于杭州'),
        (3, '特斯拉CEO马斯克访问了上海工厂');
        """

        qt_extract_entities """
            SELECT id, text, py_extract_entities(text) AS entities
            FROM chinese_ner_test
            ORDER BY id;
        """

        // ========================================
        // Test 9: Chinese Text Similarity
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_chinese_similarity(STRING, STRING); """
        sql """
        CREATE FUNCTION py_chinese_similarity(STRING, STRING)
        RETURNS DOUBLE
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "nlp_chinese.chinese_text_similarity",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """

        sql """ DROP TABLE IF EXISTS chinese_similarity_test; """
        sql """
        CREATE TABLE chinese_similarity_test (
            id INT,
            text1 STRING,
            text2 STRING
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """

        sql """
        INSERT INTO chinese_similarity_test VALUES
        (1, '今天天气很好', '今天的天气非常好'),
        (2, '我喜欢吃苹果', '我喜欢吃香蕉'),
        (3, '机器学习很重要', '计算机视觉很有趣'),
        (4, '人工智能', '人工智能');
        """

        qt_chinese_similarity """
            SELECT id, text1, text2, py_chinese_similarity(text1, text2) AS similarity
            FROM chinese_similarity_test
            ORDER BY id;
        """

        // ========================================
        // Test 10: Sentence Splitting
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_split_sentences(STRING); """
        sql """
        CREATE FUNCTION py_split_sentences(STRING)
        RETURNS STRING
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "nlp_chinese.split_sentences",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """

        qt_split_sentences """
            SELECT id, py_split_sentences(article) AS sentences
            FROM chinese_article_test
            WHERE id = 1;
        """

    } finally {
        try_sql("DROP FUNCTION IF EXISTS py_chinese_segment(STRING);")
        try_sql("DROP FUNCTION IF EXISTS py_chinese_segment_json(STRING);")
        try_sql("DROP FUNCTION IF EXISTS py_chinese_word_count(STRING);")
        try_sql("DROP FUNCTION IF EXISTS py_extract_keywords(STRING, INT);")
        try_sql("DROP FUNCTION IF EXISTS py_pos_tagging(STRING);")
        try_sql("DROP FUNCTION IF EXISTS py_extract_nouns(STRING);")
        try_sql("DROP FUNCTION IF EXISTS py_extract_verbs(STRING);")
        try_sql("DROP FUNCTION IF EXISTS py_extract_entities(STRING);")
        try_sql("DROP FUNCTION IF EXISTS py_chinese_similarity(STRING, STRING);")
        try_sql("DROP FUNCTION IF EXISTS py_split_sentences(STRING);")
        try_sql("DROP TABLE IF EXISTS chinese_text_test;")
        try_sql("DROP TABLE IF EXISTS chinese_article_test;")
        try_sql("DROP TABLE IF EXISTS chinese_ner_test;")
        try_sql("DROP TABLE IF EXISTS chinese_similarity_test;")
    }
}
