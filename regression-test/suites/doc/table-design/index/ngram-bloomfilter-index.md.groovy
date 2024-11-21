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

import org.junit.jupiter.api.Assertions

suite("docs/table-design/index/ngram-bloomfilter-index.md") {
    try {
        sql "DROP TABLE IF EXISTS `amazon_reviews`"
        sql """
        CREATE TABLE `amazon_reviews` (  
          `review_date` int(11) NULL,  
          `marketplace` varchar(20) NULL,  
          `customer_id` bigint(20) NULL,  
          `review_id` varchar(40) NULL,
          `product_id` varchar(10) NULL,
          `product_parent` bigint(20) NULL,
          `product_title` varchar(500) NULL,
          `product_category` varchar(50) NULL,
          `star_rating` smallint(6) NULL,
          `helpful_votes` int(11) NULL,
          `total_votes` int(11) NULL,
          `vine` boolean NULL,
          `verified_purchase` boolean NULL,
          `review_headline` varchar(500) NULL,
          `review_body` string NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`review_date`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`review_date`) BUCKETS 16
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "compression" = "ZSTD"
        )
        """

        var f = new File("amazon_reviews_2010.snappy.parquet")
        if (!f.exists()) {
            f.delete()
        }
        cmd("wget ${getS3Url()}/regression/doc/amazon_reviews_2010.snappy.parquet")
        cmd("""curl --location-trusted -u ${context.config.jdbcUser}:${context.config.jdbcPassword} -T amazon_reviews_2010.snappy.parquet -H "format:parquet" http://${context.config.feHttpAddress}/api/${curDbName}/amazon_reviews/_stream_load""")

        sql " SELECT COUNT() FROM amazon_reviews "
        sql """
            SELECT
                product_id,
                any(product_title),
                AVG(star_rating) AS rating,
                COUNT() AS count
            FROM
                amazon_reviews
            WHERE
                review_body LIKE '%is super awesome%'
            GROUP BY
                product_id
            ORDER BY
                count DESC,
                rating DESC,
                product_id
            LIMIT 5
        """
        sql """ ALTER TABLE amazon_reviews ADD INDEX review_body_ngram_idx(review_body) USING NGRAM_BF PROPERTIES("gram_size"="10", "bf_size"="10240") """
    } catch (Throwable t) {
        Assertions.fail("examples in docs/table-design/index/ngram-bloomfilter-index.md failed to exec, please fix it", t)
    }
}
