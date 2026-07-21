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

suite("test_aggregate_percentile_approx_array") {
    sql "DROP TABLE IF EXISTS percentile_approx_array_test"
    sql """
        CREATE TABLE percentile_approx_array_test (
            group_id INT,
            value DOUBLE NULL
        )
        DUPLICATE KEY(group_id)
        DISTRIBUTED BY HASH(group_id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """
        INSERT INTO percentile_approx_array_test VALUES
            (1, 1.0), (1, 2.0), (1, 3.0), (1, NULL),
            (2, 10.0), (2, 20.0), (2, 30.0), (2, 100.0),
            (3, NULL)
    """

    qt_percentile_approx_array_1 """
        SELECT percentile_approx_array(value, [0.0, 0.25, 0.5, 0.75, 1.0])
        FROM percentile_approx_array_test
    """

    qt_percentile_approx_array_2 """
        SELECT percentile_approx_array(value, [0.25, 0.5, 0.75], 2048)
        FROM percentile_approx_array_test
    """

    qt_percentile_approx_array_3 """
        SELECT percentile_approx_array(value, [0.9, 0.1, 0.5, 0.5])
        FROM percentile_approx_array_test
    """

    qt_percentile_approx_array_4 """
        SELECT
            percentile_approx_array(value, [0.25, 0.5, 0.75]),
            percentile_approx_array(value, [0.25, 0.5, 0.75], 10001)
        FROM percentile_approx_array_test
    """

    qt_percentile_approx_array_5 """
        SELECT percentile_approx_array(10, [0.0, 0.5, 1.0])
        FROM percentile_approx_array_test
    """

    qt_percentile_approx_array_6 """
        SELECT percentile_approx_array(CAST(NULL AS DOUBLE), [0.0, 0.5, 1.0])
        FROM percentile_approx_array_test
    """

    order_qt_percentile_approx_array_7 """
        SELECT group_id, percentile_approx_array(value, [0.0, 0.5, 1.0])
        FROM percentile_approx_array_test
        GROUP BY group_id
        ORDER BY group_id
    """

    qt_percentile_approx_array_8 """
        SELECT percentile_approx_array(value, [])
        FROM percentile_approx_array_test
    """

    qt_percentile_approx_array_9 """
        SELECT
            percentile_approx_array(value, [0.25, 0.5, 0.75]),
            percentile_approx_array(value, [0.25, 0.5, 0.75], CAST('NaN' AS DOUBLE)),
            percentile_approx_array(value, [0.25, 0.5, 0.75], CAST('Infinity' AS DOUBLE)),
            percentile_approx_array(value, [0.25, 0.5, 0.75], CAST('-Infinity' AS DOUBLE))
        FROM percentile_approx_array_test
    """

    explain {
        sql """
            SELECT percentile_approx_array(value, [0.25, 0.5, 0.75])
            FROM percentile_approx_array_test
        """
        notContains "cast(value as DOUBLE)"
    }

    test {
        sql """
            SELECT percentile_approx_array(value, array(value / 100.0, 0.5))
            FROM percentile_approx_array_test
        """
        exception "percentile_approx_array requires second parameter must be a constant"
    }

    test {
        sql """
            SELECT percentile_approx_array(value, [0.5, NULL])
            FROM percentile_approx_array_test
        """
        exception "percentile_approx_array quantile should not be null"
    }

    test {
        sql """
            SELECT percentile_approx_array(value, [0.5, 1.1])
            FROM percentile_approx_array_test
        """
        exception "quantile"
    }

    test {
        sql """
            SELECT percentile_approx_array(value, array(CAST('NaN' AS DOUBLE)))
            FROM percentile_approx_array_test
        """
        exception "quantile must be in [0, 1]"
    }

    test {
        sql """
            SELECT percentile_approx_array(value, array(CAST('Infinity' AS DOUBLE)))
            FROM percentile_approx_array_test
        """
        exception "quantile must be in [0, 1]"
    }

    test {
        sql """
            SELECT percentile_approx_array(value, array(CAST('-Infinity' AS DOUBLE)))
            FROM percentile_approx_array_test
        """
        exception "quantile must be in [0, 1]"
    }

    test {
        sql """
            SELECT percentile_approx_array(value, [0.5], value)
            FROM percentile_approx_array_test
        """
        exception "percentile_approx_array requires the third parameter must be a constant"
    }
}
