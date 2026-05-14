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

import org.junit.Assert

suite("test_pythonudf_volatility") {
    def runtimeVersion = getPythonUdfRuntimeVersion()
    def functions = [
        "py_vol_immutable",
        "py_vol_stable",
        "py_vol_volatile",
        "py_vol_default"
    ]
    def materializedViews = [
        "py_vol_immutable_mv",
        "py_vol_stable_mv",
        "py_vol_volatile_mv",
        "py_vol_default_mv"
    ]

    materializedViews.each { mv ->
        sql """ DROP MATERIALIZED VIEW IF EXISTS ${mv}; """
    }
    sql """ DROP TABLE IF EXISTS py_vol_tbl; """
    functions.each { fn ->
        sql """ DROP FUNCTION IF EXISTS ${fn}(INT); """
    }

    sql """
    CREATE TABLE py_vol_tbl (
        k INT
    )
    DISTRIBUTED BY HASH(k) BUCKETS 1
    PROPERTIES("replication_num" = "1");
    """
    sql """ INSERT INTO py_vol_tbl VALUES (1), (2); """

    sql """
    CREATE FUNCTION py_vol_immutable(INT)
    RETURNS INT
    PROPERTIES (
        "type" = "PYTHON_UDF",
        "symbol" = "evaluate",
        "runtime_version" = "${runtimeVersion}",
        "volatility" = "immutable"
    )
AS \$\$
def evaluate(x):
    if x is None:
        return None
    return x + 1
\$\$;
    """

    sql """
    CREATE FUNCTION py_vol_stable(INT)
    RETURNS INT
    PROPERTIES (
        "type" = "PYTHON_UDF",
        "symbol" = "evaluate",
        "runtime_version" = "${runtimeVersion}",
        "volatility" = "stable"
    )
AS \$\$
def evaluate(x):
    if x is None:
        return None
    return x + 2
\$\$;
    """

    sql """
    CREATE FUNCTION py_vol_volatile(INT)
    RETURNS INT
    PROPERTIES (
        "type" = "PYTHON_UDF",
        "symbol" = "evaluate",
        "runtime_version" = "${runtimeVersion}",
        "volatility" = "volatile"
    )
AS \$\$
def evaluate(x):
    if x is None:
        return None
    return x + 3
\$\$;
    """

    sql """
    CREATE FUNCTION py_vol_default(INT)
    RETURNS INT
    PROPERTIES (
        "type" = "PYTHON_UDF",
        "symbol" = "evaluate",
        "runtime_version" = "${runtimeVersion}"
    )
AS \$\$
def evaluate(x):
    if x is None:
        return None
    return x + 4
\$\$;
    """

    def result = sql """
        SELECT
            py_vol_immutable(1),
            py_vol_stable(1),
            py_vol_volatile(1),
            py_vol_default(1);
    """
    Assert.assertEquals([[2, 3, 4, 5]], result)

    def showCreateResult = sql """ SHOW CREATE FUNCTION py_vol_immutable(INT); """
    assertTrue(showCreateResult.size() == 1)
    def replaySql = showCreateResult[0][1].toString()
    assertTrue(replaySql.contains("\"RUNTIME_VERSION\"=\"${runtimeVersion}\""))
    assertTrue(replaySql.contains("\"VOLATILITY\"=\"immutable\""))
    assertTrue(replaySql.contains("AS \$\$"))
    assertTrue(replaySql.contains("return x + 1"))

    sql """ DROP FUNCTION py_vol_immutable(INT); """
    sql replaySql
    result = sql """ SELECT py_vol_immutable(1); """
    Assert.assertEquals([[2]], result)

    explain {
        sql "logical plan SELECT * FROM py_vol_tbl WHERE py_vol_immutable(k) IN (1, k + 1)"
        contains "OR["
        notContains " IN "
    }

    explain {
        sql "logical plan SELECT * FROM py_vol_tbl WHERE py_vol_stable(k) IN (1, k + 2)"
        contains "OR["
        notContains " IN "
    }

    explain {
        sql "logical plan SELECT * FROM py_vol_tbl WHERE py_vol_volatile(k) IN (1, k + 3)"
        contains " IN "
        notContains "OR["
    }

    explain {
        sql "logical plan SELECT * FROM py_vol_tbl WHERE py_vol_default(k) IN (1, k + 4)"
        contains " IN "
        notContains "OR["
    }

    result = sql """
        SELECT py_vol_volatile(k), COUNT(*)
        FROM py_vol_tbl
        GROUP BY py_vol_volatile(k)
        ORDER BY 1;
    """
    Assert.assertEquals("[[4, 1], [5, 1]]", result.toString())

    createMV("""
        CREATE MATERIALIZED VIEW py_vol_immutable_mv
        AS SELECT py_vol_immutable(k) AS v FROM py_vol_tbl;
    """)

    test {
        sql """
            CREATE MATERIALIZED VIEW py_vol_stable_mv
            AS SELECT py_vol_stable(k) AS v_stable FROM py_vol_tbl;
        """
        exception "can not contain nonDeterministic expression or unnest"
    }

    test {
        sql """
            CREATE MATERIALIZED VIEW py_vol_volatile_mv
            AS SELECT py_vol_volatile(k) AS v_volatile FROM py_vol_tbl;
        """
        exception "can not contain nonDeterministic expression or unnest"
    }

    test {
        sql """
            CREATE MATERIALIZED VIEW py_vol_default_mv
            AS SELECT py_vol_default(k) AS v_default FROM py_vol_tbl;
        """
        exception "can not contain nonDeterministic expression or unnest"
    }
}
