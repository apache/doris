// The cases is copied from 
// https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-lateral-view.html
// and modified by Doris.

def tableName = "person"

sql """ DROP TABLE IF EXISTS ${tableName} """
sql """
    CREATE TABLE ${tableName} 
    (id INT, name STRING, age INT, class INT, address STRING) 
    UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 8  
    PROPERTIES("replication_num" = "1")
"""

sql """ INSERT INTO ${tableName} VALUES
    (100, 'John', 30, 1, 'Street 1'),
    (200, 'Mary', NULL, 1, 'Street 2'),
    (300, 'Mike', 80, 3, 'Street 3'),
    (400, 'Dan', 50, 4, 'Street 4')  """

sql """ set enable_lateral_view = true """

// not vectorized
qt_explose_json_array """ SELECT * FROM ${tableName} 
                    LATERAL VIEW EXPLODE_JSON_ARRAY_INT('[30, 60]') t1 as c_age 
                    LATERAL VIEW EXPLODE_JSON_ARRAY_INT('[40, 80]') t2 as d_age 
                    ORDER BY id, c_age, d_age """

qt_explose_json_array """ SELECT c_age, COUNT(1) FROM ${tableName}
                    LATERAL VIEW EXPLODE_JSON_ARRAY_INT('[30, 60]') t1 as c_age 
                    LATERAL VIEW EXPLODE_JSON_ARRAY_INT('[40, 80]') t2 as d_age 
                    GROUP BY c_age ORDER BY c_age """

qt_explose_json_array """ SELECT * FROM ${tableName}
                        LATERAL VIEW EXPLODE_JSON_ARRAY_INT('[]') t1 AS c_age 
                        ORDER BY id, c_age """

qt_explose_json_array """ SELECT * FROM ${tableName}
                    LATERAL VIEW EXPLODE_JSON_ARRAY_STRING('[1, "b", 3]') t1 as c 
                    LATERAL VIEW EXPLODE_JSON_ARRAY_DOUBLE('[1.23, 22.214, 214.1]') t2 as d 
                    ORDER BY id, c, d """

// vectorized
sql """ set enable_vectorized_engine = true """

qt_explose_json_array """ select @@enable_vectorized_engine """
qt_explose_json_array """ SELECT * FROM ${tableName} 
                    LATERAL VIEW EXPLODE_JSON_ARRAY_INT('[30, 60]') t1 as c_age 
                    LATERAL VIEW EXPLODE_JSON_ARRAY_INT('[40, 80]') t2 as d_age 
                    ORDER BY id, c_age, d_age """

qt_explose_json_array """ SELECT c_age, COUNT(1) FROM ${tableName}
                    LATERAL VIEW EXPLODE_JSON_ARRAY_INT('[30, 60]') t1 as c_age 
                    LATERAL VIEW EXPLODE_JSON_ARRAY_INT('[40, 80]') t2 as d_age 
                    GROUP BY c_age ORDER BY c_age """

qt_explose_json_array """ SELECT * FROM ${tableName}
                        LATERAL VIEW EXPLODE_JSON_ARRAY_INT('[]') t1 AS c_age 
                        ORDER BY id, c_age """

qt_explose_json_array """ SELECT * FROM ${tableName}
                    LATERAL VIEW EXPLODE_JSON_ARRAY_STRING('[1, "b", 3]') t1 as c 
                    LATERAL VIEW EXPLODE_JSON_ARRAY_DOUBLE('[1.23, 22.214, 214.1]') t2 as d 
                    ORDER BY id, c, d """