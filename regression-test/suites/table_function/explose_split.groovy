def tableName = "test_lv_str"

sql """ DROP TABLE IF EXISTS ${tableName} """
sql """
    CREATE TABLE ${tableName} 
    (k1 INT, k2 STRING) 
    UNIQUE KEY(k1) DISTRIBUTED BY HASH(k1) BUCKETS 8  
    PROPERTIES("replication_num" = "1")
"""

sql """ INSERT INTO ${tableName} VALUES (1, 'a,b,c') """

sql """ set enable_lateral_view = true """

// not_vectorized
qt_explose_split """ select * from ${tableName} 
                    lateral view explode_split(k2, ',') tmp1 as e1 """

qt_explose_split """ select * from ${tableName}
                    lateral view explode_split(k2, ',') tmp1 as e1 
                    lateral view explode_split(k2, ',') tmp2 as e2 """

// vectorized
sql """ set enable_vectorized_engine = true """

qt_explose_split """ select * from ${tableName} 
                    lateral view explode_split(k2, ',') tmp1 as e1 """

qt_explose_split """ select * from ${tableName}
                    lateral view explode_split(k2, ',') tmp1 as e1 
                    lateral view explode_split(k2, ',') tmp2 as e2 """

