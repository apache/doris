suite("test_similar_query_with_different_results") {
    def tableName = "t0"

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
                c0 BOOLEAN DEFAULT 'false'
            )
            DISTRIBUTED BY RANDOM
            PROPERTIES ("replication_num" = "1")
        """

    sql """ INSERT INTO ${tableName} (c0) VALUES 
            (DEFAULT),('false'),('true'),(DEFAULT),('true'),(DEFAULT),(DEFAULT),('false')
        """
    qt_select1 """ SELECT c0 FROM ${tableName} """
    qt_select2 """ SELECT c0 FROM ${tableName} WHERE c0 """
    qt_select3 """ SELECT c0 FROM ${tableName} WHERE (NOT c0) """
    qt_select4 """ SELECT c0 FROM ${tableName} WHERE ((c0) IS NULL) """
    qt_select5 """ SELECT c0 FROM ${tableName} WHERE c0
                   UNION ALL
                   SELECT c0 FROM ${tableName} WHERE (NOT c0)
                   UNION ALL
                   SELECT c0 FROM ${tableName} WHERE ((c0) IS NULL)
               """

    sql""" DROP TABLE IF EXISTS ${tableName} """
}
