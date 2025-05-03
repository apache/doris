suite("test_show_create_load_command", "nereids_p0") {
    def dbName = "test_show_create_load_db"
    def tableName = "test_show_create_load_table"
    def label = "test_load_label"

    // File path where the test data will be stored
    def dataFilePath = """${context.dataPath}/test_show_create_load_data"""

    try {
        sql """CREATE DATABASE IF NOT EXISTS ${dbName}"""

        sql """DROP TABLE IF EXISTS ${dbName}.${tableName}"""

        // Create a data file dynamically
        def dataContent = """1,Alice
2,Bob
3,Charlie
"""
        new File(dataFilePath).withWriter('UTF-8') { writer ->
            writer.write(dataContent)
        }

        sql """
            CREATE TABLE IF NOT EXISTS ${dbName}.${tableName} (
                id INT,
                name STRING
            )
            DISTRIBUTED BY HASH(id) BUCKETS 3
            PROPERTIES ("replication_num" = "1");
        """

        sql """
            LOAD LABEL ${dbName}.${label}
            (
                DATA INFILE ("file:${dataFilePath}")
                INTO TABLE ${tableName}
                COLUMNS TERMINATED BY ","
            )
            PROPERTIES
            (
                "timeout" = "3600"
            );
        """

        // Test the SHOW CREATE LOAD command
        checkNereidsExecute("""SHOW CREATE LOAD FOR ${label}""")
        qt_cmd("""SHOW CREATE LOAD FOR ${label}""")
    } finally {
        sql """DROP TABLE IF EXISTS ${dbName}.${tableName}"""
        sql """DROP DATABASE IF EXISTS ${dbName}"""
        
        // Delete the data file
        def dataFile = new File(dataFilePath)
        if (dataFile.exists()) {
            dataFile.delete()
        }
    }
}
