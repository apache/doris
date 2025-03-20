suite("test_validate_restore_alias", "validate_restore") {
    def runValidateRestoreAlias = { String version ->
        String validateSuiteName = "test_backup_restore_alias"
        String dbName = "${validateSuiteName}_db_${version.replace('.', '_')}"
        String tableName = "${validateSuiteName}_table"
        String aliasName = "${validateSuiteName}_table_alias"
        String snapshotName = "test_backup_restore_snapshot"

        def syncer = getSyncer()
        def repoName = syncer.createS3ValidateRepository(validateSuiteName, version)

        try {
            sql "CREATE DATABASE IF NOT EXISTS ${dbName}"
            sql "DROP TABLE IF EXISTS ${dbName}.${tableName}"
            sql "DROP TABLE IF EXISTS ${dbName}.${aliasName}"

            def snapshot = syncer.getSnapshotTimestamp(repoName, snapshotName)
            assertTrue(snapshot != null)

            sql """
            RESTORE SNAPSHOT ${dbName}.${snapshotName}
            FROM `${repoName}`
            ON ( `${tableName}`)
            PROPERTIES
            (
                "backup_timestamp" = "${snapshot}",
                "reserve_replica" = "true"
            )
            """

            syncer.waitAllRestoreFinish(dbName)

            sql """
            RESTORE SNAPSHOT ${dbName}.${snapshotName}
            FROM `${repoName}`
            ON ( `${tableName}` AS `${aliasName}` )
            PROPERTIES
            (
                "backup_timestamp" = "${snapshot}",
                "reserve_replica" = "true"
            )
            """

            syncer.waitAllRestoreFinish(dbName)

            qt_select "SELECT * FROM ${dbName}.${tableName} ORDER BY id"
            qt_select "SELECT * FROM ${dbName}.${aliasName} ORDER BY id"

            sql "INSERT INTO ${dbName}.${tableName} VALUES (20, 21), (123, 341)"
            qt_select "SELECT * FROM ${dbName}.${tableName} ORDER BY id"

            sql "INSERT INTO ${dbName}.${aliasName} VALUES (20, 21), (123, 341)"
            qt_select "SELECT * FROM ${dbName}.${aliasName} ORDER BY id"

        } finally {
            sql "DROP TABLE IF EXISTS ${dbName}.${tableName} FORCE"
            sql "DROP DATABASE ${dbName} FORCE"
            sql "DROP REPOSITORY `${repoName}`"
        }
    }

    runValidateRestoreAlias("3.0")
}
