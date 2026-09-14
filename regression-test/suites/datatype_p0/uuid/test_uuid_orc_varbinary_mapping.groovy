import java.nio.file.Files
import java.nio.file.Paths

suite("test_uuid_orc_varbinary_mapping") {
    if (!getFeConfig("enable_outfile_to_local").equalsIgnoreCase("true")) {
        logger.warn("Please set enable_outfile_to_local to true to run test_uuid_orc_varbinary_mapping")
        return
    }
    def backend = sql_return_maparray("SHOW BACKENDS").find { it.Alive == "true" }
    String securePath = sql_return_maparray(
            "SHOW BACKEND CONFIG LIKE 'user_files_secure_path' FROM ${backend.BackendId}")[0].Value
    def outputDirectory = Files.createTempDirectory(Paths.get(securePath), "test_uuid_orc_varbinary_")
    try {
        sql "DROP TABLE IF EXISTS uuid_orc_varbinary_mapping"
        sql """CREATE TABLE uuid_orc_varbinary_mapping (
                   id INT, u UUID, a ARRAY<UUID>, m MAP<UUID,UUID>, s STRUCT<k:UUID>, b STRING)
               DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
               PROPERTIES("replication_num"="1")"""
        sql """INSERT INTO uuid_orc_varbinary_mapping VALUES
               (1, '00112233445566778899aabbccddeeff',
                   ['00112233445566778899aabbccddeeff', NULL],
                   {'00000000000000000000000000000000':'80000000000000000000000000000000',
                    'ffffffffffffffffffffffffffffffff':NULL},
                   {'80000000000000000000000000000000'}, UNHEX('0000ff')),
               (2, '80000000000000000000000000000000', [], {}, {NULL},
                   UNHEX('')),
               (3, 'ffffffffffffffffffffffffffffffff', NULL, NULL, NULL,
                   UNHEX('ffffffffffffffffffffffffffffffff')),
               (4, '00000000000000000000000000000000', [NULL],
                   {'00112233445566778899aabbccddeeff':'00000000000000000000000000000000'},
                   {'00000000000000000000000000000000'}, NULL),
               (5, NULL, NULL, NULL, NULL, NULL)"""
        sql """SELECT id, u, a, m, s, CAST(b AS VARBINARY) AS b
               FROM uuid_orc_varbinary_mapping ORDER BY id
               INTO OUTFILE "file://${outputDirectory}/orc_" FORMAT AS ORC"""
        for (boolean scannerV2 : [false, true]) {
            sql "SET enable_file_scanner_v2 = ${scannerV2}"
            for (boolean binaryMapping : [false, true]) {
                String source = """local("file_path"="${outputDirectory.fileName}/orc_*",
                        "format"="orc", "backend_id"="${backend.BackendId}",
                        "enable_mapping_varbinary"="${binaryMapping}")"""
                "order_qt_schema_${scannerV2}_${binaryMapping}" "DESC FUNCTION ${source}"
                String uuidValue = binaryMapping ? "CAST(HEX(u) AS UUID)" : "u"
                String nestedValue = binaryMapping ? "CAST(HEX(value) AS UUID)" : "value"
                String structValue = binaryMapping ? "CAST(HEX(s.k) AS UUID)" : "s.k"
                "qt_values_${scannerV2}_${binaryMapping}" """SELECT id, ${uuidValue},
                        ARRAY_MAP(value -> ${nestedValue}, a),
                        ARRAY_MAP(value -> ${nestedValue}, MAP_KEYS(m)),
                        ARRAY_MAP(value -> ${nestedValue}, MAP_VALUES(m)),
                        ${structValue}, HEX(b), LENGTH(HEX(b))
                    FROM ${source} ORDER BY id"""
                "qt_filtered_${scannerV2}_${binaryMapping}" """SELECT id, ${uuidValue}, HEX(b), LENGTH(HEX(b))
                    FROM ${source} WHERE u IS NOT NULL AND id > 1 ORDER BY id"""
            }
        }
    } finally {
        assertTrue(outputDirectory.toFile().deleteDir())
    }
}
