import org.apache.doris.regression.suite.ClusterOptions
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.kms.KmsClient
import software.amazon.awssdk.services.kms.model.ScheduleKeyDeletionRequest

suite("test_restart_when_rotating", "docker") {
    def tdeAlgorithm = ["AES256", "SM4"]
    def cloudMode = [/*false,*/ true]
    cloudMode.each { mode ->
        tdeAlgorithm.each { algorithm ->
            def options = new ClusterOptions()
            options.cloudMode = mode
            options.enableDebugPoints()
            options.feConfigs += [
                    'cloud_cluster_check_interval_second=1',
                    'sys_log_verbose_modules=org',
                    "doris_tde_key_endpoint=${context.config.tdeKeyEndpoint}",
                    "doris_tde_key_region=${context.config.tdeKeyRegion}",
                    "doris_tde_key_provider=${context.config.tdeKeyProvider}",
                    "doris_tde_algorithm=${algorithm}",
                    "doris_tde_key_id=${context.config.tdeKeyId}"
            ]
            options.tdeAk = context.config.tdeAk
            options.tdeSk = context.config.tdeSk
            options.enableDebugPoints()

            docker(options) {
                def tblName = "test_restart_when_rotating"
                sql """ DROP TABLE IF EXISTS ${tblName} """
                sql """
                CREATE TABLE IF NOT EXISTS ${tblName} (
                    `k` int NOT NULL,
                    `v` varchar(10) NOT NULL) 
                DUPLICATE KEY(`k`)
                DISTRIBUTED BY HASH(`k`) BUCKETS 8
                PROPERTIES (
                    "replication_allocation" = "tag.location.default: 1"
                )
                """;

                (1..10).each { i ->
                    sql """ INSERT INTO ${tblName} VALUES (${i}, "${i}") """
                }
                qt_sql """ SELECT * FROM ${tblName} ORDER BY `k` """

                def keys = sql """ SELECT * FROM information_schema.encryption_keys ORDER BY ID """;
                {
                    def credProvider = StaticCredentialsProvider.create(
                            AwsBasicCredentials.create(context.config.tdeAk, context.config.tdeSk)
                    );
                    def client = KmsClient.builder()
                            .region(Region.of(context.config.tdeKeyRegion))
                            .endpointOverride(URI.create(context.config.tdeKeyEndpoint))
                            .credentialsProvider(credProvider)
                            .build();

                    def resp = client.createKey()
                    def keyId = resp.keyMetadata().keyId()
                    try {
                        GetDebugPoint().enableDebugPointForAllFEs("KeyManager.stopAfterOneMasterKeyChanged")
                        def t = Thread.start {
                            try {
                                sql """ ADMIN ROTATE TDE ROOT KEY PROPERTIES(
                                    "doris_tde_key_provider" = "aws_kms",
                                    "doris_tde_key_id" = "${keyId}",
                                    "doris_tde_key_endpoint" = "${context.config.tdeKeyEndpoint}",
                                    "doris_tde_key_region" = "${context.config.tdeKeyRegion}"
                                    ) 
                                """
                            } catch (Exception ignored) {
                                // do nothing
                            }
                        }
                        sleep(3000)

                        cluster.restartFrontends()
                        sleep(30000)
                        context.reconnectFe()

                        (1..10).each { i ->
                            sql """ INSERT INTO ${tblName} VALUES (${i}, "${i}") """
                        }
                        qt_sql """ SELECT * FROM ${tblName} ORDER BY `k` """

                        def newKeys = sql """ SELECT * FROM information_schema.encryption_keys ORDER BY ID"""
                        println(keys)
                        println(newKeys)
                        assertEquals(keys[0][6], newKeys[0][6])
                        keys = newKeys
                    } finally {
                        // delete cmk id
                        def deleteReq = ScheduleKeyDeletionRequest.builder().keyId(keyId).build();
                        client.scheduleKeyDeletion((ScheduleKeyDeletionRequest) deleteReq)
                        GetDebugPoint().disableDebugPointForAllFEs("KeyManager.stopAfterOneMasterKeyChanged")
                    }
                }

                {
                    def credProvider = StaticCredentialsProvider.create(
                            AwsBasicCredentials.create(context.config.tdeAk, context.config.tdeSk)
                    );
                    def client = KmsClient.builder()
                            .region(Region.of(context.config.tdeKeyRegion))
                            .endpointOverride(URI.create(context.config.tdeKeyEndpoint))
                            .credentialsProvider(credProvider)
                            .build();

                    def resp = client.createKey()
                    def keyId = resp.keyMetadata().keyId()
                    try {
                        GetDebugPoint().enableDebugPointForAllFEs("KeyManager.stopAfterAllMasterKeyChanged")
                        def t = Thread.start {
                            try {
                                sql """ ADMIN ROTATE TDE ROOT KEY PROPERTIES(
                                    "doris_tde_key_provider" = "aws_kms",
                                    "doris_tde_key_id" = "${keyId}",
                                    "doris_tde_key_endpoint" = "${context.config.tdeKeyEndpoint}",
                                    "doris_tde_key_region" = "${context.config.tdeKeyRegion}"
                                    ) 
                                """
                            } catch (Exception ignored) {
                                // do nothing
                            }
                        }
                        sleep(3000)

                        cluster.restartFrontends()
                        sleep(30000)
                        context.reconnectFe()

                        (1..10).each { i ->
                            sql """ INSERT INTO ${tblName} VALUES (${i}, "${i}") """
                        }
                        qt_sql """ SELECT * FROM ${tblName} ORDER BY `k` """

                        def newKeys = sql """ SELECT * FROM information_schema.encryption_keys ORDER BY ID """
                        assertEquals(keys[0][6], newKeys[0][6])
                        keys = newKeys
                    } finally {
                        // delete cmk id
                        def deleteReq = ScheduleKeyDeletionRequest.builder().keyId(keyId).build();
                        client.scheduleKeyDeletion((ScheduleKeyDeletionRequest) deleteReq)
                        GetDebugPoint().disableDebugPointForAllFEs("KeyManager.stopAfterAllMasterKeyChanged")
                    }
                }

                {
                    def credProvider = StaticCredentialsProvider.create(
                            AwsBasicCredentials.create(context.config.tdeAk, context.config.tdeSk)
                    );
                    def client = KmsClient.builder()
                            .region(Region.of(context.config.tdeKeyRegion))
                            .endpointOverride(URI.create(context.config.tdeKeyEndpoint))
                            .credentialsProvider(credProvider)
                            .build();

                    def resp = client.createKey()
                    def keyId = resp.keyMetadata().keyId()
                    try {
                        GetDebugPoint().enableDebugPointForAllFEs("KeyManager.stopAfterRotateEditLogWritten")
                        def t = Thread.start {
                            try {
                                sql """ ADMIN ROTATE TDE ROOT KEY PROPERTIES(
                                    "doris_tde_key_provider" = "aws_kms",
                                    "doris_tde_key_id" = "${keyId}",
                                    "doris_tde_key_endpoint" = "${context.config.tdeKeyEndpoint}",
                                    "doris_tde_key_region" = "${context.config.tdeKeyRegion}"
                                    ) 
                                """
                            } catch (Exception ignored) {
                                // do nothing
                            }
                        }
                        sleep(3000)

                        cluster.restartFrontends()
                        sleep(30000)
                        context.reconnectFe()

                        (1..10).each { i ->
                            sql """ INSERT INTO ${tblName} VALUES (${i}, "${i}") """
                        }
                        qt_sql """ SELECT * FROM ${tblName} ORDER BY `k` """

                        def newKeys = sql """ SELECT * FROM information_schema.encryption_keys ORDER BY ID """
                        assertNotEquals(keys[0][6], newKeys[0][6])
                    } finally {
                        // delete cmk id
                        def deleteReq = ScheduleKeyDeletionRequest.builder().keyId(keyId).build();
                        client.scheduleKeyDeletion((ScheduleKeyDeletionRequest) deleteReq)
                        GetDebugPoint().disableDebugPointForAllFEs("KeyManager.stopAfterRotateEditLogWritten")
                    }
                }

                qt_sql """ SELECT * FROM ${tblName} ORDER BY `k` """
            }
        }
    }
}
