import org.apache.doris.regression.suite.ClusterOptions
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.kms.KmsClient
import software.amazon.awssdk.services.kms.model.ScheduleKeyDeletionRequest

suite("test_rotate_root_key", "docker") {
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

            docker(options) {
                def tblName = "test_rotate_root_key"
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
                """

                test {
                    sql """ ADMIN ROTATE TDE ROOT KEY PROPERTIES(
                            "doris_tde_key_provider" = "aws_kms",
                            "doris_tde_key_id" = "wrong_key_id",
                            "doris_tde_key_endpoint" = "${context.config.tdeKeyEndpoint}",
                            "doris_tde_key_region" = "${context.config.tdeKeyRegion}"
                            ) 
                    """
                    exception("describeKey failed")
                }

                test {
                    sql """ ADMIN ROTATE TDE ROOT KEY PROPERTIES(
                            "doris_tde_key_provider" = "aws_kms",
                            "doris_tde_key_id" = "${context.config.tdeKeyId}",
                            "doris_tde_key_endpoint" = "https://kms.us-east-2.amazonaws.com",
                            "doris_tde_key_region" = "wrong_region"
                            ) 
                    """
                    exception("describeKey failed")
                }

                test {
                    sql """ ADMIN ROTATE TDE ROOT KEY PROPERTIES(
                            "doris_tde_key_provider" = "local"
                            ) 
                    """
                    exception("Local key provider is currently unsupported")
                }

                test {
                    sql """ ADMIN ROTATE TDE ROOT KEY PROPERTIES(
                            "doris_tde_key_provider" = "aws_kms",
                            "doris_tde_key_id" = "${context.config.tdeKeyId}",
                            "doris_tde_key_endpoint" = "${context.config.tdeKeyEndpoint}",
                            "doris_tde_key_region" = "${context.config.tdeKeyRegion}",
                            "xxx" = "xxx"
                            ) 
                    """
                    exception("unknown properties")
                }

                def keys = sql """ SELECT * FROM information_schema.encryption_keys """

                (1..10).each { i ->
                    sql """ INSERT INTO ${tblName} VALUES (${i}, "${i}") """
                }

                qt_sql """ SELECT * FROM ${tblName} ORDER BY `k` """

                sql """ ADMIN ROTATE TDE ROOT KEY """

                (1..10).each { i ->
                    sql """ INSERT INTO ${tblName} VALUES (${i}, "${i}") """
                }

                qt_sql """ SELECT * FROM ${tblName} ORDER BY `k` """;

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
                        sql """ ADMIN ROTATE TDE ROOT KEY PROPERTIES(
                                "doris_tde_key_provider" = "aws_kms",
                                "doris_tde_key_id" = "${keyId}",
                                "doris_tde_key_endpoint" = "${context.config.tdeKeyEndpoint}",
                                "doris_tde_key_region" = "${context.config.tdeKeyRegion}"
                                ) 
                           """

                        (1..10).each { i ->
                            sql """ INSERT INTO ${tblName} VALUES (${i}, "${i}") """
                        }

                        qt_sql """ SELECT * FROM ${tblName} ORDER BY `k` """
                        def newKeys = sql """ SELECT * FROM information_schema.encryption_keys """
                        assertNotEquals(keys[0][6], newKeys[0][6])
                        keys = newKeys
                    } finally {
                        // delete cmk id
                        def deleteReq = ScheduleKeyDeletionRequest.builder().keyId(keyId).build();
                        client.scheduleKeyDeletion((ScheduleKeyDeletionRequest) deleteReq)
                    }
                }

                {
                    def credProvider = StaticCredentialsProvider.create(
                            AwsBasicCredentials.create(context.config.tdeAk, context.config.tdeSk)
                    );
                    def client = KmsClient.builder()
                            .region(Region.of("us-east-2"))
                            .endpointOverride(URI.create("https://kms.us-east-2.amazonaws.com"))
                            .credentialsProvider(credProvider)
                            .build();
                    def resp = client.createKey()
                    def keyId = resp.keyMetadata().keyId()
                    try {
                        sql """ ADMIN ROTATE TDE ROOT KEY PROPERTIES(
                                    "doris_tde_key_provider" = "aws_kms",
                                    "doris_tde_key_id" = "${keyId}",
                                    "doris_tde_key_endpoint" = "https://kms.us-east-2.amazonaws.com",
                                    "doris_tde_key_region" = "us-east-2"
                                    ) 
                               """

                        (1..10).each { i ->
                            sql """ INSERT INTO ${tblName} VALUES (${i}, "${i}") """
                        }

                        qt_sql """ SELECT * FROM ${tblName} ORDER BY `k` """
                        def newKeys = sql """ SELECT * FROM information_schema.encryption_keys """
                        assertNotEquals(keys[0][6], newKeys[0][6])

                        cluster.restartBackends()
                        cluster.restartFrontends()
                        sleep(30000)
                        context.reconnectFe()

                        qt_sql """ SELECT * FROM ${tblName} ORDER BY `k` """
                    } finally {
                        // delete cmk id
                        def deleteReq = ScheduleKeyDeletionRequest.builder().keyId(keyId).build();
                        client.scheduleKeyDeletion((ScheduleKeyDeletionRequest) deleteReq)
                    }
                }
            }
        }
    }
}
