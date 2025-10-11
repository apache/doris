import org.apache.doris.regression.suite.ClusterOptions
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.kms.KmsClient
import software.amazon.awssdk.services.kms.model.ScheduleKeyDeletionRequest

suite("test_rotate_master_keys", "docker") {
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
                "doris_tde_key_id=${context.config.tdeKeyId}",
                "doris_tde_check_rotate_master_key_interval_ms=1000",
                "doris_tde_rotate_master_key_interval_ms=10000"
            ]
            options.tdeAk = context.config.tdeAk
            options.tdeSk = context.config.tdeSk
            options.beNum = 1

            docker(options) {
                def tblName = "test_rotate_master_keys"
                sql """ DROP TABLE IF EXISTS ${tblName} """
                sql """
                    CREATE TABLE IF NOT EXISTS ${tblName} (
                    `k` int NOT NULL,
                    `v` varchar(10) NOT NULL) 
                DUPLICATE KEY(`k`)
                DISTRIBUTED BY HASH(`k`) BUCKETS 1
                PROPERTIES (
                    "replication_allocation" = "tag.location.default: 1",
                    "disable_auto_compaction" = "true"
                )
                """
                def backendId_to_backendIP = [:]
                def backendId_to_backendHttpPort = [:]
                getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);
                def beHttpAddress =backendId_to_backendIP.entrySet()[0].getValue()+":"+backendId_to_backendHttpPort.entrySet()[0].getValue()

                sql """ INSERT INTO ${tblName} VALUES(1, "10")"""
                def tablets = sql_return_maparray "SHOW TABLETS FROM ${tblName}"
                def tablet = tablets[0].TabletId
                def (code, text, err) = curl("GET", beHttpAddress+ "/api/check_tablet_encryption?tablet_id=${tablet}&get_footer=true", null/*body*/, 10/*timeoutSec*/)
                println(text)
                def parentVersion = parseJson(text).get("footer").get("data_key_info").get("parent_version")

                sleep(21000)
                sql """ INSERT INTO ${tblName} VALUES(1, "10")"""
                
                def (code1, text1, err1) = curl("GET", beHttpAddress+ "/api/check_tablet_encryption?tablet_id=${tablet}&get_footer=true", null/*body*/, 10/*timeoutSec*/)
                println(text1)
                def parentVersion1 = parseJson(text1).get("footer").get("data_key_info").get("parent_version")
                assertNotEquals(parentVersion, parentVersion1)
                cluster.restartBackends()
                cluster.restartFrontends()
                sleep(30000)
                context.reconnectFe()
                sql """ INSERT INTO ${tblName} VALUES(1, "10")"""
                def (code2, text2, err2) = curl("GET", beHttpAddress+ "/api/check_tablet_encryption?tablet_id=${tablet}&get_footer=true", null/*body*/, 10/*timeoutSec*/)
                println(text2)
                def parentVersion2 = parseJson(text2).get("footer").get("data_key_info").get("parent_version")
                assertNotEquals(parentVersion, parentVersion2)
                sql """ SELECT * FROM ${tblName} """
            }
        }
    }
}
