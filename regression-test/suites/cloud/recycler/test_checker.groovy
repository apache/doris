import groovy.json.JsonOutput
import org.codehaus.groovy.runtime.IOGroovyMethods

import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.ListObjectsRequest
import com.amazonaws.services.s3.model.ObjectListing
import com.amazonaws.services.s3.model.DeleteObjectRequest
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.LocatedFileStatus
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.RemoteIterator

suite("test_checker") {
    def token = "greedisgood9999"
    def instanceId = context.config.instanceId;
    def cloudUniqueId = context.config.cloudUniqueId;
    def caseStartTime = System.currentTimeMillis()

    def tableName = "test_recycler"
    // Create table
    sql """ DROP TABLE IF EXISTS ${tableName}; """
    sql """
        CREATE TABLE ${tableName} (
            `id` int(11) NULL,
            `name` varchar(255) NULL,
            `score` int(11) NULL
        )
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1;
    """
    sql """ INSERT INTO ${tableName} VALUES (1, "a", 100); """
    sql """ INSERT INTO ${tableName} VALUES (2, "b", 100); """
    sql """ INSERT INTO ${tableName} VALUES (3, "c", 100); """
    sql """ INSERT INTO ${tableName} VALUES (4, "d", 100); """

    // Get tabletId
    String[][] tablets = sql """ show tablets from ${tableName}; """
    def tabletId = tablets[0][0]

    HashSet<String> tabletIdSet= new HashSet<String>()
    for (tabletInfo : tablets) {
        tabletIdSet.add(tabletInfo[0])
    }
    logger.info("tabletIdSet:${tabletIdSet}")

    // Randomly delete segment file under tablet dir
    def getObjStoreInfoApiResult = getObjStoreInfo(token, cloudUniqueId)
    if (getObjStoreInfoApiResult.result.containsKey("obj_info")) {
        String ak = getObjStoreInfoApiResult.result.obj_info[0].ak
        String sk = getObjStoreInfoApiResult.result.obj_info[0].sk
        String s3Endpoint = getObjStoreInfoApiResult.result.obj_info[0].endpoint
        String region = getObjStoreInfoApiResult.result.obj_info[0].region
        String prefix = getObjStoreInfoApiResult.result.obj_info[0].prefix
        String bucket = getObjStoreInfoApiResult.result.obj_info[0].bucket
        def credentials = new BasicAWSCredentials(ak, sk)
        def endpointConfiguration = new EndpointConfiguration(s3Endpoint, region)
        def s3Client = AmazonS3ClientBuilder.standard().withEndpointConfiguration(endpointConfiguration)
                .withCredentials(new AWSStaticCredentialsProvider(credentials)).build()
        def objectListing = s3Client.listObjects(
                new ListObjectsRequest().withBucketName(bucket).withPrefix("${prefix}/data/${tabletId}/"))
        def objectSummaries = objectListing.getObjectSummaries()
        assertTrue(!objectSummaries.isEmpty())
        Random random = new Random(caseStartTime);
        def objectKey = objectSummaries[random.nextInt(objectSummaries.size())].getKey()
        logger.info("delete objectKey: ${objectKey}")
        s3Client.deleteObject(new DeleteObjectRequest(bucket, objectKey))
    } else if (getObjStoreInfoApiResult.result.containsKey("storage_vault")) {
        String fsUri = getObjStoreInfoApiResult.result.storage_vault[0].hdfs_info.build_conf.fs_name
        String prefix = getObjStoreInfoApiResult.result.storage_vault[0].hdfs_info.prefix
        String hdfsPath = "/${prefix}/data/${tabletId}/"
        String username = getObjStoreInfoApiResult.result.storage_vault[0].hdfs_info.build_conf.user
        logger.info(":${fsUri}|${hdfsPath}".toString())
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", fsUri);
        configuration.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        configuration.set("hadoop.username", username);
        FileSystem fs = FileSystem.get(configuration);
        Path path = new Path(hdfsPath);
        try {
            fs.delete(path, true); // true means recursive
        } catch (FileNotFoundException e) {
            // do nothing
        } finally {
            fs.close();
        }
    } else {
        assertTrue(false);
    }

    // Make sure to complete at least one round of checking
    def checkerLastSuccessTime = -1
    def checkerLastFinishTime = -1

    def triggerChecker = {
        def triggerCheckerApi = { checkFunc ->
            httpTest {
                endpoint context.config.recycleServiceHttpAddress
                uri "/RecyclerService/http/check_instance?token=$token&instance_id=$instanceId"
                op "get"
                check checkFunc
            }
        }
        triggerCheckerApi.call() {
            respCode, body ->
                log.info("http cli result: ${body} ${respCode}".toString())
                triggerCheckerResult = body
                logger.info("triggerCheckerResult:${triggerCheckerResult}".toString())
                assertTrue(triggerCheckerResult.trim().equalsIgnoreCase("OK"))
        }
    }
    def getCheckJobInfo = {
        def checkJobInfoApi = { checkFunc ->
            httpTest {
                endpoint context.config.recycleServiceHttpAddress
                uri "/RecyclerService/http/check_job_info?token=$token&instance_id=$instanceId"
                op "get"
                check checkFunc
            }
        }
        checkJobInfoApi.call() {
            respCode, body ->
                logger.info("http cli result: ${body} ${respCode}")
                checkJobInfoResult = body
                logger.info("checkJobInfoResult:${checkJobInfoResult}")
                assertEquals(respCode, 200)
                def info = parseJson(checkJobInfoResult.trim())
                if (info.last_finish_time_ms != null) { // Check done
                    checkerLastFinishTime = Long.parseLong(info.last_finish_time_ms)
                }
                if (info.last_success_time_ms != null) {
                    checkerLastSuccessTime = Long.parseLong(info.last_success_time_ms)
                }
        }
    }

    do {
        triggerChecker()
        Thread.sleep(10000) // 10s
        getCheckJobInfo()
        logger.info("checkerLastFinishTime=${checkerLastFinishTime}, checkerLastSuccessTime=${checkerLastSuccessTime}")
        if (checkerLastFinishTime > caseStartTime) {
            break
        }
    } while (true)
    assertTrue(checkerLastSuccessTime < checkerLastFinishTime) // Check MUST fail

    sql """ DROP TABLE IF EXISTS ${tableName} FORCE"""

    retry = 15
    success = false
    // recycle data
    do {
        triggerRecycle(token, instanceId)
        Thread.sleep(20000) // 20s
        if (checkRecycleTable(token, instanceId, cloudUniqueId, tableName, tabletIdSet)) {
            success = true
            break
        }
    } while (retry--)
    assertTrue(success)
}
