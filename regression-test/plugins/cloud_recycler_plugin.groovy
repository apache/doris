// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
import groovy.json.JsonOutput

import org.apache.doris.regression.suite.Suite

import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.ListObjectsRequest
import com.amazonaws.services.s3.model.ObjectListing
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.LocatedFileStatus
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.RemoteIterator
import org.apache.hadoop.security.UserGroupInformation

Suite.metaClass.triggerRecycle = { String token, String instanceId /* param */ ->
    // which suite invoke current function?
    Suite suite = delegate as Suite
    // function body
    suite.getLogger().info("Test plugin: suiteName: ${suite.name}, token: ${token}, instance:${instanceId}".toString())

    def triggerRecycleBody = [instance_ids: ["${instanceId}"]]
    def jsonOutput = new JsonOutput()
    def triggerRecycleJson = jsonOutput.toJson(triggerRecycleBody)
    def triggerRecycleResult = null;
    def triggerRecycleApi = { requestBody, checkFunc ->
        httpTest {
            endpoint suite.context.config.recycleServiceHttpAddress
            uri "/RecyclerService/http/recycle_instance?token=$token"
            body requestBody
            check checkFunc
        }
    }

    Thread.sleep(5000)

    triggerRecycleApi.call(triggerRecycleJson) {
        respCode, body ->
            log.info("http cli result: ${body} ${respCode}".toString())
            triggerRecycleResult = body
            suite.getLogger().info("triggerRecycleResult:${triggerRecycleResult}".toString())
            assertTrue(triggerRecycleResult.trim().equalsIgnoreCase("OK"))
    }
    return;
}

logger.info("Added 'triggerRecycle' function to Suite")

//cloud mode recycler plugin
Suite.metaClass.checkRecycleTable = { String token, String instanceId, String cloudUniqueId, String tableName, 
        Collection<String> tabletIdList /* param */ ->
    // which suite invoke current function?
    Suite suite = delegate as Suite

    // function body
    suite.getLogger().info("""Test plugin: suiteName: ${suite.name}, tableName: ${tableName}, instanceId: ${instanceId}, token:${token}, cloudUniqueId:${cloudUniqueId}""".toString())

    def getObjStoreInfoApiResult = suite.getObjStoreInfo(token, cloudUniqueId);
    suite.getLogger().info("checkRecycleTable(): getObjStoreInfoApiResult:${getObjStoreInfoApiResult}".toString())

    if (getObjStoreInfoApiResult.result.toString().contains("obj_info")) {
        String ak, sk, endpoint, region, prefix, bucket
        if(!getObjStoreInfoApiResult.result.toString().contains("storage_vault=[")){
            ak = getObjStoreInfoApiResult.result.obj_info[0].ak
            sk = getObjStoreInfoApiResult.result.obj_info[0].sk
            endpoint = getObjStoreInfoApiResult.result.obj_info[0].endpoint
            region = getObjStoreInfoApiResult.result.obj_info[0].region
            prefix = getObjStoreInfoApiResult.result.obj_info[0].prefix
            bucket = getObjStoreInfoApiResult.result.obj_info[0].bucket
        }else{
            ak = getObjStoreInfoApiResult.result.storage_vault[0].obj_info.ak
            sk = getObjStoreInfoApiResult.result.storage_vault[0].obj_info.sk
            endpoint = getObjStoreInfoApiResult.result.storage_vault[0].obj_info.endpoint
            region = getObjStoreInfoApiResult.result.storage_vault[0].obj_info.region
            prefix = getObjStoreInfoApiResult.result.storage_vault[0].obj_info.prefix
            bucket = getObjStoreInfoApiResult.result.storage_vault[0].obj_info.bucket
        }
        suite.getLogger().info("ak:${ak}, sk:${sk}, endpoint:${endpoint}, prefix:${prefix}".toString())

        def credentials = new BasicAWSCredentials(ak, sk)
        def endpointConfiguration = new EndpointConfiguration(endpoint, region)
        def s3Client = AmazonS3ClientBuilder.standard().withEndpointConfiguration(endpointConfiguration)
                .withCredentials(new AWSStaticCredentialsProvider(credentials)).build()

        assertTrue(tabletIdList.size() > 0)
        for (tabletId : tabletIdList) {
            suite.getLogger().info("tableName: ${tableName}, tabletId:${tabletId}");
            def objectListing = s3Client.listObjects(
                new ListObjectsRequest().withMaxKeys(1).withBucketName(bucket).withPrefix("${prefix}/data/${tabletId}/"))

            suite.getLogger().info("tableName: ${tableName}, tabletId:${tabletId}, objectListing:${objectListing.getObjectSummaries()}".toString())
            if (!objectListing.getObjectSummaries().isEmpty()) {
                return false;
            }
        }
        return true;
    }

    suite.getLogger().info(getObjStoreInfoApiResult.result.toString())
    if (getObjStoreInfoApiResult.result.toString().contains("storage_vault=[") && getObjStoreInfoApiResult.result.toString().contains("hdfs_info")) {
        String fsUri = getObjStoreInfoApiResult.result.storage_vault[0].hdfs_info.build_conf.fs_name
        String prefix = getObjStoreInfoApiResult.result.storage_vault[0].hdfs_info.prefix
        String kbsPrincipal = ''
        String kbsKeytab = ''
        Boolean isKbs = false
        if ( getObjStoreInfoApiResult.result.toString().contains("value=kerberos")) {
            kbsPrincipal = getObjStoreInfoApiResult.result.storage_vault[0].hdfs_info.build_conf.hdfs_kerberos_principal
            kbsKeytab = getObjStoreInfoApiResult.result.storage_vault[0].hdfs_info.build_conf.hdfs_kerberos_keytab
            isKbs = true
        }
        suite.getLogger().info("fsUri:${fsUri}, prefix:${prefix}, kbsPrincipal:${kbsPrincipal}, kbsKeytab:${kbsKeytab}".toString())

        assertTrue(tabletIdList.size() > 0)
        for (tabletId : tabletIdList) {
            suite.getLogger().info("tableName: ${tableName}, tabletId:${tabletId}");
            String hdfsPath = "/${prefix}/data/${tabletId}/"
            Configuration configuration = new Configuration();
            configuration.set("fs.defaultFS", fsUri);
            configuration.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
            if (isKbs) {
                configuration.set("hadoop.security.authentication", "kerberos")
                UserGroupInformation.setConfiguration(configuration)
                UserGroupInformation.loginUserFromKeytab(kbsPrincipal, kbsKeytab)
            }
            FileSystem fs = FileSystem.get(configuration);
            Path path = new Path(hdfsPath);
            suite.getLogger().info("tableName: ${tableName}, tabletId:${tabletId} hdfsPath:${hdfsPath}");
            try {
                RemoteIterator<LocatedFileStatus> files = fs.listFiles(path, false); // true means recursive
                while (files.hasNext()) {
                    LocatedFileStatus file = files.next();
                    suite.getLogger().info("file: ${file}".toString())
                    suite.getLogger().info("tableName: ${tableName}, tabletId:${tabletId}, file:${file}".toString())
                    return false
                }
            } catch (FileNotFoundException e) {
                continue;
            } finally {
                fs.close();
            }
        }
        return true;
    }
    return false
}

logger.info("Added 'checkRecycleTable' function to Suite")

Suite.metaClass.checkRecycleInternalStage = { String token, String instanceId, String cloudUniqueId, String fileName
    /* param */ ->
    // which suite invoke current function?
    Suite suite = delegate as Suite

    // function body
    suite.getLogger().info("""Test plugin: suiteName: ${suite.name}, instanceId: ${instanceId}, token:${token}, cloudUniqueId:${cloudUniqueId}""".toString())

    def getObjStoreInfoApiResult = suite.getObjStoreInfo(token, cloudUniqueId);
    suite.getLogger().info("checkRecycleTable(): getObjStoreInfoApiResult:${getObjStoreInfoApiResult}".toString())

    if (getObjStoreInfoApiResult.result.toString().contains("obj_info")) {
        String ak, sk, endpoint, region, prefix, bucket
        if(!getObjStoreInfoApiResult.result.toString().contains("storage_vault=[")){
            ak = getObjStoreInfoApiResult.result.obj_info[0].ak
            sk = getObjStoreInfoApiResult.result.obj_info[0].sk
            endpoint = getObjStoreInfoApiResult.result.obj_info[0].endpoint
            region = getObjStoreInfoApiResult.result.obj_info[0].region
            prefix = getObjStoreInfoApiResult.result.obj_info[0].prefix
            bucket = getObjStoreInfoApiResult.result.obj_info[0].bucket
        }else{
            ak = getObjStoreInfoApiResult.result.storage_vault[0].obj_info.ak
            sk = getObjStoreInfoApiResult.result.storage_vault[0].obj_info.sk
            endpoint = getObjStoreInfoApiResult.result.storage_vault[0].obj_info.endpoint
            region = getObjStoreInfoApiResult.result.storage_vault[0].obj_info.region
            prefix = getObjStoreInfoApiResult.result.storage_vault[0].obj_info.prefix
            bucket = getObjStoreInfoApiResult.result.storage_vault[0].obj_info.bucket
        }
        suite.getLogger().info("ak:${ak}, sk:${sk}, endpoint:${endpoint}, prefix:${prefix}".toString())

        def credentials = new BasicAWSCredentials(ak, sk)
        def endpointConfiguration = new EndpointConfiguration(endpoint, region)
        def s3Client = AmazonS3ClientBuilder.standard().withEndpointConfiguration(endpointConfiguration)
                .withCredentials(new AWSStaticCredentialsProvider(credentials)).build()

        // for root and admin, userId equal userName
        String userName = suite.context.config.jdbcUser;
        String userId = suite.context.config.jdbcUser;
        def objectListing = s3Client.listObjects(
            new ListObjectsRequest().withMaxKeys(1)
                .withBucketName(bucket)
                .withPrefix("${prefix}/stage/${userName}/${userId}/${fileName}"))

        suite.getLogger().info("${prefix}/stage/${userName}/${userId}/${fileName}, objectListing:${objectListing.getObjectSummaries()}".toString())
        if (!objectListing.getObjectSummaries().isEmpty()) {
            return false;
        }
        return true;
    }

    if (getObjStoreInfoApiResult.result.toString().contains("storage_vault=[") && getObjStoreInfoApiResult.result.toString().contains("hdfs_info")) {
        String fsUri = getObjStoreInfoApiResult.result.storage_vault[0].hdfs_info.build_conf.fs_name
        String prefix = getObjStoreInfoApiResult.result.storage_vault[0].hdfs_info.prefix
        // for root and admin, userId equal userName
        String userName = suite.context.config.jdbcUser;
        String userId = suite.context.config.jdbcUser;

        String hdfsPath = "/${prefix}/stage/${userName}/${userId}/${fileName}"
        suite.getLogger().info(":${fsUri}|${hdfsPath}".toString())
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", fsUri);
        configuration.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        FileSystem fs = FileSystem.get(configuration);
        Path path = new Path(hdfsPath);
        try {
            RemoteIterator<LocatedFileStatus> files = fs.listFiles(path, false); // true means recursive
            while (files.hasNext()) {
                LocatedFileStatus file = files.next();
                suite.getLogger().info("file exist: ${file}".toString())
                return false
            }
        } catch (FileNotFoundException e) {
            return true;
        } finally {
            fs.close();
        }

        return true;
    }

    return false
}
logger.info("Added 'checkRecycleInternalStage' function to Suite")

Suite.metaClass.checkRecycleExpiredStageObjects = { String token, String instanceId, String cloudUniqueId, Set<String> nonExistFileNames, Set<String> existFileNames ->
    // which suite invoke current function?
    Suite suite = delegate as Suite

    // function body
    suite.getLogger().info("""Test plugin: suiteName: ${suite.name}, instanceId: ${instanceId}, token:${token}, cloudUniqueId:${cloudUniqueId}""".toString())

    def getObjStoreInfoApiResult = suite.getObjStoreInfo(token, cloudUniqueId);
    suite.getLogger().info("checkRecycleExpiredStageObjects(): getObjStoreInfoApiResult:${getObjStoreInfoApiResult}".toString())

    if (getObjStoreInfoApiResult.result.toString().contains("obj_info")) {
        String ak, sk, endpoint, region, prefix, bucket
        if(!getObjStoreInfoApiResult.result.toString().contains("storage_vault=[")){
            ak = getObjStoreInfoApiResult.result.obj_info[0].ak
            sk = getObjStoreInfoApiResult.result.obj_info[0].sk
            endpoint = getObjStoreInfoApiResult.result.obj_info[0].endpoint
            region = getObjStoreInfoApiResult.result.obj_info[0].region
            prefix = getObjStoreInfoApiResult.result.obj_info[0].prefix
            bucket = getObjStoreInfoApiResult.result.obj_info[0].bucket
        }else{
            ak = getObjStoreInfoApiResult.result.storage_vault[0].obj_info.ak
            sk = getObjStoreInfoApiResult.result.storage_vault[0].obj_info.sk
            endpoint = getObjStoreInfoApiResult.result.storage_vault[0].obj_info.endpoint
            region = getObjStoreInfoApiResult.result.storage_vault[0].obj_info.region
            prefix = getObjStoreInfoApiResult.result.storage_vault[0].obj_info.prefix
            bucket = getObjStoreInfoApiResult.result.storage_vault[0].obj_info.bucket
        }
        suite.getLogger().info("ak:${ak}, sk:${sk}, endpoint:${endpoint}, prefix:${prefix}".toString())

        def credentials = new BasicAWSCredentials(ak, sk)
        def endpointConfiguration = new EndpointConfiguration(endpoint, region)
        def s3Client = AmazonS3ClientBuilder.standard().withEndpointConfiguration(endpointConfiguration)
                .withCredentials(new AWSStaticCredentialsProvider(credentials)).build()

        // for root and admin, userId equal userName
        String userName = suite.context.config.jdbcUser;
        String userId = suite.context.config.jdbcUser;
        def objectListing = s3Client.listObjects(
                new ListObjectsRequest()
                        .withBucketName(bucket)
                        .withPrefix("${prefix}/stage/${userName}/${userId}/"))

        suite.getLogger().info("${prefix}/stage/${userName}/${userId}/, objectListing:${objectListing.getObjectSummaries()}".toString())
        Set<String> fileNames = new HashSet<>()
        for (def os: objectListing.getObjectSummaries()) {
            def split = os.key.split("/")
            if (split.length <= 0 ) {
                continue
            }
            fileNames.add(split[split.length-1])
        }
        for(def f : nonExistFileNames) {
            if (fileNames.contains(f)) {
                return false
            }
        }
        for(def f : existFileNames) {
            if (!fileNames.contains(f)) {
                return false
            }
        }
        return true
    } else if (getObjStoreInfoApiResult.result.toString().contains("storage_vault=[") && getObjStoreInfoApiResult.result.toString().contains("hdfs_info")) {
        String userName = suite.context.config.jdbcUser;
        String userId = suite.context.config.jdbcUser;
        String fsUri = getObjStoreInfoApiResult.result.storage_vault[0].hdfs_info.build_conf.fs_name
        String prefix = getObjStoreInfoApiResult.result.storage_vault[0].hdfs_info.prefix
        String hdfsPath = "/${prefix}/stage/${userName}/${userId}/"
        String username = getObjStoreInfoApiResult.result.storage_vault[0].hdfs_info.build_conf.user
    
        logger.info(":${fsUri}|${hdfsPath}".toString())
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", fsUri);
        configuration.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        configuration.set("hadoop.username", username);
        FileSystem fs = FileSystem.get(configuration);
        Path path = new Path(hdfsPath);
        Set<String> fileNames = new HashSet<>();
        if (fs.exists(path)) {
            FileStatus[] fileStatuses = fs.listStatus(path);
            for (FileStatus status : fileStatuses) {
                String fileName = status.getPath().getName();
                fileNames.add(fileName);
            }
        }
        suite.getLogger().info("hdfsPath:${hdfsPath}, files:${fileNames}".toString())

        for(def f : nonExistFileNames) {
            if (fileNames.contains(f)) {
                return false
            }
        }
        for(def f : existFileNames) {
            if (!fileNames.contains(f)) {
                return false
            }
        }
        fs.close()
        return true
    } else {
        assertTrue(false)
    }
    return false
}
logger.info("Added 'checkRecycleExpiredStageObjects' function to Suite")

Suite.metaClass.checkRecycleMetrics = { String recyclerHttpPort, String recycleJobType ->

    def getRecyclerMetricsMethod = { String ipWithPort, String metricsName, String resourceType = null ->
        def parsedMetricData = null

        def parseMetricLine = { String line ->
            def matcher = (line =~ /\{(.*?)\}\s+(\d+\.?\d*)/)
            if (matcher.find()) {
                String labelsString = matcher.group(1)
                String metricValue = matcher.group(2)

                def labels = [:]
                def labelMatcher = (labelsString =~ /(\w+?)="([^"]*?)"/)
                labelMatcher.each { match ->
                    labels[match[1]] = match[2]
                }
                return [labels: labels, value: metricValue]
            }
            return null
        }

        httpTest {
            endpoint ipWithPort
            uri "/brpc_metrics"
            op "get"
            printResponse false
            check { respCode, body ->
                assertEquals("${respCode}".toString(), "200")

                String out = "${body}".toString()
                def lines = out.split('\n')

                for (String line in lines) {
                    if (line.contains(metricsName) && !line.startsWith("#")) {
                        def data = parseMetricLine(line)
                        if (data) {
                            if (resourceType != null && data.labels?.resource_type == resourceType) {
                                parsedMetricData = data
                                break
                            } else if (resourceType == null) {
                                parsedMetricData = data
                                break
                            }
                        }
                    }
                }
            }
        }
        return parsedMetricData
    }

    int retryCount = 0
    while (true) {
        def metricDataBeforeRecycle = getRecyclerMetricsMethod.call(
            recyclerHttpPort,
            "recycler_instance_last_round_to_recycle_bytes",
            recycleJobType
        )

        def metricDataAftereRecycle = getRecyclerMetricsMethod.call(
            recyclerHttpPort,
            "recycler_instance_last_round_recycled_bytes",
            recycleJobType
        )

        // not all resource types have bytes metrics
        def validResourceTypes = ["recycle_indexes", "recycle_partitions", "recycle_tmp_rowsets", "recycle_rowsets", "recycle_tablet", "recycle_segment"]
        
        boolean checkFlag1 = false
        boolean checkFlag2 = false

        if (validResourceTypes.contains(recycleJobType)) {
            checkFlag1 = true
        }

        if (metricDataBeforeRecycle && metricDataAftereRecycle && !checkFlag1) {
            if (metricDataBeforeRecycle.value == metricDataAftereRecycle.value) {
                logger.info("--- Recycle Success ---")
                logger.info("Metric Name: recycler_instance_last_round_recycled_bytes")
                logger.info("Value: ${metricDataBeforeRecycle.value}")
                logger.info("Resource Type: ${metricDataBeforeRecycle.labels?.resource_type}")
                logger.info("--------------------------------------")
                checkFlag1 = true
            } else {
                logger.info("--- Recycle failed ---")
                logger.info("Metric Name: recycler_instance_last_round_to_recycle_bytes")
                logger.info("Value: ${metricDataBeforeRecycle.value}")
                logger.info("Resource Type: ${metricDataBeforeRecycle.labels?.resource_type}")
                logger.info("--------------------------------------")
                logger.info("Metric Name: recycler_instance_last_round_recycled_bytes")
                logger.info("Value: ${metricDataAftereRecycle.value}")
                logger.info("Resource Type: ${metricDataAftereRecycle.labels?.resource_type}")
                logger.info("--------------------------------------")
            }
        }

        metricDataBeforeRecycle = getRecyclerMetricsMethod.call(
            recyclerHttpPort,
            "recycler_instance_last_round_to_recycle_num",
            recycleJobType
        )

        metricDataAftereRecycle = getRecyclerMetricsMethod.call(
            recyclerHttpPort,
            "recycler_instance_last_round_recycled_num",
            recycleJobType
        )

        if (metricDataBeforeRecycle && metricDataAftereRecycle && !checkFlag2) {
            if (metricDataBeforeRecycle.value == metricDataAftereRecycle.value) {
                logger.info("--- Recycle Success ---")
                logger.info("Metric Name: recycler_instance_last_round_recycled_num")
                logger.info("Value: ${metricDataBeforeRecycle.value}")
                logger.info("Resource Type: ${metricDataBeforeRecycle.labels?.resource_type}")
                logger.info("--------------------------------------")
                checkFlag2 = true
            } else {
                logger.info("--- Recycle failed ---")
                logger.info("Metric Name: recycler_instance_last_round_to_recycle_num")
                logger.info("Value: ${metricDataBeforeRecycle.value}")
                logger.info("Resource Type: ${metricDataBeforeRecycle.labels?.resource_type}")
                logger.info("--------------------------------------")
                logger.info("Metric Name: recycler_instance_last_round_recycled_num")
                logger.info("Value: ${metricDataAftereRecycle.value}")
                logger.info("Resource Type: ${metricDataAftereRecycle.labels?.resource_type}")
                logger.info("--------------------------------------")
            }
        }

        if (checkFlag1 && checkFlag2) {
            break;
        }

        retryCount++
        if (retryCount > 10) {
            logger.error("Failed to get metric 'recycler_instance_last_round_to_recycle_bytes' after 10 retries.")
            return;
        }
        sleep(5000)
    }
}
logger.info("Added 'checkRecycleMetrics' function to Suite")