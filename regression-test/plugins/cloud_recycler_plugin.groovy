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

    String ak = getObjStoreInfoApiResult.result.obj_info[0].ak
    String sk = getObjStoreInfoApiResult.result.obj_info[0].sk
    String endpoint = getObjStoreInfoApiResult.result.obj_info[0].endpoint
    String region = getObjStoreInfoApiResult.result.obj_info[0].region
    String prefix = getObjStoreInfoApiResult.result.obj_info[0].prefix
    String bucket = getObjStoreInfoApiResult.result.obj_info[0].bucket
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

logger.info("Added 'checkRecycleTable' function to Suite")

Suite.metaClass.checkRecycleInternalStage = { String token, String instanceId, String cloudUniqueId, String fileName
    /* param */ ->
    // which suite invoke current function?
    Suite suite = delegate as Suite

    // function body
    suite.getLogger().info("""Test plugin: suiteName: ${suite.name}, instanceId: ${instanceId}, token:${token}, cloudUniqueId:${cloudUniqueId}""".toString())

    def getObjStoreInfoApiResult = suite.getObjStoreInfo(token, cloudUniqueId);
    suite.getLogger().info("checkRecycleTable(): getObjStoreInfoApiResult:${getObjStoreInfoApiResult}".toString())

    String ak = getObjStoreInfoApiResult.result.obj_info[0].ak
    String sk = getObjStoreInfoApiResult.result.obj_info[0].sk
    String endpoint = getObjStoreInfoApiResult.result.obj_info[0].endpoint
    String region = getObjStoreInfoApiResult.result.obj_info[0].region
    String prefix = getObjStoreInfoApiResult.result.obj_info[0].prefix
    String bucket = getObjStoreInfoApiResult.result.obj_info[0].bucket
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
logger.info("Added 'checkRecycleInternalStage' function to Suite")

Suite.metaClass.checkRecycleExpiredStageObjects = { String token, String instanceId, String cloudUniqueId, Set<String> nonExistFileNames, Set<String> existFileNames ->
    // which suite invoke current function?
    Suite suite = delegate as Suite

    // function body
    suite.getLogger().info("""Test plugin: suiteName: ${suite.name}, instanceId: ${instanceId}, token:${token}, cloudUniqueId:${cloudUniqueId}""".toString())

    def getObjStoreInfoApiResult = suite.getObjStoreInfo(token, cloudUniqueId);
    suite.getLogger().info("checkRecycleExpiredStageObjects(): getObjStoreInfoApiResult:${getObjStoreInfoApiResult}".toString())

    String ak = getObjStoreInfoApiResult.result.obj_info[0].ak
    String sk = getObjStoreInfoApiResult.result.obj_info[0].sk
    String endpoint = getObjStoreInfoApiResult.result.obj_info[0].endpoint
    String region = getObjStoreInfoApiResult.result.obj_info[0].region
    String prefix = getObjStoreInfoApiResult.result.obj_info[0].prefix
    String bucket = getObjStoreInfoApiResult.result.obj_info[0].bucket
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
}
logger.info("Added 'checkRecycleExpiredStageObjects' function to Suite")
