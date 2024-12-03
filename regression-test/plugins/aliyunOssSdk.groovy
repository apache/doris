

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
import org.apache.doris.regression.suite.Suite;
import org.apache.doris.regression.Config;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import com.aliyun.oss.ClientException;
import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.model.DeleteObjectsRequest;
import com.aliyun.oss.model.DeleteObjectsResult;
import com.aliyun.oss.model.ListObjectsRequest;
import com.aliyun.oss.model.OSSObjectSummary;
import com.aliyun.oss.model.ObjectListing;

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import groovy.util.logging.Slf4j

Suite.metaClass.initOssClient = { String accessKeyId, String accessKeySecret, String endpoint ->
    return new OSSClientBuilder().build(endpoint, accessKeyId, accessKeySecret)
}

Suite.metaClass.listOssObjectWithPrefix = { OSS client, String bucketName, String prefix="" ->
    try {
        ObjectListing objectListing = null;
        String nextMarker = null;
        final int maxKeys = 500;
        List<OSSObjectSummary> sums = null;
        
        if (!client.doesBucketExist(bucketName)) {
            logger.info("no bucket named ${bucketName} in ${endpoint}")
            return
        }

        // Gets all object with specified marker by paging. Each page will have up to 100 entries.
        logger.info("List all objects with prefix:");
        nextMarker = null;
        do {
            objectListing = client.listObjects(new ListObjectsRequest(bucketName).
                    withPrefix(prefix).withMarker(nextMarker).withMaxKeys(maxKeys));
            
            sums = objectListing.getObjectSummaries();
            for (OSSObjectSummary s : sums) {
                logger.info("\t" + s.getKey());
            }
            
            nextMarker = objectListing.getNextMarker();
            
        } while (objectListing.isTruncated());
    } catch (OSSException oe) {
        logger.error("Caught an OSSException, which means your request made it to OSS, "
                + "but was rejected with an error response for some reason.");
        logger.error("Error Message: " + oe.getErrorMessage());
        logger.error("Error Code:       " + oe.getErrorCode());
        logger.error("Request ID:      " + oe.getRequestId());
        logger.error("Host ID:           " + oe.getHostId());
    } catch (ClientException ce) {
        logger.error("Caught an ClientException, which means the client encountered "
                + "a serious internal problem while trying to communicate with OSS, "
                + "such as not being able to access the network.");
        logger.error("Error Message: " + ce.getMessage());
    } finally {
        /*
            * Do not forget to shut down the client finally to release all allocated resources.
            */
        //client.shutdown();
        logger.info("Done!")
    }

}

// 获取某个存储空间下指定目录（文件夹）下的文件大小。
Suite.metaClass.calculateFolderLength = { OSS client, String bucketName, String folder ->
    long size = 0L;
    ObjectListing objectListing = null;
    do {
        // MaxKey默认值为100，最大值为1000。
        ListObjectsRequest request = new ListObjectsRequest(bucketName).withPrefix(folder).withMaxKeys(1000);
        if (objectListing != null) {
            request.setMarker(objectListing.getNextMarker());
        }
        objectListing = client.listObjects(request);
        List<OSSObjectSummary> sums = objectListing.getObjectSummaries();
        for (OSSObjectSummary s : sums) {
            size += s.getSize();
        }
        } while (objectListing.isTruncated());
    return size;
}

Suite.metaClass.shutDownOssClient = { OSS client ->
    client.shutdown();
}



Suite.metaClass.getOssAllDirSizeWithPrefix = { OSS client, String bucketName, String prefix="" ->
    try {
        if (!client.doesBucketExist(bucketName)) {
            logger.info("no bucket named ${bucketName} in ${endpoint}")
            return
        }

        // Gets all object with specified marker by paging. Each page will have up to 100 entries.
        logger.info("List all objects with prefix:");
        ObjectListing objectListing = null;
        do {
            // 默认情况下，每次列举100个文件或目录。
            ListObjectsRequest request = new ListObjectsRequest(bucketName).withDelimiter("/").withPrefix(prefix);
            if (objectListing != null) {
                request.setMarker(objectListing.getNextMarker());
            }
            objectListing = client.listObjects(request);
            List<String> folders = objectListing.getCommonPrefixes();
            for (String folder : folders) {
                logger.info(folder + " : " + (calculateFolderLength(client, bucketName, folder) / (1024 * 1024 * 1024)) + "GB");
            }
            List<OSSObjectSummary> sums = objectListing.getObjectSummaries();
            for (OSSObjectSummary s : sums) {
                logger.info(s.getKey() + " : " + (s.getSize() / (1024 * 1024 * 1024)) + "GB");
            }
        } while (objectListing.isTruncated());
        
    } catch (OSSException oe) {
        logger.error("Caught an OSSException, which means your request made it to OSS, "
                + "but was rejected with an error response for some reason.");
        logger.error("Error Message: " + oe.getErrorMessage());
        logger.error("Error Code:       " + oe.getErrorCode());
        logger.error("Request ID:      " + oe.getRequestId());
        logger.error("Host ID:           " + oe.getHostId());
    } catch (ClientException ce) {
        logger.error("Caught an ClientException, which means the client encountered "
                + "a serious internal problem while trying to communicate with OSS, "
                + "such as not being able to access the network.");
        logger.error("Error Message: " + ce.getMessage());
    } finally {
        /*
            * Do not forget to shut down the client finally to release all allocated resources.
            */
        //client.shutdown();
        logger.info("Done!")
    }
}



