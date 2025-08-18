
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

/**
 * 计算指定文件夹的总大小（递归计算所有文件）
 * @param client OSS客户端实例
 * @param bucketName OSS存储桶名称
 * @param folder 文件夹路径前缀
 * @return 文件夹总大小（字节）
 */
Suite.metaClass.calculateFolderLength = { OSS client, String bucketName, String folder ->
    logger.info("[calculateFolderLength] 开始计算文件夹大小 - Bucket: ${bucketName}, Folder: ${folder}")
    
    long size = 0L;  // 累计文件大小
    ObjectListing objectListing = null;
    int pageCount = 0;  // 分页计数器
    int totalObjects = 0;  // 总文件数量计数器
    
    try {
        // 使用分页方式遍历所有对象，避免一次性加载过多数据
        do {
            pageCount++;
            logger.info("[calculateFolderLength] 正在处理第 ${pageCount} 页数据...")
            
            // 创建列表对象请求，设置最大返回数量为1000（OSS限制的最大值）
            ListObjectsRequest request = new ListObjectsRequest(bucketName)
                .withPrefix(folder)
                .withMaxKeys(1000);
            
            // 如果不是第一页，设置分页标记
            if (objectListing != null) {
                String nextMarker = objectListing.getNextMarker();
                logger.info("[calculateFolderLength] 设置分页标记: ${nextMarker}")
                request.setMarker(nextMarker);
            }
            
            // 执行OSS请求获取对象列表
            logger.info("[calculateFolderLength] 执行OSS listObjects请求...")
            objectListing = client.listObjects(request);
            
            // 获取当前页的对象摘要列表
            List<OSSObjectSummary> sums = objectListing.getObjectSummaries();
            logger.info("[calculateFolderLength] 当前页找到 ${sums.size()} 个对象")
            
            // 如果当前页没有对象，记录警告信息
            if (sums.isEmpty()) {
                logger.warn("[calculateFolderLength] 第 ${pageCount} 页没有找到任何对象，但分页标记显示还有更多数据")
            }
            
            // 遍历当前页的所有对象，累加大小
            for (OSSObjectSummary s : sums) {
                totalObjects++;
                long objSize = s.getSize();
                
                // 详细记录每个对象的信息
                logger.info("[calculateFolderLength] 对象 #${totalObjects}:")
                logger.info("  - Key: ${s.getKey()}")
                logger.info("  - Size: ${objSize} bytes (${String.format('%.2f', objSize / 1024.0 / 1024.0)} MB)")
                logger.info("  - Last Modified: ${s.getLastModified()}")
                logger.info("  - Storage Class: ${s.getStorageClass()}")
                logger.info("  - Owner: ${s.getOwner()?.getId() ?: 'N/A'}")
                logger.info("  - ETag: ${s.getETag()}")
                
                // 累加到总大小
                size += objSize;
                logger.info("  - 当前累计大小: ${size} bytes (${String.format('%.2f', size / 1024.0 / 1024.0)} MB)")
                logger.info("-------------------")
            }
            
            // 记录分页状态信息
            logger.info("[calculateFolderLength] 第 ${pageCount} 页处理完成:")
            logger.info("  - 本页对象数: ${sums.size()}")
            logger.info("  - 累计对象数: ${totalObjects}")
            logger.info("  - 当前累计大小: ${size} bytes")
            logger.info("  - 是否还有更多页: ${objectListing.isTruncated()}")
            logger.info("  - 下一页标记: ${objectListing.getNextMarker() ?: 'N/A'}")
            
        } while (objectListing.isTruncated()); // 继续处理下一页，直到所有数据处理完毕
        
        // 记录最终统计结果
        logger.info("[calculateFolderLength] 文件夹大小计算完成:")
        logger.info("  - 文件夹路径: ${folder}")
        logger.info("  - 总页数: ${pageCount}")
        logger.info("  - 总文件数: ${totalObjects}")
        logger.info("  - 总大小: ${size} bytes")
        logger.info("  - 总大小: ${String.format('%.2f', size / 1024.0 / 1024.0)} MB")
        logger.info("  - 总大小: ${String.format('%.2f', size / 1024.0 / 1024.0 / 1024.0)} GB")
        
    } catch (Exception e) {
        logger.error("[calculateFolderLength] 计算文件夹大小时发生异常:", e)
        logger.error("  - Bucket: ${bucketName}")
        logger.error("  - Folder: ${folder}")
        logger.error("  - 已处理页数: ${pageCount}")
        logger.error("  - 已处理对象数: ${totalObjects}")
        logger.error("  - 当前累计大小: ${size} bytes")
        throw e  // 重新抛出异常
    }
    
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
            // By default, list 100 files or directories at a time
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
