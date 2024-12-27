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

package org.apache.doris.datasource.property.catalog;

import org.apache.doris.common.UserException;
import org.apache.doris.datasource.property.metastore.AliyunDLFProperties;
import org.apache.doris.datasource.property.metastore.HMSProperties;
import org.apache.doris.datasource.property.metastore.MetastoreProperties;
import org.apache.doris.datasource.property.storage.HDFSProperties;
import org.apache.doris.datasource.property.storage.S3Properties;
import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.datasource.property.storage.StorageProperties.Type;

import com.aliyun.datalake.metastore.hive2.ProxyMetaStoreClient;
import com.google.common.base.Strings;
import org.apache.hadoop.conf.Configuration;
import org.apache.paimon.options.Options;

import java.net.URI;
import java.nio.file.Paths;

public class PaimonCatalogProperties {

    private Options options = new Options();
    private Configuration hadoopConf = new Configuration(false);

    public PaimonCatalogProperties(String warehouse, MetastoreProperties metaProps, StorageProperties storeProps)
            throws UserException {
        init(warehouse, metaProps, storeProps);
    }

    private void init(String warehouse, MetastoreProperties metaProps, StorageProperties storeProps)
            throws UserException {
        initMetastoreProperties(metaProps);
        initFileIOProperties(warehouse, storeProps);
    }

    private void initMetastoreProperties(MetastoreProperties metaProps)
            throws UserException {
        switch (metaProps.getType()) {
            case HMS:
                options.set("metastore", "hive");
                HMSProperties hmsProperties = (HMSProperties) metaProps;
                hmsProperties.toPaimonOptionsAndConf(options, hadoopConf);
                break;
            case DLF:
                options.set("metastore", "hive");
                options.set("metastore.client.class", ProxyMetaStoreClient.class.getName());
                AliyunDLFProperties dlfProperties = (AliyunDLFProperties) metaProps;
                dlfProperties.toPaimonOptions(options);
                break;
            case FILE_SYSTEM:
                options.set("metastore", "filesystem");
                break;
            default:
                throw new UserException("Unsupported metastore type: " + metaProps.getType());
        }
    }

    private void initFileIOProperties(String warehouse, StorageProperties storeProps)
            throws UserException {
        String finalWarehouse = warehouse;
        // init file io properties
        URI uri = Paths.get(warehouse).toUri();
        // need to set file io properties based on the warehouse scheme.
        String scheme = Strings.nullToEmpty(uri.getScheme());
        switch (scheme) {
            case "":
            case "file":
                break;
            case "oss":
                initOSSFileIOProps(storeProps);
                break;
            case "hdfs":
                initHadoopFileIOProps(storeProps);
                break;
            case "s3":
            case "cos":
            case "obs":
            case "tos":
            case "bos":
            case "gcs":
                // Use S3FileIO for all S3-like storage,
                // replace the scheme with s3.
                finalWarehouse = "s3://" + uri.getAuthority() + uri.getPath();
                initS3FileIOProps(storeProps);
                break;
            default:
                throw new UserException("Unsupported warehouse type: " + scheme);
        }
        options.set("warehouse", finalWarehouse);
    }

    private void initOSSFileIOProps(StorageProperties storeProps) throws UserException {
        if (storeProps.getType() != Type.S3) {
            throw new UserException("The warehouse is on OSS, but the storage property is not for S3: "
                    + storeProps.getType());
        }
        S3Properties s3Props = (S3Properties) storeProps;
        s3Props.toPaimonOSSFileIOProperties(options);
    }

    private void initHadoopFileIOProps(StorageProperties storeProps) throws UserException {
        if (storeProps.getType() != Type.HDFS) {
            throw new UserException("The warehouse is on HDFS-like storage, but the storage property is not for HDFS: "
                    + storeProps.getType());
        }
        HDFSProperties hdfsProps = (HDFSProperties) storeProps;

    }

    private void initS3FileIOProps(StorageProperties storeProps) throws UserException {
        if (storeProps.getType() != Type.S3) {
            throw new UserException("The warehouse is on S3-like storage, but the storage property is not for S3: "
                    + storeProps.getType());
        }
        S3Properties s3Props = (S3Properties) storeProps;
        s3Props.toPaimonS3FileIOProperties(options);
    }
}
