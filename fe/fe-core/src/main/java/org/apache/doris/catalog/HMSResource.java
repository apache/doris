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

package org.apache.doris.catalog;

import org.apache.doris.common.DdlException;
import org.apache.doris.common.proc.BaseProcResult;

import com.amazonaws.glue.catalog.util.AWSGlueConfig;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * HMS resource
 * <p>
 * Syntax:
 * CREATE RESOURCE "hive"
 * PROPERTIES
 * (
 * "type" = "hms",
 * "hive.metastore.uris" = "thrift://172.21.0.44:7004"
 * );
 */
public class HMSResource extends Resource {
    private static final Logger LOG = LogManager.getLogger(HMSResource.class);
    public static final String HIVE_METASTORE_TYPE = "hive.metastore.type";
    public static final String DLF_TYPE = "dlf";
    public static final String GLUE_TYPE = "glue";
    public static final String HIVE_VERSION = "hive.version";
    // required
    public static final String HIVE_METASTORE_URIS = "hive.metastore.uris";
    public static final List<String> REQUIRED_FIELDS = Collections.singletonList(HIVE_METASTORE_URIS);

    @SerializedName(value = "properties")
    private Map<String, String> properties;

    public HMSResource(String name) {
        super(name, ResourceType.HMS);
        this.properties = Maps.newHashMap();
    }

    @Override
    public void modifyProperties(Map<String, String> properties) throws DdlException {
        for (Map.Entry<String, String> kv : properties.entrySet()) {
            replaceIfEffectiveValue(this.properties, kv.getKey(), kv.getValue());
        }
        this.properties = getPropertiesFromDLF(this.properties);
        this.properties = getPropertiesFromGlue(this.properties);
        super.modifyProperties(this.properties);
    }

    @Override
    protected void setProperties(Map<String, String> properties) throws DdlException {
        for (String field : REQUIRED_FIELDS) {
            if (!properties.containsKey(field)) {
                throw new DdlException("Missing [" + field + "] in properties.");
            }
        }
        this.properties.putAll(properties);
        this.properties = getPropertiesFromDLF(this.properties);
        this.properties = getPropertiesFromGlue(this.properties);
    }

    @Override
    public Map<String, String> getCopiedProperties() {
        return Maps.newHashMap(properties);
    }

    @Override
    protected void getProcNodeData(BaseProcResult result) {
        String lowerCaseType = type.name().toLowerCase();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            result.addRow(Lists.newArrayList(name, lowerCaseType, entry.getKey(), entry.getValue()));
        }
    }

    public static Map<String, String> getPropertiesFromDLF(Map<String, String> res) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Get properties from hive-site.xml");
        }
        // read properties from hive-site.xml.
        HiveConf hiveConf = new HiveConf();
        String metastoreType = hiveConf.get(HIVE_METASTORE_TYPE);
        if (!DLF_TYPE.equalsIgnoreCase(metastoreType)) {
            return res;
        }

        // get following properties from hive-site.xml
        // 1. region and endpoint. eg: cn-beijing
        String region = hiveConf.get("dlf.catalog.region");
        if (!Strings.isNullOrEmpty(region)) {
            // See: https://help.aliyun.com/document_detail/31837.html
            // And add "-internal" to access oss within vpc
            // TODO: find to way to access oss on public?
            res.put(S3Resource.S3_REGION, "oss-" + region);
            res.put(S3Resource.S3_ENDPOINT, "http://oss-" + region + "-internal.aliyuncs.com");
        }

        // 2. ak and sk
        String ak = hiveConf.get("dlf.catalog.accessKeyId");
        String sk = hiveConf.get("dlf.catalog.accessKeySecret");
        if (!Strings.isNullOrEmpty(ak)) {
            res.put(S3Resource.S3_ACCESS_KEY, ak);
        }
        if (!Strings.isNullOrEmpty(sk)) {
            res.put(S3Resource.S3_SECRET_KEY, sk);
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Get properties for oss in hive-site.xml: {}", res);
        }
        return res;
    }

    public static Map<String, String> getPropertiesFromGlue(Map<String, String> res) {
        String metastoreType = res.get(HIVE_METASTORE_TYPE);
        if (!GLUE_TYPE.equalsIgnoreCase(metastoreType)) {
            return res;
        }

        // https://docs.aws.amazon.com/general/latest/gr/s3.html
        // Convert:
        // (
        //  "aws.region" = "us-east-1",
        //  "aws.glue.access-key" = "xx",
        //  "aws.glue.secret-key" = "yy"
        // )
        // To:
        // (
        //  "AWS_REGION" = "us-east-1",
        //  "AWS_ENDPOINT" = "s3.us-east-1.amazonaws.com"
        //  "AWS_ACCESS_KEY" = "xx",
        //  "AWS_SCRETE_KEY" = "yy"
        // )
        String region = res.get(AWSGlueConfig.AWS_REGION);
        if (!Strings.isNullOrEmpty(region)) {
            res.put(S3Resource.S3_REGION, region);
            res.put(S3Resource.S3_ENDPOINT, "s3." + region + ".amazonaws.com");
        }

        String ak = res.get(AWSGlueConfig.AWS_GLUE_ACCESS_KEY);
        String sk = res.get(AWSGlueConfig.AWS_GLUE_SECRET_KEY);
        if (!Strings.isNullOrEmpty(ak) && !Strings.isNullOrEmpty(sk)) {
            res.put(S3Resource.S3_ACCESS_KEY, ak);
            res.put(S3Resource.S3_SECRET_KEY, sk);
        }
        return res;
    }
}
