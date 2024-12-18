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

package org.apache.doris.common.util;

import org.apache.doris.catalog.HdfsResource;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Pair;
import org.apache.doris.datasource.hive.HMSExternalCatalog;
import org.apache.doris.datasource.property.constants.CosProperties;
import org.apache.doris.datasource.property.constants.ObsProperties;
import org.apache.doris.datasource.property.constants.OssProperties;
import org.apache.doris.datasource.property.constants.S3Properties;
import org.apache.doris.fs.FileSystemType;
import org.apache.doris.thrift.TFileType;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.InvalidPathException;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;

public class LocationPath {
    private static final Logger LOG = LogManager.getLogger(LocationPath.class);
    private static final String SCHEME_DELIM = "://";
    private static final String NONSTANDARD_SCHEME_DELIM = ":/";
    private final Scheme scheme;
    private final String location;
    private final boolean isBindBroker;

    public enum Scheme {
        HDFS,
        LOCAL, // Local File
        BOS, // Baidu
        GCS, // Google,
        OBS, // Huawei,
        COS, // Tencent
        COSN, // Tencent
        OFS, // Tencent CHDFS
        GFS, // Tencent GooseFs,
        LAKEFS, // used by Tencent DLC
        OSS, // Alibaba,
        OSS_HDFS, // JindoFS on OSS
        JFS, // JuiceFS,
        S3,
        S3A,
        S3N,
        VIEWFS,
        UNKNOWN,
        NOSCHEME // no scheme info
    }

    @VisibleForTesting
    public LocationPath(String location) {
        this(location, Maps.newHashMap(), true);
    }

    public LocationPath(String location, Map<String, String> props) {
        this(location, props, true);
    }

    private LocationPath(String originLocation, Map<String, String> props, boolean convertPath) {
        isBindBroker = props.containsKey(HMSExternalCatalog.BIND_BROKER_NAME);
        String tmpLocation = originLocation;
        if (!(originLocation.contains(SCHEME_DELIM) || originLocation.contains(NONSTANDARD_SCHEME_DELIM))) {
            // Sometimes the file path does not contain scheme, need to add default fs
            // eg, /path/to/file.parquet -> hdfs://nn/path/to/file.parquet
            // the default fs is from the catalog properties
            String defaultFS = props.getOrDefault(HdfsResource.HADOOP_FS_NAME, "");
            tmpLocation = defaultFS + originLocation;
        }
        String scheme = parseScheme(tmpLocation).toLowerCase();
        switch (scheme) {
            case "":
                this.scheme = Scheme.NOSCHEME;
                break;
            case FeConstants.FS_PREFIX_HDFS:
                this.scheme = Scheme.HDFS;
                // Need add hdfs host to location
                String host = props.get(HdfsResource.DSF_NAMESERVICES);
                tmpLocation = convertPath ? normalizedHdfsPath(tmpLocation, host) : tmpLocation;
                break;
            case FeConstants.FS_PREFIX_S3:
                this.scheme = Scheme.S3;
                break;
            case FeConstants.FS_PREFIX_S3A:
                this.scheme = Scheme.S3A;
                tmpLocation = convertPath ? convertToS3(tmpLocation) : tmpLocation;
                break;
            case FeConstants.FS_PREFIX_S3N:
                // include the check for multi locations and in a table, such as both s3 and hdfs are in a table.
                this.scheme = Scheme.S3N;
                tmpLocation = convertPath ? convertToS3(tmpLocation) : tmpLocation;
                break;
            case FeConstants.FS_PREFIX_BOS:
                this.scheme = Scheme.BOS;
                // use s3 client to access
                tmpLocation = convertPath ? convertToS3(tmpLocation) : tmpLocation;
                break;
            case FeConstants.FS_PREFIX_GCS:
                this.scheme = Scheme.GCS;
                // use s3 client to access
                tmpLocation = convertPath ? convertToS3(tmpLocation) : tmpLocation;
                break;
            case FeConstants.FS_PREFIX_OSS:
                String endpoint = "";
                if (props.containsKey(OssProperties.ENDPOINT)) {
                    endpoint = props.get(OssProperties.ENDPOINT);
                    if (endpoint.startsWith(OssProperties.OSS_PREFIX)) {
                        // may use oss.oss-cn-beijing.aliyuncs.com
                        endpoint = endpoint.replace(OssProperties.OSS_PREFIX, "");
                    }
                } else if (props.containsKey(S3Properties.ENDPOINT)) {
                    endpoint = props.get(S3Properties.ENDPOINT);
                } else if (props.containsKey(S3Properties.Env.ENDPOINT)) {
                    endpoint = props.get(S3Properties.Env.ENDPOINT);
                }
                if (isHdfsOnOssEndpoint(endpoint)) {
                    this.scheme = Scheme.OSS_HDFS;
                } else {
                    if (useS3EndPoint(props)) {
                        tmpLocation = convertPath ? convertToS3(tmpLocation) : tmpLocation;
                    }
                    this.scheme = Scheme.OSS;
                }
                break;
            case FeConstants.FS_PREFIX_COS:
                if (useS3EndPoint(props)) {
                    tmpLocation = convertPath ? convertToS3(tmpLocation) : tmpLocation;
                }
                this.scheme = Scheme.COS;
                break;
            case FeConstants.FS_PREFIX_OBS:
                if (useS3EndPoint(props)) {
                    tmpLocation = convertPath ? convertToS3(tmpLocation) : tmpLocation;
                }
                this.scheme = Scheme.OBS;
                break;
            case FeConstants.FS_PREFIX_OFS:
                this.scheme = Scheme.OFS;
                break;
            case FeConstants.FS_PREFIX_JFS:
                this.scheme = Scheme.JFS;
                break;
            case FeConstants.FS_PREFIX_GFS:
                this.scheme = Scheme.GFS;
                break;
            case FeConstants.FS_PREFIX_COSN:
                // if treat cosn(tencent hadoop-cos) as a s3 file system, may bring incompatible issues
                this.scheme = Scheme.COSN;
                break;
            case FeConstants.FS_PREFIX_LAKEFS:
                this.scheme = Scheme.COSN;
                tmpLocation = normalizedLakefsPath(tmpLocation);
                break;
            case FeConstants.FS_PREFIX_VIEWFS:
                this.scheme = Scheme.VIEWFS;
                break;
            case FeConstants.FS_PREFIX_FILE:
                this.scheme = Scheme.LOCAL;
                break;
            default:
                this.scheme = Scheme.UNKNOWN;
                break;
        }
        this.location = tmpLocation;
    }

    // Return true if this location is with oss-hdfs
    public static boolean isHdfsOnOssEndpoint(String location) {
        // example: cn-shanghai.oss-dls.aliyuncs.com contains the "oss-dls.aliyuncs".
        // https://www.alibabacloud.com/help/en/e-mapreduce/latest/oss-kusisurumen
        return location.contains("oss-dls.aliyuncs");
    }

    // Return the file system type and the file system identity.
    // The file system identity is the scheme and authority of the URI, eg. "hdfs://host:port" or "s3://bucket".
    public static Pair<FileSystemType, String> getFSIdentity(String location, String bindBrokerName) {
        LocationPath locationPath = new LocationPath(location, Collections.emptyMap(), true);
        FileSystemType fsType = (bindBrokerName != null) ? FileSystemType.BROKER : locationPath.getFileSystemType();
        URI uri = locationPath.getPath().toUri();
        String fsIdent = Strings.nullToEmpty(uri.getScheme()) + "://" + Strings.nullToEmpty(uri.getAuthority());
        return Pair.of(fsType, fsIdent);
    }

    /**
     * provide file type for BE.
     *
     * @param location the location is from fs.listFile
     * @return on BE, we will use TFileType to get the suitable client to access storage.
     */
    public static TFileType getTFileTypeForBE(String location) {
        if (location == null || location.isEmpty()) {
            return null;
        }
        LocationPath locationPath = new LocationPath(location, Collections.emptyMap(), false);
        return locationPath.getTFileTypeForBE();
    }

    public static String getTempWritePath(String loc, String prefix) {
        Path tempRoot = new Path(loc, prefix);
        Path tempPath = new Path(tempRoot, UUID.randomUUID().toString().replace("-", ""));
        return tempPath.toString();
    }

    public TFileType getTFileTypeForBE() {
        switch (scheme) {
            case S3:
            case S3A:
            case S3N:
            case COS:
            case OSS:
            case OBS:
            case BOS:
            case GCS:
                // ATTN, for COSN, on FE side, use HadoopFS to access, but on BE, use S3 client to access.
            case COSN:
            case LAKEFS:
                // now we only support S3 client for object storage on BE
                return TFileType.FILE_S3;
            case HDFS:
            case OSS_HDFS: // if hdfs service is enabled on oss, use hdfs lib to access oss.
            case VIEWFS:
                return TFileType.FILE_HDFS;
            case GFS:
            case JFS:
            case OFS:
                return TFileType.FILE_BROKER;
            case LOCAL:
                return TFileType.FILE_LOCAL;
            default:
                return null;
        }
    }

    /**
     * The converted path is used for BE
     *
     * @return BE scan range path
     */
    public Path toStorageLocation() {
        switch (scheme) {
            case S3:
            case S3A:
            case S3N:
            case COS:
            case OSS:
            case OBS:
            case BOS:
            case GCS:
            case COSN:
                // All storage will use s3 client to access on BE, so need convert to s3
                return new Path(convertToS3(location));
            case HDFS:
            case OSS_HDFS:
            case VIEWFS:
            case GFS:
            case JFS:
            case OFS:
            case LOCAL:
            default:
                return getPath();
        }
    }

    public Scheme getScheme() {
        return scheme;
    }

    public String get() {
        return location;
    }

    public Path getPath() {
        return new Path(location);
    }

    public boolean isBindBroker() {
        return isBindBroker;
    }

    private static String parseScheme(String finalLocation) {
        String scheme = "";
        String[] schemeSplit = finalLocation.split(SCHEME_DELIM);
        if (schemeSplit.length > 1) {
            scheme = schemeSplit[0];
        } else {
            schemeSplit = finalLocation.split(NONSTANDARD_SCHEME_DELIM);
            if (schemeSplit.length > 1) {
                scheme = schemeSplit[0];
            }
        }

        // if not get scheme, need consider /path/to/local to no scheme
        if (scheme.isEmpty()) {
            try {
                Paths.get(finalLocation);
            } catch (InvalidPathException exception) {
                throw new IllegalArgumentException("Fail to parse scheme, invalid location: " + finalLocation);
            }
        }

        return scheme;
    }

    private boolean useS3EndPoint(Map<String, String> props) {
        if (props.containsKey(ObsProperties.ENDPOINT)
                || props.containsKey(OssProperties.ENDPOINT)
                || props.containsKey(CosProperties.ENDPOINT)) {
            return false;
        }
        // wide check range for the compatibility of s3 properties
        return (props.containsKey(S3Properties.ENDPOINT) || props.containsKey(S3Properties.Env.ENDPOINT));
    }

    /**
     * The converted path is used for FE to get metadata.
     * Change http://xxxx to s3://xxxx
     *
     * @param location origin location
     * @return metadata location path. just convert when storage is compatible with s3 client.
     */
    private static String convertToS3(String location) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("try convert location to s3 prefix: " + location);
        }
        int pos = findDomainPos(location);
        return "s3" + location.substring(pos);
    }

    private static int findDomainPos(String rangeLocation) {
        int pos = rangeLocation.indexOf("://");
        if (pos == -1) {
            throw new RuntimeException("No '://' found in location: " + rangeLocation);
        }
        return pos;
    }

    private static String normalizedHdfsPath(String location, String host) {
        try {
            // Hive partition may contain special characters such as ' ', '<', '>' and so on.
            // Need to encode these characters before creating URI.
            // But doesn't encode '/' and ':' so that we can get the correct uri host.
            location = URLEncoder.encode(location, StandardCharsets.UTF_8.name())
                .replace("%2F", "/").replace("%3A", ":");
            URI normalizedUri = new URI(location);
            // compatible with 'hdfs:///' or 'hdfs:/'
            if (StringUtils.isEmpty(normalizedUri.getHost())) {
                location = URLDecoder.decode(location, StandardCharsets.UTF_8.name());
                String normalizedPrefix = HdfsResource.HDFS_PREFIX + "//";
                String brokenPrefix = HdfsResource.HDFS_PREFIX + "/";
                if (location.startsWith(brokenPrefix) && !location.startsWith(normalizedPrefix)) {
                    location = location.replace(brokenPrefix, normalizedPrefix);
                }
                if (StringUtils.isNotEmpty(host)) {
                    // Replace 'hdfs://key/' to 'hdfs://name_service/key/'
                    // Or hdfs:///abc to hdfs://name_service/abc
                    return location.replace(normalizedPrefix, normalizedPrefix + host + "/");
                } else {
                    // 'hdfs://null/' equals the 'hdfs:///'
                    if (location.startsWith(HdfsResource.HDFS_PREFIX + "///")) {
                        // Do not support hdfs:///location
                        throw new RuntimeException("Invalid location with empty host: " + location);
                    } else {
                        // Replace 'hdfs://key/' to '/key/', try access local NameNode on BE.
                        return location.replace(normalizedPrefix, "/");
                    }
                }
            }
            return URLDecoder.decode(location, StandardCharsets.UTF_8.name());
        } catch (URISyntaxException | UnsupportedEncodingException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    private static String normalizedLakefsPath(String location) {
        int atIndex = location.indexOf("@dlc");
        if (atIndex != -1) {
            return "lakefs://" + location.substring(atIndex + 1);
        } else {
            return location;
        }
    }

    public FileSystemType getFileSystemType() {
        FileSystemType fsType;
        switch (scheme) {
            case S3:
            case S3A:
            case S3N:
            case COS:
            case OSS:
            case OBS:
            case BOS:
            case GCS:
                // All storage will use s3 client to access on BE, so need convert to s3
                fsType = FileSystemType.S3;
                break;
            case COSN:
                // COSN use s3 client on FE side, because it need to complete multi-part uploading files on FE side.
                fsType = FileSystemType.S3;
                break;
            case OFS:
                // ofs:// use the underlying file system: Tencent Cloud HDFS, aka CHDFS)) {
                fsType = FileSystemType.OFS;
                break;
            case HDFS:
            case OSS_HDFS: // if hdfs service is enabled on oss, use hdfs lib to access oss.
            case VIEWFS:
            case GFS:
                fsType = FileSystemType.DFS;
                break;
            case JFS:
                fsType = FileSystemType.JFS;
                break;
            case LOCAL:
                fsType = FileSystemType.FILE;
                break;
            default:
                throw new UnsupportedOperationException("Unknown file system for location: " + location);
        }
        return fsType;
    }

    @Override
    public String toString() {
        return get();
    }
}
