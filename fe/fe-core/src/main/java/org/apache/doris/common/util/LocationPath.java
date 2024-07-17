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
import org.apache.doris.datasource.property.constants.CosProperties;
import org.apache.doris.datasource.property.constants.ObsProperties;
import org.apache.doris.datasource.property.constants.OssProperties;
import org.apache.doris.datasource.property.constants.S3Properties;
import org.apache.doris.fs.FileSystemType;
import org.apache.doris.thrift.TFileType;

import com.google.common.base.Strings;
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
    private final LocationType locationType;
    private final String location;

    public enum LocationType {
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

    private LocationPath(String location) {
        this(location, Collections.emptyMap(), true);
    }

    public LocationPath(String location, Map<String, String> props) {
        this(location, props, true);
    }

    public LocationPath(String location, Map<String, String> props, boolean convertPath) {
        String scheme = parseScheme(location).toLowerCase();
        if (scheme.isEmpty()) {
            locationType = LocationType.NOSCHEME;
            this.location = location;
        } else {
            switch (scheme) {
                case FeConstants.FS_PREFIX_HDFS:
                    locationType = LocationType.HDFS;
                    // Need add hdfs host to location
                    String host = props.get(HdfsResource.DSF_NAMESERVICES);
                    this.location = convertPath ? normalizedHdfsPath(location, host) : location;
                    break;
                case FeConstants.FS_PREFIX_S3:
                    locationType = LocationType.S3;
                    this.location = location;
                    break;
                case FeConstants.FS_PREFIX_S3A:
                    locationType = LocationType.S3A;
                    this.location = convertPath ? convertToS3(location) : location;
                    break;
                case FeConstants.FS_PREFIX_S3N:
                    // include the check for multi locations and in a table, such as both s3 and hdfs are in a table.
                    locationType = LocationType.S3N;
                    this.location = convertPath ? convertToS3(location) : location;
                    break;
                case FeConstants.FS_PREFIX_BOS:
                    locationType = LocationType.BOS;
                    // use s3 client to access
                    this.location = convertPath ? convertToS3(location) : location;
                    break;
                case FeConstants.FS_PREFIX_GCS:
                    locationType = LocationType.GCS;
                    // use s3 client to access
                    this.location = convertPath ? convertToS3(location) : location;
                    break;
                case FeConstants.FS_PREFIX_OSS:
                    if (isHdfsOnOssEndpoint(location)) {
                        locationType = LocationType.OSS_HDFS;
                        this.location = location;
                    } else {
                        if (useS3EndPoint(props)) {
                            this.location = convertPath ? convertToS3(location) : location;
                        } else {
                            this.location = location;
                        }
                        locationType = LocationType.OSS;
                    }
                    break;
                case FeConstants.FS_PREFIX_COS:
                    if (useS3EndPoint(props)) {
                        this.location = convertPath ? convertToS3(location) : location;
                    } else {
                        this.location = location;
                    }
                    locationType = LocationType.COS;
                    break;
                case FeConstants.FS_PREFIX_OBS:
                    if (useS3EndPoint(props)) {
                        this.location = convertPath ? convertToS3(location) : location;
                    } else {
                        this.location = location;
                    }
                    locationType = LocationType.OBS;
                    break;
                case FeConstants.FS_PREFIX_OFS:
                    locationType = LocationType.OFS;
                    this.location = location;
                    break;
                case FeConstants.FS_PREFIX_JFS:
                    locationType = LocationType.JFS;
                    this.location = location;
                    break;
                case FeConstants.FS_PREFIX_GFS:
                    locationType = LocationType.GFS;
                    this.location = location;
                    break;
                case FeConstants.FS_PREFIX_COSN:
                    // if treat cosn(tencent hadoop-cos) as a s3 file system, may bring incompatible issues
                    locationType = LocationType.COSN;
                    this.location = location;
                    break;
                case FeConstants.FS_PREFIX_LAKEFS:
                    locationType = LocationType.COSN;
                    this.location = normalizedLakefsPath(location);
                    break;
                case FeConstants.FS_PREFIX_VIEWFS:
                    locationType = LocationType.VIEWFS;
                    this.location = location;
                    break;
                case FeConstants.FS_PREFIX_FILE:
                    locationType = LocationType.LOCAL;
                    this.location = location;
                    break;
                default:
                    locationType = LocationType.UNKNOWN;
                    this.location = location;
            }
        }
    }

    private static String parseScheme(String location) {
        String scheme = "";
        String[] schemeSplit = location.split(SCHEME_DELIM);
        if (schemeSplit.length > 1) {
            scheme = schemeSplit[0];
        } else {
            schemeSplit = location.split(NONSTANDARD_SCHEME_DELIM);
            if (schemeSplit.length > 1) {
                scheme = schemeSplit[0];
            }
        }

        // if not get scheme, need consider /path/to/local to no scheme
        if (scheme.isEmpty()) {
            try {
                Paths.get(location);
            } catch (InvalidPathException exception) {
                throw new IllegalArgumentException("Fail to parse scheme, invalid location: " + location);
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

    public static boolean isHdfsOnOssEndpoint(String location) {
        // example: cn-shanghai.oss-dls.aliyuncs.com contains the "oss-dls.aliyuncs".
        // https://www.alibabacloud.com/help/en/e-mapreduce/latest/oss-kusisurumen
        return location.contains("oss-dls.aliyuncs");
    }

    /**
     * The converted path is used for FE to get metadata
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

    public static Pair<FileSystemType, String> getFSIdentity(String location, String bindBrokerName) {
        LocationPath locationPath = new LocationPath(location);
        FileSystemType fsType = (bindBrokerName != null) ? FileSystemType.BROKER : locationPath.getFileSystemType();
        URI uri = locationPath.getPath().toUri();
        String fsIdent = Strings.nullToEmpty(uri.getScheme()) + "://" + Strings.nullToEmpty(uri.getAuthority());
        return Pair.of(fsType, fsIdent);
    }

    private FileSystemType getFileSystemType() {
        FileSystemType fsType;
        switch (locationType) {
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

    public TFileType getTFileTypeForBE() {
        switch (this.getLocationType()) {
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
        switch (locationType) {
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

    public LocationType getLocationType() {
        return locationType;
    }

    public String get() {
        return location;
    }

    public Path getPath() {
        return new Path(location);
    }

    public static String getTempWritePath(String loc, String prefix) {
        Path tempRoot = new Path(loc, prefix);
        Path tempPath = new Path(tempRoot, UUID.randomUUID().toString().replace("-", ""));
        return tempPath.toString();
    }

    @Override
    public String toString() {
        return get();
    }
}
