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

package org.apache.doris.broker.hdfs;

import org.apache.doris.common.WildcardURI;
import org.apache.doris.thrift.TBrokerFD;
import org.apache.doris.thrift.TBrokerFileStatus;
import org.apache.doris.thrift.TBrokerOperationStatusCode;

import com.google.common.base.Strings;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;
import org.apache.hadoop.fs.CommonConfigurationKeys;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.FileLock;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.PrivilegedExceptionAction;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class FileSystemManager {

    private static Logger logger = Logger
            .getLogger(FileSystemManager.class.getName());
    // supported scheme
    private static final String HDFS_SCHEME = "hdfs";
    private static final String VIEWFS_SCHEME = "viewfs";
    private static final String S3A_SCHEME = "s3a";
    private static final String KS3_SCHEME = "ks3";
    private static final String CHDFS_SCHEME = "ofs";
    private static final String OBS_SCHEME = "obs";
    private static final String OSS_SCHEME = "oss";
    private static final String COS_SCHEME = "cosn";
    private static final String BOS_SCHEME = "bos";
    private static final String JFS_SCHEME = "jfs";
    private static final String AFS_SCHEME = "afs";
    private static final String GFS_SCHEME = "gfs";

    private static final String GCS_SCHEME = "gs";
    
    private static final String FS_PREFIX = "fs.";
    
    private static final String GCS_PROJECT_ID_KEY = "fs.gs.project.id";

    private static final String USER_NAME_KEY = "username";
    private static final String PASSWORD_KEY = "password";
    private static final String AUTHENTICATION_SIMPLE = "simple";
    private static final String AUTHENTICATION_KERBEROS = "kerberos";
    private static final String KERBEROS_PRINCIPAL = "kerberos_principal";
    private static final String KERBEROS_KEYTAB = "kerberos_keytab";
    private static final String KERBEROS_KEYTAB_CONTENT = "kerberos_keytab_content";
    private static final String DFS_HA_NAMENODE_KERBEROS_PRINCIPAL_PATTERN =
            "dfs.namenode.kerberos.principal.pattern";
    // arguments for ha hdfs
    private static final String DFS_NAMESERVICES_KEY = "dfs.nameservices";
    private static final String DFS_HA_NAMENODES_PREFIX = "dfs.ha.namenodes.";
    private static final String DFS_HA_NAMENODE_RPC_ADDRESS_PREFIX = "dfs.namenode.rpc-address.";
    private static final String DFS_CLIENT_FAILOVER_PROXY_PROVIDER_PREFIX =
            "dfs.client.failover.proxy.provider.";
    private static final String DEFAULT_DFS_CLIENT_FAILOVER_PROXY_PROVIDER =
            "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider";
    private static final String FS_DEFAULTFS_KEY = "fs.defaultFS";
    // If this property is not set to "true", FileSystem instance will be returned from cache
    // which is not thread-safe and may cause 'Filesystem closed' exception when it is closed by other thread.
    private static final String FS_HDFS_IMPL_DISABLE_CACHE = "fs.hdfs.impl.disable.cache";

    // arguments for s3a
    private static final String FS_S3A_ACCESS_KEY = "fs.s3a.access.key";
    private static final String FS_S3A_SECRET_KEY = "fs.s3a.secret.key";
    private static final String FS_S3A_ENDPOINT = "fs.s3a.endpoint";
    // This property is used like 'fs.hdfs.impl.disable.cache'
    private static final String FS_S3A_IMPL_DISABLE_CACHE = "fs.s3a.impl.disable.cache";

    // arguments for obs
    private static final String FS_OBS_ACCESS_KEY = "fs.obs.access.key";
    private static final String FS_OBS_SECRET_KEY = "fs.obs.secret.key";
    private static final String FS_OBS_ENDPOINT = "fs.obs.endpoint";
    // This property is used like 'fs.hdfs.impl.disable.cache'
    private static final String FS_OBS_IMPL_DISABLE_CACHE = "fs.obs.impl.disable.cache";
    private static final String FS_OBS_IMPL = "fs.obs.impl";

    // arguments for ks3
    private static final String FS_KS3_ACCESS_KEY = "fs.ks3.AccessKey";
    private static final String FS_KS3_SECRET_KEY = "fs.ks3.AccessSecret";
    private static final String FS_KS3_ENDPOINT = "fs.ks3.endpoint";
    private static final String FS_KS3_IMPL = "fs.ks3.impl";
    // This property is used like 'fs.ks3.impl.disable.cache'
    private static final String FS_KS3_IMPL_DISABLE_CACHE = "fs.ks3.impl.disable.cache";

    // arguments for oss
    private static final String FS_OSS_ACCESS_KEY = "fs.oss.accessKeyId";
    private static final String FS_OSS_SECRET_KEY = "fs.oss.accessKeySecret";
    private static final String FS_OSS_ENDPOINT = "fs.oss.endpoint";
    // This property is used like 'fs.oss.impl.disable.cache'
    private static final String FS_OSS_IMPL_DISABLE_CACHE = "fs.oss.impl.disable.cache";
    private static final String FS_OSS_IMPL = "fs.oss.impl";

    // arguments for cos
    private static final String FS_COS_ACCESS_KEY = "fs.cosn.userinfo.secretId";
    private static final String FS_COS_SECRET_KEY = "fs.cosn.userinfo.secretKey";
    private static final String FS_COS_ENDPOINT = "fs.cosn.bucket.endpoint_suffix";
    private static final String FS_COS_IMPL = "fs.cosn.impl";
    private static final String FS_COS_IMPL_DISABLE_CACHE = "fs.cosn.impl.disable.cache";

    // arguments for bos
    private static final String FS_BOS_ACCESS_KEY = "fs.bos.access.key";
    private static final String FS_BOS_SECRET_KEY = "fs.bos.secret.access.key";
    private static final String FS_BOS_ENDPOINT = "fs.bos.endpoint";
    private static final String FS_BOS_IMPL = "fs.bos.impl";
    private static final String FS_BOS_MULTIPART_UPLOADS_BLOCK_SIZE = "fs.bos.multipart.uploads.block.size";

    // arguments for afs
    private static final String HADOOP_JOB_GROUP_NAME = "hadoop.job.group.name";
    private static final String HADOOP_JOB_UGI = "hadoop.job.ugi";
    private static final String FS_DEFAULT_NAME = "fs.default.name";
    private static final String FS_AFS_IMPL = "fs.afs.impl";
    private static final String DFS_AGENT_PORT = "dfs.agent.port";
    private static final String DFS_CLIENT_AUTH_METHOD = "dfs.client.auth.method";
    private static final String DFS_RPC_TIMEOUT = "dfs.rpc.timeout";

    private ScheduledExecutorService handleManagementPool = Executors.newScheduledThreadPool(2);

    private int readBufferSize = 128 << 10; // 128k
    private int writeBufferSize = 128 << 10; // 128k

    private ConcurrentHashMap<FileSystemIdentity, BrokerFileSystem> cachedFileSystem;
    private ClientContextManager clientContextManager;

    public FileSystemManager() {
        cachedFileSystem = new ConcurrentHashMap<>();
        clientContextManager = new ClientContextManager(handleManagementPool);
        readBufferSize = BrokerConfig.hdfs_read_buffer_size_kb << 10;
        writeBufferSize = BrokerConfig.hdfs_write_buffer_size_kb << 10;
    }

    private static String preparePrincipal(String originalPrincipal) throws UnknownHostException {
        String finalPrincipal = originalPrincipal;
        String[] components = originalPrincipal.split("[/@]");
        if (components != null && components.length == 3) {
            if (components[1].equals("_HOST")) {
                // Convert hostname(fqdn) to lower case according to SecurityUtil.getServerPrincipal
                finalPrincipal = components[0] + "/" +
                        StringUtils.toLowerCase(InetAddress.getLocalHost().getCanonicalHostName())
                        + "@" + components[2];
            } else if (components[1].equals("_IP")) {
                finalPrincipal = components[0] + "/" +
                        InetAddress.getByName(InetAddress.getLocalHost().getCanonicalHostName()).getHostAddress()
                        + "@" + components[2];
            }
        }

        return finalPrincipal;
    }

    /**
     * visible for test
     *
     * @param path
     * @param properties
     * @return BrokerFileSystem with different FileSystem based on scheme
     * @throws URISyntaxException
     * @throws Exception
     */
    public BrokerFileSystem getFileSystem(String path, Map<String, String> properties) {
        WildcardURI pathUri = new WildcardURI(path);
        String scheme = pathUri.getUri().getScheme();
        if (Strings.isNullOrEmpty(scheme)) {
            throw new BrokerException(TBrokerOperationStatusCode.INVALID_INPUT_FILE_PATH,
                "invalid path. scheme is null");
        }
        BrokerFileSystem brokerFileSystem = null;
        if (scheme.equals(HDFS_SCHEME) || scheme.equals(VIEWFS_SCHEME)) {
            brokerFileSystem = getDistributedFileSystem(path, properties);
        } else if (scheme.equals(S3A_SCHEME)) {
            brokerFileSystem = getS3AFileSystem(path, properties);
        } else if (scheme.equals(KS3_SCHEME)) {
            brokerFileSystem = getKS3FileSystem(path, properties);
        } else if (scheme.equals(CHDFS_SCHEME)) {
            brokerFileSystem = getChdfsFileSystem(path, properties);
        } else if (scheme.equals(OBS_SCHEME)) {
            brokerFileSystem = getOBSFileSystem(path, properties);
        } else if (scheme.equals(OSS_SCHEME)) {
            brokerFileSystem = getOSSFileSystem(path, properties);
        } else if (scheme.equals(COS_SCHEME)) {
            brokerFileSystem = getCOSFileSystem(path, properties);
        } else if (scheme.equals(BOS_SCHEME)) {
            brokerFileSystem = getBOSFileSystem(path, properties);
        } else if (scheme.equals(JFS_SCHEME)) {
            brokerFileSystem = getJuiceFileSystem(path, properties);
        } else if (scheme.equals(GFS_SCHEME)) {
            brokerFileSystem = getGooseFSFileSystem(path, properties);
        } else if (scheme.equals(GCS_SCHEME)) {
            brokerFileSystem = getGCSFileSystem(path, properties);
        } else {
            throw new BrokerException(TBrokerOperationStatusCode.INVALID_INPUT_FILE_PATH,
                "invalid path. scheme is not supported");
        }
        return brokerFileSystem;
    }

    /**
     * visible for test
     *
     * file system handle is cached, the identity is host + username_password
     * it will have safety problem if only hostname is used because one user may specify username and password
     * and then access hdfs, another user may not specify username and password but could also access data
     * @param path
     * @param properties
     * @return
     * @throws URISyntaxException
     * @throws Exception
     */
    public BrokerFileSystem getDistributedFileSystem(String path, Map<String, String> properties) {
        WildcardURI pathUri = new WildcardURI(path);
        String host = HDFS_SCHEME + "://" + pathUri.getAuthority();
        if (Strings.isNullOrEmpty(pathUri.getAuthority())) {
            if (properties.containsKey(FS_DEFAULTFS_KEY)) {
                host = properties.get(FS_DEFAULTFS_KEY);
                logger.info("no schema and authority in path. use fs.defaultFs");
            } else {
                logger.warn("invalid hdfs path. authority is null,path:" + path);
                throw  new BrokerException(TBrokerOperationStatusCode.INVALID_ARGUMENT,
                        "invalid hdfs path. authority is null");
            }
        }
        String username = properties.getOrDefault(USER_NAME_KEY, "");
        String password = properties.getOrDefault(PASSWORD_KEY, "");
        String dfsNameServices = properties.getOrDefault(DFS_NAMESERVICES_KEY, "");
        String authentication = properties.getOrDefault(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
            AUTHENTICATION_SIMPLE);
        if (Strings.isNullOrEmpty(authentication) || (!authentication.equals(AUTHENTICATION_SIMPLE)
            && !authentication.equals(AUTHENTICATION_KERBEROS))) {
            logger.warn("invalid authentication:" + authentication);
            throw new BrokerException(TBrokerOperationStatusCode.INVALID_ARGUMENT,
                "invalid authentication:" + authentication);
        }
        String hdfsUgi = username + "," + password;
        FileSystemIdentity fileSystemIdentity = null;
        if (authentication.equals(AUTHENTICATION_SIMPLE)) {
            fileSystemIdentity = new FileSystemIdentity(host, hdfsUgi);
        } else {
            // for kerberos, use host + principal + keytab as filesystemindentity
            String kerberosContent = "";
            if (properties.containsKey(KERBEROS_KEYTAB)) {
                kerberosContent = properties.get(KERBEROS_KEYTAB);
            } else if (properties.containsKey(KERBEROS_KEYTAB_CONTENT)) {
                kerberosContent = properties.get(KERBEROS_KEYTAB_CONTENT);
            } else {
                throw  new BrokerException(TBrokerOperationStatusCode.INVALID_ARGUMENT,
                        "keytab is required for kerberos authentication");
            }
            if (!properties.containsKey(KERBEROS_PRINCIPAL)) {
                throw  new BrokerException(TBrokerOperationStatusCode.INVALID_ARGUMENT,
                        "principal is required for kerberos authentication");
            } else {
                kerberosContent = kerberosContent + properties.get(KERBEROS_PRINCIPAL);
            }
            try {
                MessageDigest digest = MessageDigest.getInstance("md5");
                byte[] result = digest.digest(kerberosContent.getBytes());
                String kerberosUgi = new String(result);
                fileSystemIdentity = new FileSystemIdentity(host, kerberosUgi);
            } catch (NoSuchAlgorithmException e) {
                throw  new BrokerException(TBrokerOperationStatusCode.INVALID_ARGUMENT,
                        e.getMessage());
            }
        }
        BrokerFileSystem fileSystem = updateCachedFileSystem(fileSystemIdentity, properties);
        fileSystem.getLock().lock();
        try {
            if (fileSystem.getDFSFileSystem() == null) {
                logger.info("create file system for new path: " + path);
                UserGroupInformation ugi = null;

                // create a new filesystem
                Configuration conf = new HdfsConfiguration();

                // fallback when kerberos auth fail
                conf.set(CommonConfigurationKeys.IPC_CLIENT_FALLBACK_TO_SIMPLE_AUTH_ALLOWED_KEY, "true");

                // TODO get this param from properties
                // conf.set("dfs.replication", "2");
                String tmpFilePath = null;
                if (authentication.equals(AUTHENTICATION_KERBEROS)){
                    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
                            AUTHENTICATION_KERBEROS);

                    String principal = preparePrincipal(properties.get(KERBEROS_PRINCIPAL));
                    String keytab = "";
                    if (properties.containsKey(KERBEROS_KEYTAB)) {
                        keytab = properties.get(KERBEROS_KEYTAB);
                    } else if (properties.containsKey(KERBEROS_KEYTAB_CONTENT)) {
                        // pass kerberos keytab content use base64 encoding
                        // so decode it and write it to tmp path under /tmp
                        // because ugi api only accept a local path as argument
                        String keytab_content = properties.get(KERBEROS_KEYTAB_CONTENT);
                        byte[] base64decodedBytes = Base64.getDecoder().decode(keytab_content);
                        long currentTime = System.currentTimeMillis();
                        Random random = new SecureRandom();
                        int randNumber = random.nextInt(10000);
                        // different kerberos account has different file
                        tmpFilePath ="/tmp/." +
                                principal.replace('/', '_') +
                                "_" + currentTime +
                                "_" + randNumber +
                                "_" + Thread.currentThread().getId();
                        logger.info("create kerberos tmp file" + tmpFilePath);
                        try (FileOutputStream fileOutputStream = new FileOutputStream(tmpFilePath)) {
                            FileLock lock = fileOutputStream.getChannel().lock();
                            fileOutputStream.write(base64decodedBytes);
                            lock.release();
                        }
                        keytab = tmpFilePath;
                    } else {
                        throw  new BrokerException(TBrokerOperationStatusCode.INVALID_ARGUMENT,
                                "keytab is required for kerberos authentication");
                    }
                    UserGroupInformation.setConfiguration(conf);
                    ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytab);
                    if (properties.containsKey(KERBEROS_KEYTAB_CONTENT)) {
                        try {
                            File file = new File(tmpFilePath);
                            if(!file.delete()){
                                logger.warn("delete tmp file:" +  tmpFilePath + " failed");
                            }
                        } catch (Exception e) {
                            throw new  BrokerException(TBrokerOperationStatusCode.FILE_NOT_FOUND,
                                    e.getMessage());
                        }
                    }
                }
                if (!Strings.isNullOrEmpty(dfsNameServices)) {
                    // ha hdfs arguments
                    final String dfsHaNameNodesKey = DFS_HA_NAMENODES_PREFIX + dfsNameServices;
                    if (!properties.containsKey(dfsHaNameNodesKey)) {
                        throw  new BrokerException(TBrokerOperationStatusCode.INVALID_ARGUMENT,
                                "load request missed necessary arguments for ha mode");
                    }
                    String dfsHaNameNodes =  properties.get(dfsHaNameNodesKey);
                    conf.set(DFS_NAMESERVICES_KEY, dfsNameServices);
                    conf.set(dfsHaNameNodesKey, dfsHaNameNodes);
                    String[] nameNodes = dfsHaNameNodes.split(",");
                    if (nameNodes == null) {
                        throw  new BrokerException(TBrokerOperationStatusCode.INVALID_ARGUMENT,
                                "invalid " + dfsHaNameNodesKey + " configuration");
                    } else {
                        for (String nameNode : nameNodes) {
                            nameNode = nameNode.trim();
                            String nameNodeRpcAddress =
                                    DFS_HA_NAMENODE_RPC_ADDRESS_PREFIX + dfsNameServices + "." + nameNode;
                            if (!properties.containsKey(nameNodeRpcAddress)) {
                                throw  new BrokerException(TBrokerOperationStatusCode.INVALID_ARGUMENT,
                                        "missed " + nameNodeRpcAddress + " configuration");
                            } else {
                                conf.set(nameNodeRpcAddress, properties.get(nameNodeRpcAddress));
                            }
                        }
                    }

                    final String dfsClientFailoverProxyProviderKey =
                            DFS_CLIENT_FAILOVER_PROXY_PROVIDER_PREFIX + dfsNameServices;
                    if (properties.containsKey(dfsClientFailoverProxyProviderKey)) {
                        conf.set(dfsClientFailoverProxyProviderKey,
                                properties.get(dfsClientFailoverProxyProviderKey));
                    } else {
                        conf.set(dfsClientFailoverProxyProviderKey,
                                DEFAULT_DFS_CLIENT_FAILOVER_PROXY_PROVIDER);
                    }
                    if (properties.containsKey(FS_DEFAULTFS_KEY)) {
                        conf.set(FS_DEFAULTFS_KEY, properties.get(FS_DEFAULTFS_KEY));
                    }
                    if (properties.containsKey(DFS_HA_NAMENODE_KERBEROS_PRINCIPAL_PATTERN)) {
                        conf.set(DFS_HA_NAMENODE_KERBEROS_PRINCIPAL_PATTERN,
                            properties.get(DFS_HA_NAMENODE_KERBEROS_PRINCIPAL_PATTERN));
                    }
                }

                conf.set(FS_HDFS_IMPL_DISABLE_CACHE, "true");
                FileSystem dfsFileSystem = null;
                if (authentication.equals(AUTHENTICATION_SIMPLE) &&
                    properties.containsKey(USER_NAME_KEY) && !Strings.isNullOrEmpty(username)) {
                    // Use the specified 'username' as the login name
                    ugi = UserGroupInformation.createRemoteUser(username);
                    // make sure hadoop client know what auth method would be used now,
                    // don't set as default
                    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION, AUTHENTICATION_SIMPLE);
                    ugi.setAuthenticationMethod(UserGroupInformation.AuthenticationMethod.SIMPLE);
                }
                dfsFileSystem = ugi != null ?
                        ugi.doAs((PrivilegedExceptionAction<FileSystem>) () -> FileSystem.get(pathUri.getUri(), conf)) :
                        FileSystem.get(pathUri.getUri(), conf);
                fileSystem.setFileSystem(dfsFileSystem);
            }
            return fileSystem;
        } catch (Exception e) {
            logger.error("errors while connect to " + path, e);
            throw new BrokerException(TBrokerOperationStatusCode.NOT_AUTHORIZED, e);
        } finally {
            fileSystem.getLock().unlock();
        }
    }

    /**
     * visible for test
     *
     * file system handle is cached, the identity is host + accessKey_secretKey
     * @param path
     * @param properties
     * @return
     * @throws URISyntaxException
     * @throws Exception
     */
    public BrokerFileSystem getS3AFileSystem(String path, Map<String, String> properties) {
        WildcardURI pathUri = new WildcardURI(path);
        String accessKey = properties.getOrDefault(FS_S3A_ACCESS_KEY, "");
        String secretKey = properties.getOrDefault(FS_S3A_SECRET_KEY, "");
        String endpoint = properties.getOrDefault(FS_S3A_ENDPOINT, "");
        String host = S3A_SCHEME + "://" + endpoint;
        String disableCache = properties.getOrDefault(FS_S3A_IMPL_DISABLE_CACHE, "true");
        String s3aUgi = accessKey + "," + secretKey;
        FileSystemIdentity fileSystemIdentity = new FileSystemIdentity(host, s3aUgi);
        BrokerFileSystem fileSystem = updateCachedFileSystem(fileSystemIdentity, properties);
        fileSystem.getLock().lock();
        try {
            if (fileSystem.getDFSFileSystem() == null) {
                logger.info("create file system for new path " + path);
                // create a new filesystem
                Configuration conf = new Configuration();
                conf.set(FS_S3A_ACCESS_KEY, accessKey);
                conf.set(FS_S3A_SECRET_KEY, secretKey);
                conf.set(FS_S3A_ENDPOINT, endpoint);
                conf.set(FS_S3A_IMPL_DISABLE_CACHE, disableCache);
                FileSystem s3AFileSystem = FileSystem.get(pathUri.getUri(), conf);
                fileSystem.setFileSystem(s3AFileSystem);
            }
            return fileSystem;
        } catch (Exception e) {
            logger.error("errors while connect to " + path, e);
            throw new BrokerException(TBrokerOperationStatusCode.NOT_AUTHORIZED, e);
        } finally {
            fileSystem.getLock().unlock();
        }
    }

    /**
     *  get GCS file system
     * @param path gcs path
     * @param properties See {@link <a href="https://github.com/GoogleCloudDataproc/hadoop-connectors/blob/branch-2.2.x/gcs/CONFIGURATION.md)">...</a>}
     *                   in broker load scenario, the properties should contains fs.gs.project.id
     * @return GcsBrokerFileSystem
     */
    public BrokerFileSystem getGCSFileSystem(String path, Map<String, String> properties) {
        WildcardURI pathUri = new WildcardURI(path);
        String projectId = properties.get(GCS_PROJECT_ID_KEY);
        if (Strings.isNullOrEmpty(projectId)) {
            throw new BrokerException(TBrokerOperationStatusCode.INVALID_ARGUMENT,
                    "missed fs.gs.project.id configuration");
        }
        FileSystemIdentity fileSystemIdentity = new FileSystemIdentity(projectId, null);
        BrokerFileSystem fileSystem = updateCachedFileSystem(fileSystemIdentity, properties);
        fileSystem.getLock().lock();
        try {
            if (fileSystem.getDFSFileSystem() == null) {
                logger.info("create file system for new path " + path);
                // create a new filesystem
                Configuration conf = new Configuration();
                properties.forEach((k, v) -> {
                    if (Strings.isNullOrEmpty(v) && k.startsWith(FS_PREFIX)) {
                        conf.set(k, v);
                    }
                });
                FileSystem gcsFileSystem = FileSystem.get(pathUri.getUri(), conf);
                fileSystem.setFileSystem(gcsFileSystem);
            }
            return fileSystem;
        } catch (Exception e) {
            logger.error("errors while connect to " + path, e);
            throw new BrokerException(TBrokerOperationStatusCode.NOT_AUTHORIZED, e);
        } finally {
            fileSystem.getLock().unlock();
        }
    }

    /**
     * file system handle is cached, the identity is endpoint + bucket + accessKey_secretKey
     * @param path
     * @param properties
     * @return
     */
    public BrokerFileSystem getOBSFileSystem(String path, Map<String, String> properties) {
        WildcardURI pathUri = new WildcardURI(path);
        String accessKey = properties.getOrDefault(FS_OBS_ACCESS_KEY, "");
        String secretKey = properties.getOrDefault(FS_OBS_SECRET_KEY, "");
        String endpoint = properties.getOrDefault(FS_OBS_ENDPOINT, "");
        String disableCache = properties.getOrDefault(FS_OBS_IMPL_DISABLE_CACHE, "true");
        String host = OBS_SCHEME + "://" + endpoint + "/" + pathUri.getUri().getHost();
        String obsUgi = accessKey + "," + secretKey;
        FileSystemIdentity fileSystemIdentity = new FileSystemIdentity(host, obsUgi);
        cachedFileSystem.putIfAbsent(fileSystemIdentity, new BrokerFileSystem(fileSystemIdentity));
        BrokerFileSystem fileSystem = updateCachedFileSystem(fileSystemIdentity, properties);
        fileSystem.getLock().lock();
        try {
            if (fileSystem.getDFSFileSystem() == null) {
                logger.info("create file system for new path " + path);
                // create a new filesystem
                Configuration conf = new Configuration();
                conf.set(FS_OBS_ACCESS_KEY, accessKey);
                conf.set(FS_OBS_SECRET_KEY, secretKey);
                conf.set(FS_OBS_ENDPOINT, endpoint);
                conf.set(FS_OBS_IMPL, "org.apache.hadoop.fs.obs.OBSFileSystem");
                conf.set(FS_OBS_IMPL_DISABLE_CACHE, disableCache);
                FileSystem obsFileSystem = FileSystem.get(pathUri.getUri(), conf);
                fileSystem.setFileSystem(obsFileSystem);
            }
            return fileSystem;
        } catch (Exception e) {
            logger.error("errors while connect to " + path, e);
            throw new BrokerException(TBrokerOperationStatusCode.NOT_AUTHORIZED, e);
        } finally {
            fileSystem.getLock().unlock();
        }
    }

    /**
     * visible for test
     * <p>
     * file system handle is cached, the identity is endpoint + bucket + accessKey_secretKey
     *
     * @param path
     * @param properties
     * @return
     * @throws URISyntaxException
     * @throws Exception
     */
    public BrokerFileSystem getKS3FileSystem(String path, Map<String, String> properties) {
        WildcardURI pathUri = new WildcardURI(path);
        String accessKey = properties.getOrDefault(FS_KS3_ACCESS_KEY, "");
        String secretKey = properties.getOrDefault(FS_KS3_SECRET_KEY, "");
        String endpoint = properties.getOrDefault(FS_KS3_ENDPOINT, "");
        String disableCache = properties.getOrDefault(FS_KS3_IMPL_DISABLE_CACHE, "true");
        // endpoint is the server host, pathUri.getUri().getHost() is the bucket
        // we should use these two params as the host identity, because FileSystem will cache both.
        String host = KS3_SCHEME + "://" + endpoint + "/" + pathUri.getUri().getHost();
        String ks3aUgi = accessKey + "," + secretKey;
        FileSystemIdentity fileSystemIdentity = new FileSystemIdentity(host, ks3aUgi);
        BrokerFileSystem fileSystem = updateCachedFileSystem(fileSystemIdentity, properties);
        fileSystem.getLock().lock();
        try {
            if (fileSystem.getDFSFileSystem() == null) {
                logger.info("could not find file system for path " + path + " create a new one");
                // create a new filesystem
                Configuration conf = new Configuration();
                conf.set(FS_KS3_ACCESS_KEY, accessKey);
                conf.set(FS_KS3_SECRET_KEY, secretKey);
                conf.set(FS_KS3_ENDPOINT, endpoint);
                conf.set(FS_KS3_IMPL, "com.ksyun.kmr.hadoop.fs.ks3.Ks3FileSystem");
                conf.set(FS_KS3_IMPL_DISABLE_CACHE, disableCache);
                FileSystem ks3FileSystem = FileSystem.get(pathUri.getUri(), conf);
                fileSystem.setFileSystem(ks3FileSystem);
            }
            return fileSystem;
        } catch (Exception e) {
            logger.error("errors while connect to " + path, e);
            throw new BrokerException(TBrokerOperationStatusCode.NOT_AUTHORIZED, e);
        } finally {
            fileSystem.getLock().unlock();
        }
    }

   /**
     * file system handle is cached, the identity is endpoint + bucket + accessKey_secretKey
     * @param path
     * @param properties
     * @return
     */
    public BrokerFileSystem getOSSFileSystem(String path, Map<String, String> properties) {
        WildcardURI pathUri = new WildcardURI(path);
        String accessKey = properties.getOrDefault(FS_OSS_ACCESS_KEY, "");
        String secretKey = properties.getOrDefault(FS_OSS_SECRET_KEY, "");
        String endpoint = properties.getOrDefault(FS_OSS_ENDPOINT, "");
        String disableCache = properties.getOrDefault(FS_OSS_IMPL_DISABLE_CACHE, "true");
        String host = OSS_SCHEME + "://" + endpoint + "/" + pathUri.getUri().getHost();
        String ossUgi = accessKey + "," + secretKey;
        FileSystemIdentity fileSystemIdentity = new FileSystemIdentity(host, ossUgi);
        cachedFileSystem.putIfAbsent(fileSystemIdentity, new BrokerFileSystem(fileSystemIdentity));
        BrokerFileSystem fileSystem = updateCachedFileSystem(fileSystemIdentity, properties);
        fileSystem.getLock().lock();
        try {
            if (fileSystem.getDFSFileSystem() == null) {
                logger.info("create file system for new path " + path);
                // create a new filesystem
                Configuration conf = new Configuration();
                conf.set(FS_OSS_ACCESS_KEY, accessKey);
                conf.set(FS_OSS_SECRET_KEY, secretKey);
                conf.set(FS_OSS_ENDPOINT, endpoint);
                conf.set(FS_OSS_IMPL, "org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystem");
                conf.set(FS_OSS_IMPL_DISABLE_CACHE, disableCache);
                FileSystem ossFileSystem = FileSystem.get(pathUri.getUri(), conf);
                fileSystem.setFileSystem(ossFileSystem);
            }
            return fileSystem;
        } catch (Exception e) {
            logger.error("errors while connect to " + path, e);
            throw new BrokerException(TBrokerOperationStatusCode.NOT_AUTHORIZED, e);
        } finally {
            fileSystem.getLock().unlock();
        }
    }

    /**
     * visible for test
     *
     * file system handle is cached, the identity is for all chdfs.
     * @param path
     * @param properties
     * @return
     * @throws URISyntaxException
     * @throws Exception
     */
    public BrokerFileSystem getChdfsFileSystem(String path, Map<String, String> properties) {
        WildcardURI pathUri = new WildcardURI(path);
        String host = CHDFS_SCHEME;
        String authentication = properties.getOrDefault(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
                AUTHENTICATION_SIMPLE);
        if (Strings.isNullOrEmpty(authentication) || (!authentication.equals(AUTHENTICATION_SIMPLE)
                && !authentication.equals(AUTHENTICATION_KERBEROS))) {
            logger.warn("invalid authentication:" + authentication);
            throw new BrokerException(TBrokerOperationStatusCode.INVALID_ARGUMENT,
                    "invalid authentication:" + authentication);
        }

        FileSystemIdentity fileSystemIdentity = null;
        if (authentication.equals(AUTHENTICATION_SIMPLE)) {
            fileSystemIdentity = new FileSystemIdentity(host, "");
        } else {
            // for kerberos, use host + principal + keytab as filesystemindentity
            String kerberosContent = "";
            if (properties.containsKey(KERBEROS_KEYTAB)) {
                kerberosContent = properties.get(KERBEROS_KEYTAB);
            } else if (properties.containsKey(KERBEROS_KEYTAB_CONTENT)) {
                kerberosContent = properties.get(KERBEROS_KEYTAB_CONTENT);
            } else {
                throw new BrokerException(TBrokerOperationStatusCode.INVALID_ARGUMENT,
                        "keytab is required for kerberos authentication");
            }
            if (!properties.containsKey(KERBEROS_PRINCIPAL)) {
                throw new BrokerException(TBrokerOperationStatusCode.INVALID_ARGUMENT,
                        "principal is required for kerberos authentication");
            } else {
                kerberosContent = kerberosContent + properties.get(KERBEROS_PRINCIPAL);
            }
            try {
                MessageDigest digest = MessageDigest.getInstance("md5");
                byte[] result = digest.digest(kerberosContent.getBytes());
                String kerberosUgi = new String(result);
                fileSystemIdentity = new FileSystemIdentity(host, kerberosUgi);
            } catch (NoSuchAlgorithmException e) {
                throw  new BrokerException(TBrokerOperationStatusCode.INVALID_ARGUMENT,
                        e.getMessage());
            }
        }
        BrokerFileSystem fileSystem = updateCachedFileSystem(fileSystemIdentity, properties);
        fileSystem.getLock().lock();
        try {
            // create a new filesystem
            Configuration conf = new Configuration();
            for (Map.Entry<String, String> propElement : properties.entrySet()) {
                conf.set(propElement.getKey(), propElement.getValue());
            }

            if (fileSystem.getDFSFileSystem() == null) {
                logger.info("create file system for new path " + path);
                String tmpFilePath = null;
                if (authentication.equals(AUTHENTICATION_KERBEROS)){
                    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
                            AUTHENTICATION_KERBEROS);

                    String principal = preparePrincipal(properties.get(KERBEROS_PRINCIPAL));
                    String keytab = "";
                    if (properties.containsKey(KERBEROS_KEYTAB)) {
                        keytab = properties.get(KERBEROS_KEYTAB);
                    } else if (properties.containsKey(KERBEROS_KEYTAB_CONTENT)) {
                        // pass kerberos keytab content use base64 encoding
                        // so decode it and write it to tmp path under /tmp
                        // because ugi api only accept a local path as argument
                        String keytabContent = properties.get(KERBEROS_KEYTAB_CONTENT);
                        byte[] base64decodedBytes = Base64.getDecoder().decode(keytabContent);
                        long currentTime = System.currentTimeMillis();
                        Random random = new SecureRandom();
                        int randNumber = random.nextInt(10000);
                        tmpFilePath = "/tmp/." + currentTime + "_" + randNumber;
                        try (FileOutputStream fileOutputStream = new FileOutputStream(tmpFilePath)) {
                            fileOutputStream.write(base64decodedBytes);
                        }
                        keytab = tmpFilePath;
                    } else {
                        throw  new BrokerException(TBrokerOperationStatusCode.INVALID_ARGUMENT,
                                "keytab is required for kerberos authentication");
                    }
                    UserGroupInformation.setConfiguration(conf);
                    UserGroupInformation.loginUserFromKeytab(principal, keytab);
                    if (properties.containsKey(KERBEROS_KEYTAB_CONTENT)) {
                        try {
                            File file = new File(tmpFilePath);
                            if(!file.delete()){
                                logger.warn("delete tmp file:" +  tmpFilePath + " failed");
                            }
                        } catch (Exception e) {
                            throw new  BrokerException(TBrokerOperationStatusCode.FILE_NOT_FOUND,
                                    e.getMessage());
                        }
                    }
                }
                FileSystem chdfsFileSystem = FileSystem.get(pathUri.getUri(), conf);
                fileSystem.setFileSystem(chdfsFileSystem);
            }
            return fileSystem;
        } catch (Exception e) {
            logger.error("errors while connect to " + path, e);
            throw new BrokerException(TBrokerOperationStatusCode.NOT_AUTHORIZED, e);
        } finally {
            fileSystem.getLock().unlock();
        }
    }

    /**
     * visible for test
     * <p>
     * file system handle is cached, the identity is endpoint + bucket + accessKey_secretKey
     *
     * @param path
     * @param properties
     * @return
     * @throws URISyntaxException
     * @throws Exception
     */
    public BrokerFileSystem getCOSFileSystem(String path, Map<String, String> properties) {
        WildcardURI pathUri = new WildcardURI(path);
        String accessKey = properties.getOrDefault(FS_COS_ACCESS_KEY, "");
        String secretKey = properties.getOrDefault(FS_COS_SECRET_KEY, "");
        String endpoint = properties.getOrDefault(FS_COS_ENDPOINT, "");
        String disableCache = properties.getOrDefault(FS_COS_IMPL_DISABLE_CACHE, "true");
        // endpoint is the server host, pathUri.getUri().getHost() is the bucket
        // we should use these two params as the host identity, because FileSystem will cache both.
        String host = COS_SCHEME + "://" + endpoint + "/" + pathUri.getUri().getHost();
        String cosUgi = accessKey + "," + secretKey;
        FileSystemIdentity fileSystemIdentity = new FileSystemIdentity(host, cosUgi);
        BrokerFileSystem fileSystem = updateCachedFileSystem(fileSystemIdentity, properties);
        fileSystem.getLock().lock();
        try {
            if (fileSystem.getDFSFileSystem() == null) {
                logger.info("could not find file system for path " + path + " create a new one");
                // create a new filesystem
                Configuration conf = new Configuration();
                conf.set(FS_COS_ACCESS_KEY, accessKey);
                conf.set(FS_COS_SECRET_KEY, secretKey);
                conf.set(FS_COS_ENDPOINT, endpoint);
                conf.set(FS_COS_IMPL, "org.apache.hadoop.fs.CosFileSystem");
                conf.set(FS_COS_IMPL_DISABLE_CACHE, disableCache);
                FileSystem cosFileSystem = FileSystem.get(pathUri.getUri(), conf);
                fileSystem.setFileSystem(cosFileSystem);
            }
            return fileSystem;
        } catch (Exception e) {
            logger.error("errors while connect to " + path, e);
            throw new BrokerException(TBrokerOperationStatusCode.NOT_AUTHORIZED, e);
        } finally {
            fileSystem.getLock().unlock();
        }
    }

    /**
     * visible for test
     * <p>
     * file system handle is cached, the identity is endpoint + bucket + accessKey_secretKey
     *
     * @param path
     * @param properties
     * @return
     * @throws URISyntaxException
     * @throws Exception
     */
    public BrokerFileSystem getBOSFileSystem(String path, Map<String, String> properties) {
        WildcardURI pathUri = new WildcardURI(path);
        String accessKey = properties.getOrDefault(FS_BOS_ACCESS_KEY, "");
        String secretKey = properties.getOrDefault(FS_BOS_SECRET_KEY, "");
        String endpoint = properties.getOrDefault(FS_BOS_ENDPOINT, "");
        String multiPartUploadBlockSize = properties.getOrDefault(FS_BOS_MULTIPART_UPLOADS_BLOCK_SIZE, "9437184");
        // endpoint is the server host, pathUri.getUri().getHost() is the bucket
        // we should use these two params as the host identity, because FileSystem will cache both.
        String host = BOS_SCHEME + "://" + endpoint + "/" + pathUri.getUri().getHost();
        String bosUgi = accessKey + "," + secretKey;
        FileSystemIdentity fileSystemIdentity = new FileSystemIdentity(host, bosUgi);
        BrokerFileSystem fileSystem = updateCachedFileSystem(fileSystemIdentity, properties);
        fileSystem.getLock().lock();
        try {
            if (fileSystem.getDFSFileSystem() == null) {
                logger.info("could not find file system for path " + path + " create a new one");
                // create a new filesystem
                Configuration conf = new Configuration();
                conf.set(FS_BOS_ACCESS_KEY, accessKey);
                conf.set(FS_BOS_SECRET_KEY, secretKey);
                conf.set(FS_BOS_ENDPOINT, endpoint);
                conf.set(FS_BOS_IMPL, "org.apache.hadoop.fs.bos.BaiduBosFileSystem");
                conf.set(FS_BOS_MULTIPART_UPLOADS_BLOCK_SIZE, multiPartUploadBlockSize);
                FileSystem bosFileSystem = FileSystem.get(pathUri.getUri(), conf);
                fileSystem.setFileSystem(bosFileSystem);
            }
            return fileSystem;
        } catch (Exception e) {
            logger.error("errors while connect to " + path, e);
            throw new BrokerException(TBrokerOperationStatusCode.NOT_AUTHORIZED, e);
        } finally {
            fileSystem.getLock().unlock();
        }
    }

    /**
     * visible for test
     *
     * file system handle is cached, the identity is for all juicefs.
     * @param path
     * @param properties
     * @return
     * @throws URISyntaxException
     * @throws Exception
     */
    public BrokerFileSystem getJuiceFileSystem(String path, Map<String, String> properties) {
        WildcardURI pathUri = new WildcardURI(path);
        String host = JFS_SCHEME;
        if (Strings.isNullOrEmpty(pathUri.getAuthority())) {
            if (properties.containsKey(FS_DEFAULTFS_KEY)) {
                host = properties.get(FS_DEFAULTFS_KEY);
                logger.info("no schema and authority in path. use fs.defaultFs");
            } else {
                logger.warn("invalid jfs path. authority is null,path:" + path);
                throw  new BrokerException(TBrokerOperationStatusCode.INVALID_ARGUMENT,
                    "invalid jfs path. authority is null");
            }
        }
        String authentication = properties.getOrDefault(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
            AUTHENTICATION_SIMPLE);
        if (Strings.isNullOrEmpty(authentication) || (!authentication.equals(AUTHENTICATION_SIMPLE)
            && !authentication.equals(AUTHENTICATION_KERBEROS))) {
            logger.warn("invalid authentication:" + authentication);
            throw new BrokerException(TBrokerOperationStatusCode.INVALID_ARGUMENT,
                "invalid authentication:" + authentication);
        }

        FileSystemIdentity fileSystemIdentity = null;
        if (authentication.equals(AUTHENTICATION_SIMPLE)) {
            fileSystemIdentity = new FileSystemIdentity(host, "");
        } else {
            // for kerberos, use host + principal + keytab as filesystemindentity
            String kerberosContent = "";
            if (properties.containsKey(KERBEROS_KEYTAB)) {
                kerberosContent = properties.get(KERBEROS_KEYTAB);
            } else if (properties.containsKey(KERBEROS_KEYTAB_CONTENT)) {
                kerberosContent = properties.get(KERBEROS_KEYTAB_CONTENT);
            } else {
                throw new BrokerException(TBrokerOperationStatusCode.INVALID_ARGUMENT,
                    "keytab is required for kerberos authentication");
            }
            if (!properties.containsKey(KERBEROS_PRINCIPAL)) {
                throw new BrokerException(TBrokerOperationStatusCode.INVALID_ARGUMENT,
                    "principal is required for kerberos authentication");
            } else {
                kerberosContent = kerberosContent + properties.get(KERBEROS_PRINCIPAL);
            }
            try {
                MessageDigest digest = MessageDigest.getInstance("md5");
                byte[] result = digest.digest(kerberosContent.getBytes());
                String kerberosUgi = new String(result);
                fileSystemIdentity = new FileSystemIdentity(host, kerberosUgi);
            } catch (NoSuchAlgorithmException e) {
                throw  new BrokerException(TBrokerOperationStatusCode.INVALID_ARGUMENT,
                    e.getMessage());
            }
        }
        BrokerFileSystem fileSystem = updateCachedFileSystem(fileSystemIdentity, properties);
        fileSystem.getLock().lock();
        try {
            // create a new filesystem
            Configuration conf = new Configuration();
            for (Map.Entry<String, String> propElement : properties.entrySet()) {
                conf.set(propElement.getKey(), propElement.getValue());
            }

            if (fileSystem.getDFSFileSystem() == null) {
                logger.info("create file system for new path " + path);
                String tmpFilePath = null;
                if (authentication.equals(AUTHENTICATION_KERBEROS)){
                    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
                        AUTHENTICATION_KERBEROS);

                    String principal = preparePrincipal(properties.get(KERBEROS_PRINCIPAL));
                    String keytab = "";
                    if (properties.containsKey(KERBEROS_KEYTAB)) {
                        keytab = properties.get(KERBEROS_KEYTAB);
                    } else if (properties.containsKey(KERBEROS_KEYTAB_CONTENT)) {
                        // pass kerberos keytab content use base64 encoding
                        // so decode it and write it to tmp path under /tmp
                        // because ugi api only accept a local path as argument
                        String keytabContent = properties.get(KERBEROS_KEYTAB_CONTENT);
                        byte[] base64decodedBytes = Base64.getDecoder().decode(keytabContent);
                        long currentTime = System.currentTimeMillis();
                        Random random = new SecureRandom();
                        int randNumber = random.nextInt(10000);
                        tmpFilePath = "/tmp/." + currentTime + "_" + randNumber;
                        try (FileOutputStream fileOutputStream = new FileOutputStream(tmpFilePath)) {
                            fileOutputStream.write(base64decodedBytes);
                        }
                        keytab = tmpFilePath;
                    } else {
                        throw new BrokerException(TBrokerOperationStatusCode.INVALID_ARGUMENT,
                            "keytab is required for kerberos authentication");
                    }
                    UserGroupInformation.setConfiguration(conf);
                    UserGroupInformation.loginUserFromKeytab(principal, keytab);
                    if (properties.containsKey(KERBEROS_KEYTAB_CONTENT)) {
                        try {
                            File file = new File(tmpFilePath);
                            if(!file.delete()){
                                logger.warn("delete tmp file:" +  tmpFilePath + " failed");
                            }
                        } catch (Exception e) {
                            throw new  BrokerException(TBrokerOperationStatusCode.FILE_NOT_FOUND,
                                e.getMessage());
                        }
                    }
                }
                FileSystem jfsFileSystem = FileSystem.get(pathUri.getUri(), conf);
                fileSystem.setFileSystem(jfsFileSystem);
            }
            return fileSystem;
        } catch (Exception e) {
            logger.error("errors while connect to " + path, e);
            throw new BrokerException(TBrokerOperationStatusCode.NOT_AUTHORIZED, e);
        } finally {
            fileSystem.getLock().unlock();
        }
    }

    private BrokerFileSystem getAfsFileSystem(String path, Map<String, String> properties) {
        URI pathUri = new WildcardURI(path).getUri();
        String host = pathUri.getScheme() + "://" + pathUri.getAuthority();
        String username = properties.containsKey(USER_NAME_KEY) ? properties.get(USER_NAME_KEY) : "";
        String password = properties.containsKey(PASSWORD_KEY) ? properties.get(PASSWORD_KEY) : "";
        String group = properties.containsKey(HADOOP_JOB_GROUP_NAME) ? properties.get(HADOOP_JOB_GROUP_NAME) : "";
        String afsUgi = username + "," + password;
        FileSystemIdentity fileSystemIdentity = new FileSystemIdentity(host, afsUgi, group);
        cachedFileSystem.putIfAbsent(fileSystemIdentity, new BrokerFileSystem(fileSystemIdentity));
        BrokerFileSystem fileSystem = updateCachedFileSystem(fileSystemIdentity, properties);
        fileSystem.getLock().lock();
        try {
            if (!cachedFileSystem.containsKey(fileSystemIdentity)) {
                // this means the file system is closed by file system checker thread
                // it is a corner case
                return null;
            }
            if (fileSystem.getDFSFileSystem() == null) {
                logger.info("could not find file system for path " + path + " create a new one");
                // create a new file system
                Configuration conf = new Configuration();
                conf.set(HADOOP_JOB_UGI, afsUgi);
                conf.set(HADOOP_JOB_GROUP_NAME, group);
                conf.set(FS_DEFAULT_NAME, host);
                conf.set(FS_AFS_IMPL, "org.apache.hadoop.fs.DFileSystem");
                conf.set(DFS_CLIENT_AUTH_METHOD, properties.getOrDefault(DFS_CLIENT_AUTH_METHOD, "3"));
                conf.set(DFS_AGENT_PORT, properties.getOrDefault(DFS_AGENT_PORT, "20001"));
                conf.set(DFS_RPC_TIMEOUT, properties.getOrDefault(DFS_RPC_TIMEOUT, "300000"));
                FileSystem dfsFileSystem = FileSystem.get(URI.create(host), conf);
                fileSystem.setFileSystem(dfsFileSystem);
            }
            return fileSystem;
        } catch (Exception e) {
            logger.error("errors while connect to " + path, e);
            throw new BrokerException(TBrokerOperationStatusCode.NOT_AUTHORIZED, e, e.getMessage());
        } finally {
            fileSystem.getLock().unlock();
        }
    }

    /**
     * @param path
     * @param properties
     * @return
     * @throws URISyntaxException
     * @throws Exception
     */
    public BrokerFileSystem getGooseFSFileSystem(String path, Map<String, String> properties) {
        WildcardURI pathUri = new WildcardURI(path);
        // endpoint is the server host, pathUri.getUri().getHost() is the bucket
        // we should use these two params as the host identity, because FileSystem will cache both.
        String host = GFS_SCHEME + "://" + pathUri.getAuthority();

        String username = properties.getOrDefault(USER_NAME_KEY, "");
        String password = properties.getOrDefault(PASSWORD_KEY, "");
        String gfsUgi = username + "," + password;
        FileSystemIdentity fileSystemIdentity = new FileSystemIdentity(host, gfsUgi);
        BrokerFileSystem brokerFileSystem = updateCachedFileSystem(fileSystemIdentity, properties);
        brokerFileSystem.getLock().lock();
        try {
            if (brokerFileSystem.getDFSFileSystem() == null) {
                logger.info("create goosefs client: " + path);
                Configuration conf = new Configuration();
                for (Map.Entry<String, String> propElement : properties.entrySet()) {
                    conf.set(propElement.getKey(), propElement.getValue());
                }
                FileSystem fileSystem = FileSystem.get(pathUri.getUri(), conf);
                brokerFileSystem.setFileSystem(fileSystem);
            }
            return brokerFileSystem;
        } catch (Exception e) {
            logger.error("errors while connect to " + path, e);
            throw new BrokerException(TBrokerOperationStatusCode.NOT_AUTHORIZED, e);
        } finally {
            brokerFileSystem.getLock().unlock();
        }
    }

    public List<TBrokerFileStatus> listPath(String path, boolean fileNameOnly, Map<String, String> properties) {
        List<TBrokerFileStatus> resultFileStatus = null;
        WildcardURI pathUri = new WildcardURI(path);
        BrokerFileSystem fileSystem = getFileSystem(path, properties);
        Path pathPattern = new Path(pathUri.getPath());
        try {
            FileStatus[] files = fileSystem.getDFSFileSystem().globStatus(pathPattern);
            if (files == null) {
                resultFileStatus = new ArrayList<>(0);
                return resultFileStatus;
            }
            resultFileStatus = new ArrayList<>(files.length);
            for (FileStatus fileStatus : files) {
                TBrokerFileStatus brokerFileStatus = new TBrokerFileStatus();
                brokerFileStatus.setIsDir(fileStatus.isDirectory());
                if (fileStatus.isDirectory()) {
                    brokerFileStatus.setIsSplitable(false);
                    brokerFileStatus.setSize(-1);
                } else {
                    brokerFileStatus.setSize(fileStatus.getLen());
                    brokerFileStatus.setIsSplitable(true);
                }
                brokerFileStatus.setModificationTime(fileStatus.getModificationTime());
                if (fileNameOnly) {
                    // return like this: file.txt
                    brokerFileStatus.setPath(fileStatus.getPath().getName());
                } else {
                    // return like this: //path/to/your/file.txt
                    brokerFileStatus.setPath(fileStatus.getPath().toString());
                }
                resultFileStatus.add(brokerFileStatus);
            }
        } catch (FileNotFoundException e) {
            logger.info("file not found: " + e.getMessage());
            throw new BrokerException(TBrokerOperationStatusCode.FILE_NOT_FOUND,
                    e, "file not found");
        } catch (Exception e) {
            logger.error("errors while get file status ", e);
            fileSystem.closeFileSystem();
            throw new BrokerException(TBrokerOperationStatusCode.TARGET_STORAGE_SERVICE_ERROR,
                    e, "unknown error when get file status");
        }
        return resultFileStatus;
    }

    public void deletePath(String path, Map<String, String> properties) {
        WildcardURI pathUri = new WildcardURI(path);
        BrokerFileSystem fileSystem = getFileSystem(path, properties);
        Path filePath = new Path(pathUri.getPath());
        try {
            fileSystem.getDFSFileSystem().delete(filePath, true);
        } catch (IOException e) {
            logger.error("errors while delete path " + path);
            fileSystem.closeFileSystem();
            throw new BrokerException(TBrokerOperationStatusCode.TARGET_STORAGE_SERVICE_ERROR,
                    e, "delete path {} error", path);
        }
    }

    public void renamePath(String srcPath, String destPath, Map<String, String> properties) {
        WildcardURI srcPathUri = new WildcardURI(srcPath);
        WildcardURI destPathUri = new WildcardURI(destPath);
        if (!srcPathUri.getAuthority().trim().equals(destPathUri.getAuthority().trim())) {
            throw new BrokerException(TBrokerOperationStatusCode.TARGET_STORAGE_SERVICE_ERROR,
                    "only allow rename in same file system");
        }
        BrokerFileSystem fileSystem = getFileSystem(srcPath, properties);
        Path srcfilePath = new Path(srcPathUri.getPath());
        Path destfilePath = new Path(destPathUri.getPath());
        try {
            boolean isRenameSuccess = fileSystem.getDFSFileSystem().rename(srcfilePath, destfilePath);
            if (!isRenameSuccess) {
                throw new BrokerException(TBrokerOperationStatusCode.TARGET_STORAGE_SERVICE_ERROR,
                        "failed to rename path from {} to {}", srcPath, destPath);
            }
        } catch (IOException e) {
            logger.error("errors while rename path from " + srcPath + " to " + destPath);
            fileSystem.closeFileSystem();
            throw new BrokerException(TBrokerOperationStatusCode.TARGET_STORAGE_SERVICE_ERROR,
                    e, "errors while rename {} to {}", srcPath, destPath);
        }
    }

    public boolean checkPathExist(String path, Map<String, String> properties) {
        WildcardURI pathUri = new WildcardURI(path);
        BrokerFileSystem fileSystem = getFileSystem(path, properties);
        Path filePath = new Path(pathUri.getPath());
        try {
            boolean isPathExist = fileSystem.getDFSFileSystem().exists(filePath);
            return isPathExist;
        } catch (IOException e) {
            logger.error("errors while check path exist: " + path);
            fileSystem.closeFileSystem();
            throw new BrokerException(TBrokerOperationStatusCode.TARGET_STORAGE_SERVICE_ERROR,
                    e, "errors while check if path {} exist", path);
        }
    }

    public TBrokerFD openReader(String clientId, String path, long startOffset, Map<String, String> properties) {
        WildcardURI pathUri = new WildcardURI(path);
        Path inputFilePath = new Path(pathUri.getPath());
        BrokerFileSystem fileSystem = getFileSystem(path, properties);
        try {
            FSDataInputStream fsDataInputStream = fileSystem.getDFSFileSystem().open(inputFilePath, readBufferSize);
            fsDataInputStream.seek(startOffset);
            UUID uuid = UUID.randomUUID();
            TBrokerFD fd = parseUUIDToFD(uuid);
            clientContextManager.putNewInputStream(clientId, fd, fsDataInputStream, fileSystem);
            return fd;
        } catch (IOException e) {
            logger.error("errors while open path", e);
            fileSystem.closeFileSystem();
            throw new BrokerException(TBrokerOperationStatusCode.TARGET_STORAGE_SERVICE_ERROR,
                    e, "could not open file {}", path);
        }
    }

    public ByteBuffer pread(TBrokerFD fd, long offset, long length) {
        FSDataInputStream fsDataInputStream = clientContextManager.getFsDataInputStream(fd);
        synchronized (fsDataInputStream) {
            long currentStreamOffset;
            try {
                currentStreamOffset = fsDataInputStream.getPos();
            } catch (IOException e) {
                logger.error("errors while get file pos from output stream", e);
                throw new BrokerException(TBrokerOperationStatusCode.TARGET_STORAGE_SERVICE_ERROR,
                        "errors while get file pos from output stream");
            }
            if (currentStreamOffset != offset) {
                // it's ok, when reading some format like parquet, it is not a sequential read
                logger.debug("invalid offset, current read offset is "
                        + currentStreamOffset + " is not equal to request offset "
                        + offset + " seek to it");
                try {
                    fsDataInputStream.seek(offset);
                } catch (IOException e) {
                    throw new BrokerException(TBrokerOperationStatusCode.INVALID_INPUT_OFFSET,
                            e, "current read offset {} is not equal to {}, and could not seek to it",
                            currentStreamOffset, offset);
                }
            }
            // Avoid using the ByteBuffer based read for Hadoop because some FSDataInputStream
            // implementations are not ByteBufferReadable,
            // See https://issues.apache.org/jira/browse/HADOOP-14603
            int hasRead = 0;
            byte[] buf = new byte[(int)length];
            while (hasRead < length) {
                int bufSize = Math.min((int) length - hasRead, readBufferSize);
                try {
                    int readLength = fsDataInputStream.read(buf, hasRead, bufSize);
                    if (readLength < 0) {
                        // If no data is read, just return EOF
                        // Otherwise, return the read data.
                        // Because the "length" may be longer than the rest length of the file.
                        if (hasRead == 0) {
                            throw new BrokerException(TBrokerOperationStatusCode.END_OF_FILE,
                                    "end of file reached");
                        }
                        break;
                    }
                    if (logger.isDebugEnabled()) {
                        logger.debug("read buffer from input stream, buffer size:" + buf.length + ", read length:"
                                + readLength);
                    }
                    hasRead += readLength;
                } catch (IOException e) {
                    logger.error("errors while read data from stream", e);
                    throw new BrokerException(TBrokerOperationStatusCode.TARGET_STORAGE_SERVICE_ERROR,
                            e, "errors while read data from stream");
                }
            }
            if (hasRead != length) {
                logger.debug("broker pread return diff length. read bytes: " + hasRead + ", request bytes: " + length);
            }
            return ByteBuffer.wrap(buf, 0, hasRead);
        }
    }

    public void seek(TBrokerFD fd, long offset) {
        throw new BrokerException(TBrokerOperationStatusCode.OPERATION_NOT_SUPPORTED,
                "seek this method is not supported");
    }

    public void closeReader(TBrokerFD fd) {
        FSDataInputStream fsDataInputStream = clientContextManager.getFsDataInputStream(fd);
        synchronized (fsDataInputStream) {
            try {
                fsDataInputStream.close();
            } catch (IOException e) {
                logger.error("errors while close file input stream", e);
                throw new BrokerException(TBrokerOperationStatusCode.TARGET_STORAGE_SERVICE_ERROR,
                        e, "errors while close file input stream");
            } finally {
                clientContextManager.removeInputStream(fd);
            }
        }
    }

    public TBrokerFD openWriter(String clientId, String path, Map<String, String> properties) {
        WildcardURI pathUri = new WildcardURI(path);
        Path inputFilePath = new Path(pathUri.getPath());
        BrokerFileSystem fileSystem = getFileSystem(path, properties);
        try {
            FSDataOutputStream fsDataOutputStream = fileSystem.getDFSFileSystem().create(inputFilePath,
                    true, writeBufferSize);
            UUID uuid = UUID.randomUUID();
            TBrokerFD fd = parseUUIDToFD(uuid);
            clientContextManager.putNewOutputStream(clientId, fd, fsDataOutputStream, fileSystem);
            return fd;
        } catch (IOException e) {
            logger.error("errors while open path", e);
            fileSystem.closeFileSystem();
            throw new BrokerException(TBrokerOperationStatusCode.TARGET_STORAGE_SERVICE_ERROR,
                    e, "could not open file {}", path);
        }
    }

    public void pwrite(TBrokerFD fd, long offset, byte[] data) {
        FSDataOutputStream fsDataOutputStream = clientContextManager.getFsDataOutputStream(fd);
        synchronized (fsDataOutputStream) {
            long currentStreamOffset;
            try {
                currentStreamOffset = fsDataOutputStream.getPos();
            } catch (IOException e) {
                logger.error("errors while get file pos from output stream", e);
                throw new BrokerException(TBrokerOperationStatusCode.TARGET_STORAGE_SERVICE_ERROR,
                        "errors while get file pos from output stream");
            }
            if (currentStreamOffset != offset) {
                // it's ok, it means that last pwrite succeed finally
                if (currentStreamOffset == offset + data.length) {
                    logger.warn("invalid offset, but current outputstream offset is "
                        + currentStreamOffset + ", data length is " + data.length + ", the sum is equal to request offset "
                        + offset + ", so just skip it");
                } else {
                    throw new BrokerException(TBrokerOperationStatusCode.INVALID_INPUT_OFFSET,
                        "current outputstream offset is {} not equal to request {}",
                        currentStreamOffset, offset);
                }
            } else {
                try {
                    fsDataOutputStream.write(data);
                } catch (IOException e) {
                    logger.error("errors while write data to output stream", e);
                    throw new BrokerException(TBrokerOperationStatusCode.TARGET_STORAGE_SERVICE_ERROR,
                        e, "errors while write data to output stream");
                }
            }
        }
    }

    public void closeWriter(TBrokerFD fd) {
        FSDataOutputStream fsDataOutputStream = clientContextManager.getFsDataOutputStream(fd);
        synchronized (fsDataOutputStream) {
            try {
                fsDataOutputStream.flush();
                fsDataOutputStream.close();
            } catch (IOException e) {
                logger.error("errors while close file output stream", e);
                throw new BrokerException(TBrokerOperationStatusCode.TARGET_STORAGE_SERVICE_ERROR,
                        e, "errors while close file output stream");
            } finally {
                clientContextManager.removeOutputStream(fd);
            }
        }
    }

    public void ping(String clientId) {
        clientContextManager.onPing(clientId);
    }

    private static TBrokerFD parseUUIDToFD(UUID uuid) {
        return new TBrokerFD(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
    }

    /**
     *   In view of the different expiration mechanisms of different authentication modes，
     *   there are two ways to determine whether BrokerFileSystem has expired:
     *   1. For the authentication mode of Kerberos and S3 aksk, use the createTime to determine whether it expires
     *   2. For other authentication modes, the lastAccessTime is used to determine whether it has expired
     */
    private BrokerFileSystem updateCachedFileSystem(FileSystemIdentity fileSystemIdentity, Map<String, String> properties) {
        BrokerFileSystem brokerFileSystem;
        if (cachedFileSystem.containsKey(fileSystemIdentity)) {
            brokerFileSystem = cachedFileSystem.get(fileSystemIdentity);
            if (properties.containsKey(KERBEROS_KEYTAB) && properties.containsKey(KERBEROS_PRINCIPAL)) {
                if (brokerFileSystem.isExpiredByCreateTime(BrokerConfig.client_expire_seconds)) {
                    logger.info("file system " + brokerFileSystem + " is expired, update it.");
                    try {
                        Configuration conf = new HdfsConfiguration();
                        conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION, AUTHENTICATION_KERBEROS);
                        UserGroupInformation ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(
                            preparePrincipal(properties.get(KERBEROS_PRINCIPAL)), properties.get(KERBEROS_KEYTAB));
                        // update FileSystem TGT
                        ugi.checkTGTAndReloginFromKeytab();
                    } catch (Exception e) {
                        logger.error("errors while checkTGTAndReloginFromKeytab: ", e);
                    }
                }
            } else if (brokerFileSystem.isExpiredByLastAccessTime(BrokerConfig.client_expire_seconds)) {
                brokerFileSystem.getLock().lock();
                try {
                    logger.info("file system " + brokerFileSystem + " is expired, update it.");
                    brokerFileSystem.closeFileSystem();
                    brokerFileSystem.getLock().unlock();
                } catch (Throwable t) {
                    logger.error("errors while close file system: ", t);
                }
                brokerFileSystem = new BrokerFileSystem(fileSystemIdentity);
                cachedFileSystem.put(fileSystemIdentity, brokerFileSystem);
            }
        } else {
            brokerFileSystem = new BrokerFileSystem(fileSystemIdentity);
            cachedFileSystem.put(fileSystemIdentity, brokerFileSystem);
        }
        return brokerFileSystem;
    }

    public long fileSize(String path, Map<String, String> properties) {
        WildcardURI pathUri = new WildcardURI(path);
        BrokerFileSystem fileSystem = getFileSystem(path, properties);
        Path filePath = new Path(pathUri.getPath());
        try {
            FileStatus fileStatus = fileSystem.getDFSFileSystem().getFileStatus(filePath);
            if (fileStatus.isDirectory()) {
                throw new BrokerException(TBrokerOperationStatusCode.INVALID_INPUT_FILE_PATH,
                    "not a file: {}", path);
            }
            return fileStatus.getLen();
        } catch (IOException e) {
            logger.error("errors while getting file size: " + path);
            fileSystem.closeFileSystem();
            throw new BrokerException(TBrokerOperationStatusCode.TARGET_STORAGE_SERVICE_ERROR,
                    e, "errors while getting file size {}", path);
        }
    }
}

