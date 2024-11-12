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

package org.apache.doris.fs.remote;

import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.security.authentication.AuthenticationConfig;
import org.apache.doris.common.security.authentication.HadoopAuthenticator;
import org.apache.doris.common.security.authentication.HadoopKerberosAuthenticator;
import org.apache.doris.common.security.authentication.HadoopSimpleAuthenticator;
import org.apache.doris.common.util.LocationPath;
import org.apache.doris.fs.FileSystemCache;
import org.apache.doris.fs.FileSystemType;
import org.apache.doris.fs.remote.dfs.DFSFileSystem;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import mockit.Mock;
import mockit.MockUp;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Map;

public class RemoteFileSystemTest {

    @Test
    public void testFilesystemAndAuthType() throws UserException {

        // These paths should use s3 filesystem, and use simple auth
        ArrayList<String> s3Paths = new ArrayList<>();
        s3Paths.add("s3://a/b/c");
        s3Paths.add("s3a://a/b/c");
        s3Paths.add("s3n://a/b/c");
        s3Paths.add("oss://a/b/c");  // default use s3 filesystem
        s3Paths.add("gs://a/b/c");
        s3Paths.add("bos://a/b/c");
        s3Paths.add("cos://a/b/c");
        s3Paths.add("cosn://a/b/c");
        s3Paths.add("lakefs://a/b/c");
        s3Paths.add("obs://a/b/c");

        // These paths should use dfs filesystem, and auth will be changed by configure
        ArrayList<String> dfsPaths = new ArrayList<>();
        dfsPaths.add("ofs://a/b/c");
        dfsPaths.add("gfs://a/b/c");
        dfsPaths.add("hdfs://a/b/c");
        dfsPaths.add("oss://a/b/c");  // if endpoint contains 'oss-dls.aliyuncs', will use dfs filesystem

        new MockUp<UserGroupInformation>(UserGroupInformation.class) {
            @Mock
            public <T> T doAs(PrivilegedExceptionAction<T> action) throws IOException, InterruptedException {
                return (T) new LocalFileSystem();
            }
        };

        new MockUp<HadoopKerberosAuthenticator>(HadoopKerberosAuthenticator.class) {
            @Mock
            public synchronized UserGroupInformation getUGI() throws IOException {
                return UserGroupInformation.getCurrentUser();
            }
        };

        Configuration confWithoutKerberos = new Configuration();

        Configuration confWithKerberosIncomplete = new Configuration();
        confWithKerberosIncomplete.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION, "kerberos");

        Configuration confWithKerberos = new Configuration();
        confWithKerberos.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
        confWithKerberos.set(AuthenticationConfig.HADOOP_KERBEROS_PRINCIPAL, "principal");
        confWithKerberos.set(AuthenticationConfig.HADOOP_KERBEROS_KEYTAB, "keytab");

        ImmutableMap<String, String> s3props = ImmutableMap.of("s3.endpoint", "http://127.0.0.1");
        s3props.forEach(confWithKerberos::set);
        s3props.forEach(confWithoutKerberos::set);
        s3props.forEach(confWithKerberosIncomplete::set);

        for (String path : s3Paths) {
            checkS3Filesystem(path, confWithKerberos, s3props);
        }
        for (String path : s3Paths) {
            checkS3Filesystem(path, confWithKerberosIncomplete, s3props);
        }
        for (String path : s3Paths) {
            checkS3Filesystem(path, confWithoutKerberos, s3props);
        }

        s3props = ImmutableMap.of("s3.endpoint", "oss://xx-oss-dls.aliyuncs/abc");
        System.setProperty("java.security.krb5.realm", "realm");
        System.setProperty("java.security.krb5.kdc", "kdc");

        for (String path : dfsPaths) {
            checkDFSFilesystem(path, confWithKerberos, HadoopKerberosAuthenticator.class.getName(), s3props);
        }
        for (String path : dfsPaths) {
            checkDFSFilesystem(path, confWithKerberosIncomplete, HadoopSimpleAuthenticator.class.getName(), s3props);
        }
        for (String path : dfsPaths) {
            checkDFSFilesystem(path, confWithoutKerberos, HadoopSimpleAuthenticator.class.getName(), s3props);
        }

    }

    private void checkS3Filesystem(String path, Configuration conf, Map<String, String> m) throws UserException {
        RemoteFileSystem fs = createFs(path, conf, m);
        Assert.assertTrue(fs instanceof S3FileSystem);
        HadoopAuthenticator authenticator = ((S3FileSystem) fs).getAuthenticator();
        Assert.assertTrue(authenticator instanceof HadoopSimpleAuthenticator);
    }

    private void checkDFSFilesystem(String path, Configuration conf, String authClass, Map<String, String> m) throws UserException {
        RemoteFileSystem fs = createFs(path, conf, m);
        Assert.assertTrue(fs instanceof DFSFileSystem);
        HadoopAuthenticator authenticator = ((DFSFileSystem) fs).getAuthenticator();
        Assert.assertEquals(authClass, authenticator.getClass().getName());
    }

    private RemoteFileSystem createFs(String path, Configuration conf, Map<String, String> m) throws UserException {
        LocationPath locationPath = new LocationPath(path, m);
        FileSystemType fileSystemType = locationPath.getFileSystemType();
        URI uri = locationPath.getPath().toUri();
        String fsIdent = Strings.nullToEmpty(uri.getScheme()) + "://" + Strings.nullToEmpty(uri.getAuthority());
        FileSystemCache fileSystemCache = new FileSystemCache();
        RemoteFileSystem fs = fileSystemCache.getRemoteFileSystem(
            new FileSystemCache.FileSystemCacheKey(
                Pair.of(fileSystemType, fsIdent),
                ImmutableMap.of(),
                null,
                conf));
        fs.nativeFileSystem(path);
        return fs;
    }

}
