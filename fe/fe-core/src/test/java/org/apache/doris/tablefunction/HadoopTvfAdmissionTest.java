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

package org.apache.doris.tablefunction;

import org.apache.doris.common.Config;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.tablefunction.HadoopTvfAdmission.Access;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

class HadoopTvfAdmissionTest {
    private boolean originalEnable;

    @BeforeEach
    void setUp() {
        originalEnable = Config.enable_local_tvf;
        Config.enable_local_tvf = false;
    }

    @AfterEach
    void tearDown() {
        Config.enable_local_tvf = originalEnable;
    }

    @Test
    void javaMountKeysPreserveNulWhileNativeKeysAreCStringPrefixes() {
        Map<String, String> properties = viewFs();
        properties.put("fs.viewfs.mounttable.cluster.link./data\0ignored", "hdfs://remote/base");
        properties.put("fs.viewfs.mounttable.cluster.linkFallback", "file:///tmp");
        Assertions.assertDoesNotThrow(() -> HadoopTvfAdmission.check(
                Access.NATIVE_WRITE, "/data/sub/prefix_", properties));
        Assertions.assertThrows(AnalysisException.class, () -> HadoopTvfAdmission.check(
                Access.JAVA_DELETE, "/data/sub/", properties));
        Assertions.assertThrows(AnalysisException.class, () -> HadoopTvfAdmission.check(
                Access.HADOOP_READ, "viewfs://cluster/data/sub/file.csv", properties));
    }

    @Test
    void javaImplementationKeysAreNotTruncatedAtNul() {
        Map<String, String> properties = new HashMap<>();
        properties.put("fs.defaultFS", "hdfs://remote");
        properties.put("fs.hdfs.impl\0ignored", "org.apache.hadoop.fs.LocalFileSystem\0ignored");
        Assertions.assertDoesNotThrow(() -> HadoopTvfAdmission.check(
                Access.JAVA_DELETE, "/data/sub/", properties));
        Assertions.assertThrows(AnalysisException.class, () -> HadoopTvfAdmission.check(
                Access.NATIVE_WRITE, "/data/sub/prefix_", properties));
    }

    @Test
    void nativeAuthorityCompletionUsesTheRemainingLegacyConfiguration() {
        Map<String, String> properties = new HashMap<>();
        properties.put("fs.defaultFS", "viewfs:///");
        properties.put("fs.default.name", "viewfs://local");
        properties.put("fs.viewfs.mounttable.default.link./data", "hdfs://remote/base");
        properties.put("fs.viewfs.mounttable.local.link./data", "file:///tmp");
        Assertions.assertThrows(AnalysisException.class, () -> HadoopTvfAdmission.check(
                Access.NATIVE_WRITE, "/data/sub/prefix_", properties));
        properties.put("fs.viewfs.mounttable.local.link./data", "hdfs://remote/base");
        Assertions.assertDoesNotThrow(() -> HadoopTvfAdmission.check(
                Access.NATIVE_WRITE, "/data/sub/prefix_", properties));
    }

    @Test
    void distributedViewWrappersCannotBeMistakenForDirectRemoteProviders() {
        Map<String, String> properties = new HashMap<>();
        properties.put("fs.defaultFS", "hdfs://cluster");
        properties.put("fs.hdfs.impl", "org.apache.hadoop.hdfs.ViewDistributedFileSystem");
        properties.put("fs.viewfs.mounttable.cluster.link./data", "file:///tmp");
        for (Access access : Access.values()) {
            Assertions.assertThrows(AnalysisException.class, () -> HadoopTvfAdmission.check(
                    access, "hdfs://cluster/data/sub/file.csv", properties));
        }
        Config.enable_local_tvf = true;
        Assertions.assertDoesNotThrow(() -> HadoopTvfAdmission.check(
                Access.NATIVE_WRITE, "/data/sub/prefix_", properties));
        Assertions.assertThrows(AnalysisException.class, () -> HadoopTvfAdmission.check(
                Access.HADOOP_READ, "hdfs://cluster/data/sub/file.csv", properties));
    }

    @Test
    void hadoopReadsRejectLocalDelegationEvenWithLocalOptIn() {
        Map<String, String> properties = viewFs();
        properties.put("fs.viewfs.mounttable.cluster.link./data", "file:///tmp");
        for (boolean enabled : new boolean[] {false, true}) {
            Config.enable_local_tvf = enabled;
            AnalysisException error = Assertions.assertThrows(AnalysisException.class,
                    () -> HadoopTvfAdmission.check(Access.HADOOP_READ,
                            "viewfs://cluster/data/sub/file.csv", properties));
            Assertions.assertTrue(error.getMessage().contains("Use local()"));
        }
    }

    @Test
    void remoteReadsDoNotRejectAnUnrelatedLocalSiblingMount() {
        Map<String, String> properties = viewFs();
        properties.put("fs.viewfs.mounttable.cluster.link./data", "hdfs://remote/base");
        properties.put("fs.viewfs.mounttable.cluster.link./local", "file:///tmp");
        Assertions.assertDoesNotThrow(() -> HadoopTvfAdmission.check(
                Access.HADOOP_READ, "viewfs://cluster/data/file.csv", properties));
    }

    @Test
    void readPatternsCheckEveryIntersectingMountBeforeListing() {
        Map<String, String> properties = viewFs();
        properties.put("fs.viewfs.mounttable.cluster.link./data", "hdfs://remote/base");
        properties.put("fs.viewfs.mounttable.cluster.link./data/local", "file:///tmp");
        for (String pattern : new String[] {"/data/*", "/data/?/file.csv", "/data/{a,b}/file.csv", "/data"}) {
            Assertions.assertThrows(AnalysisException.class, () -> HadoopTvfAdmission.checkReadDiscovery("viewfs://cluster" + pattern, properties));
        }
        Assertions.assertDoesNotThrow(() -> HadoopTvfAdmission.checkReadDiscovery("viewfs://cluster/data/remote/file.csv", properties));
        properties.put("fs.viewfs.mounttable.cluster.link./data/local", "hdfs://remote/other");
        Assertions.assertDoesNotThrow(() -> HadoopTvfAdmission.checkReadDiscovery("viewfs://cluster/data/*", properties));
    }

    @Test
    void schemaAndDataReadNameNodesAreCheckedIndependently() {
        Map<String, String> properties = new HashMap<>();
        properties.put("fs.defaultFS", "viewfs://schema");
        properties.put("fs.viewfs.mounttable.schema.link./viewfs:", "file:///tmp");
        properties.put("fs.viewfs.mounttable.data.link./data", "hdfs://remote/base");
        Assertions.assertThrows(AnalysisException.class, () -> HadoopTvfAdmission.check(
                Access.HADOOP_READ, "viewfs://data/data/file.csv", properties));
        properties.put("fs.viewfs.mounttable.schema.link./viewfs:", "hdfs://remote/schema");
        Assertions.assertDoesNotThrow(() -> HadoopTvfAdmission.check(
                Access.HADOOP_READ, "viewfs://data/data/file.csv", properties));
    }

    @Test
    void unknownImplementationsAndRuntimeMountsNeedExplicitWriteOptIn() {
        Map<String, String> properties = new HashMap<>();
        properties.put("fs.defaultFS", "hdfs://remote");
        properties.put("fs.hdfs.impl", "example.DynamicFileSystem");
        Assertions.assertThrows(AnalysisException.class, () -> HadoopTvfAdmission.check(
                Access.NATIVE_WRITE, "/data/prefix_", properties));
        Config.enable_local_tvf = true;
        Assertions.assertDoesNotThrow(() -> HadoopTvfAdmission.check(
                Access.NATIVE_WRITE, "/data/prefix_", properties));
        Assertions.assertThrows(AnalysisException.class, () -> HadoopTvfAdmission.check(
                Access.HADOOP_READ, "hdfs://remote/data/file.csv", properties));
    }

    @Test
    void nativeProjectionRejectsConflictingCStringKeys() {
        Map<String, String> properties = viewFs();
        properties.put("fs.viewfs.mounttable.cluster.link./data", "hdfs://remote/base");
        properties.put("fs.viewfs.mounttable.cluster.link./data\0ignored", "file:///tmp");
        Assertions.assertThrows(AnalysisException.class, () -> HadoopTvfAdmission.check(
                Access.NATIVE_WRITE, "/data/sub/prefix_", properties));
    }

    @Test
    void nativeNameNodeCaseCollisionsCannotChooseAnUncheckedProvider() {
        Map<String, String> properties = new HashMap<>();
        properties.put("fs.defaultFS", "hdfs://remote");
        properties.put("fs.DEFAULTFS", "file:///");
        Assertions.assertThrows(AnalysisException.class, () -> HadoopTvfAdmission.check(
                Access.NATIVE_WRITE, "/data/prefix_", properties));
    }

    @Test
    void nestedRemoteReadPatternsReuseTheSameMountInterpreter() {
        Map<String, String> properties = viewFs();
        properties.put("fs.viewfs.mounttable.cluster.link./data", "viewfs://inner/base");
        properties.put("fs.viewfs.mounttable.inner.link./base", "hdfs://remote/base");
        Assertions.assertDoesNotThrow(() -> HadoopTvfAdmission.checkReadDiscovery("viewfs://cluster/data/*", properties));
        properties.put("fs.viewfs.mounttable.inner.link./base/private", "file:///tmp");
        Assertions.assertThrows(AnalysisException.class, () -> HadoopTvfAdmission.checkReadDiscovery("viewfs://cluster/data/*", properties));
    }

    @Test
    void emptyNativeWriteNameNodeKeepsExistingAnalysisWithoutSelectingLocal() {
        Map<String, String> properties = new HashMap<>();
        Assertions.assertDoesNotThrow(() -> HadoopTvfAdmission.check(
                Access.NATIVE_WRITE, "remote/data", properties));
        Assertions.assertThrows(AnalysisException.class, () -> HadoopTvfAdmission.check(
                Access.HADOOP_READ, "hdfs://remote/data", properties));
        properties.put("fs.defaultFS", "default");
        Assertions.assertThrows(AnalysisException.class, () -> HadoopTvfAdmission.check(
                Access.NATIVE_WRITE, "remote/data", properties));
        properties.put("fs.defaultFS", "\0ignored");
        Assertions.assertDoesNotThrow(() -> HadoopTvfAdmission.check(
                Access.NATIVE_WRITE, "remote/data", properties));
    }

    @Test
    void builtInRemoteProviderImplementationsRemainAvailable() {
        for (String[] provider : new String[][] {
                {"oss", "com.aliyun.jindodata.oss.JindoOssFileSystem"},
                {"obs", "org.apache.hadoop.fs.obs.OBSFileSystem"}}) {
            Map<String, String> properties = new HashMap<>();
            properties.put("fs.defaultFS", provider[0] + "://bucket");
            properties.put("fs." + provider[0] + ".impl", provider[1]);
            for (Access access : Access.values()) {
                Assertions.assertDoesNotThrow(() -> HadoopTvfAdmission.check(
                        access, provider[0] + "://bucket/data/file.csv", properties));
            }
        }
    }

    @Test
    void nativePathsAreRootedBeforeHadoopUriParsing() {
        Map<String, String> properties = viewFs();
        properties.put("fs.viewfs.mounttable.cluster.link./remote", "hdfs://remote/base");
        properties.put("fs.viewfs.mounttable.cluster.link./hdfs:", "file:///tmp");
        Assertions.assertThrows(AnalysisException.class, () -> HadoopTvfAdmission.check(
                Access.NATIVE_WRITE, "hdfs://unrelated/remote/prefix_", properties));
        Assertions.assertDoesNotThrow(() -> HadoopTvfAdmission.check(
                Access.NATIVE_WRITE, "remote/prefix_", properties));

        properties.put("fs.defaultFS", "viewfs://schema");
        properties.put("fs.viewfs.mounttable.data.link./remote", "hdfs://remote/base");
        properties.put("fs.viewfs.mounttable.schema.link./remote", "hdfs://remote/base");
        properties.put("fs.viewfs.mounttable.schema.link./viewfs:", "file:///tmp");
        Assertions.assertThrows(AnalysisException.class, () -> HadoopTvfAdmission.check(
                Access.HADOOP_READ, "viewfs://data/remote/file.csv", properties));
        properties.put("fs.viewfs.mounttable.schema.link./viewfs:", "hdfs://remote/schema");
        Assertions.assertDoesNotThrow(() -> HadoopTvfAdmission.check(
                Access.HADOOP_READ, "viewfs://data/remote/file.csv", properties));
    }

    @Test
    void nonPrefixNameNodeMatchesRequireAnEstablishedNativePath() {
        String nameNode = "viewfs://clüster";
        Map<String, String> properties = new HashMap<>();
        properties.put("fs.defaultFS", nameNode);
        properties.put("fs.viewfs.mounttable.clüster.link./x", "hdfs://remote/base");
        properties.put("fs.viewfs.mounttable.clüster.link./local", "file:///tmp");
        String path = "abcdefghijklmnopx/local/prefix_" + nameNode + "/a";
        Assertions.assertThrows(AnalysisException.class, () -> HadoopTvfAdmission.check(
                Access.NATIVE_WRITE, path, properties));
        Assertions.assertDoesNotThrow(() -> HadoopTvfAdmission.check(
                Access.NATIVE_WRITE, nameNode + "/x/prefix_", properties));
        Config.enable_local_tvf = true;
        Assertions.assertDoesNotThrow(() -> HadoopTvfAdmission.check(
                Access.NATIVE_WRITE, path, properties));
    }

    @Test
    void relativeJavaViewFsPathsCannotAssumeTheFilesystemRoot() {
        Map<String, String> properties = viewFs();
        properties.put("fs.viewfs.mounttable.cluster.link./data", "hdfs://remote/base");
        properties.put("fs.viewfs.mounttable.cluster.link./user/tvf/data", "file:///tmp");
        Assertions.assertDoesNotThrow(() -> HadoopTvfAdmission.check(
                Access.NATIVE_WRITE, "data/sub/prefix_", properties));
        Assertions.assertThrows(AnalysisException.class, () -> HadoopTvfAdmission.check(
                Access.JAVA_DELETE, "data/sub/", properties));
        properties.put("fs.defaultFS", "hdfs://remote");
        Assertions.assertDoesNotThrow(() -> HadoopTvfAdmission.check(
                Access.JAVA_DELETE, "data/sub/", properties));
        properties.put("fs.defaultFS", "viewfs://cluster");
        Config.enable_local_tvf = true;
        Assertions.assertDoesNotThrow(() -> HadoopTvfAdmission.check(
                Access.JAVA_DELETE, "data/sub/", properties));
    }

    @Test
    void selectedMountTargetsMustNotBeEmpty() {
        Map<String, String> properties = viewFs();
        properties.put("fs.viewfs.mounttable.cluster.linkFallback", "file:///tmp");
        for (String target : new String[] {"", ","}) {
            properties.put("fs.viewfs.mounttable.cluster.link./data", target);
            for (Access access : new Access[] {Access.NATIVE_WRITE, Access.JAVA_DELETE}) {
                Assertions.assertThrows(AnalysisException.class, () -> HadoopTvfAdmission.check(
                        access, "/data/sub/prefix_", properties));
            }
        }
        properties.put("fs.viewfs.mounttable.cluster.link./data", "hdfs://remote/base,");
        Assertions.assertDoesNotThrow(() -> HadoopTvfAdmission.check(
                Access.NATIVE_WRITE, "/data/sub/prefix_", properties));
        properties.remove("fs.viewfs.mounttable.cluster.link./data");
        properties.put("fs.viewfs.mounttable.cluster.linkMerge./data", "hdfs://remote/base,");
        Assertions.assertThrows(AnalysisException.class, () -> HadoopTvfAdmission.check(
                Access.NATIVE_WRITE, "/data/sub/prefix_", properties));
        properties.remove("fs.viewfs.mounttable.cluster.linkMerge./data");
        properties.put("fs.viewfs.mounttable.cluster.link./data", "hdfs://remote/base");
        properties.put("fs.viewfs.mounttable.cluster.link./unused", "");
        Assertions.assertDoesNotThrow(() -> HadoopTvfAdmission.check(
                Access.NATIVE_WRITE, "/data/sub/prefix_", properties));
    }

    @Test
    void nativeWriterRejectsCaseAndNulMapCollisions() {
        Map<String, String> properties = viewFs();
        String upper = "fs.viewfs.mounttable.CLUSTER.link./data\0A";
        String lower = "fs.viewfs.mounttable.cluster.link./data\0b";
        properties.put(upper, "hdfs://remote/base");
        properties.put(lower, "hdfs://remote/base");
        properties.put("fs.viewfs.mounttable.cluster.linkFallback", "file:///tmp");
        Assertions.assertThrows(AnalysisException.class, () -> HadoopTvfAdmission.check(
                Access.NATIVE_WRITE, "/data/sub/prefix_", properties));
        properties.put("fs.viewfs.mounttable.cluster.link./public", "hdfs://remote/public");
        Assertions.assertDoesNotThrow(() -> HadoopTvfAdmission.check(
                Access.HADOOP_READ, "viewfs://cluster/public/file.csv", properties));
        properties.remove(upper);
        properties.put(upper + "long", "hdfs://remote/base");
        Assertions.assertDoesNotThrow(() -> HadoopTvfAdmission.check(
                Access.NATIVE_WRITE, "/data/sub/prefix_", properties));
        properties.remove(upper + "long");
        properties.put(upper, "hdfs://remote/base");
        Config.enable_local_tvf = true;
        Assertions.assertDoesNotThrow(() -> HadoopTvfAdmission.check(
                Access.NATIVE_WRITE, "/data/sub/prefix_", properties));
        Config.enable_local_tvf = false;
        properties.remove(upper);
        properties.remove(lower);
        properties.put("fs.viewfs.mounttable.cluster.link./data", "hdfs://remote/base");
        properties.put("dfs.test.VALUE\0a", "one");
        properties.put("dfs.test.value\0b", "two");
        Assertions.assertDoesNotThrow(() -> HadoopTvfAdmission.check(
                Access.NATIVE_WRITE, "/data/sub/prefix_", properties));
    }

    @Test
    void crossMountGlobsRequireAnExplicitRemoteRoute() {
        Map<String, String> properties = viewFs();
        properties.put("fs.viewfs.mounttable.cluster.link./data", "hdfs://remote/base");
        properties.put("fs.viewfs.mount.links.as.symlinks", "false");
        Assertions.assertThrows(AnalysisException.class, () -> HadoopTvfAdmission.checkReadDiscovery("viewfs://cluster/*/*.csv", properties));
        properties.put("fs.viewfs.mounttable.cluster.linkFallback", "hdfs://remote/fallback");
        for (String pattern : new String[] {"/*/*.csv", "/d*/*.csv", "/{data,other}/*.csv"}) {
            Assertions.assertDoesNotThrow(() -> HadoopTvfAdmission.checkReadDiscovery("viewfs://cluster" + pattern, properties));
        }
        properties.put("fs.viewfs.mounttable.cluster.link./private", "file:///tmp");
        Assertions.assertThrows(AnalysisException.class, () -> HadoopTvfAdmission.checkReadDiscovery("viewfs://cluster/*/*.csv", properties));
        properties.remove("fs.viewfs.mounttable.cluster.link./private");
        properties.put("fs.viewfs.mounttable.cluster.linkFallback", "file:///tmp");
        Assertions.assertThrows(AnalysisException.class, () -> HadoopTvfAdmission.checkReadDiscovery("viewfs://cluster/*/*.csv", properties));
        properties.remove("fs.viewfs.mounttable.cluster.linkFallback");
        properties.put("fs.viewfs.mounttable.cluster.link./data", "");
        Assertions.assertThrows(AnalysisException.class, () -> HadoopTvfAdmission.checkReadDiscovery("viewfs://cluster/*/*.csv", properties));
    }

    @Test
    void defaultFilesystemHostPortShorthandRemainsRemote() {
        for (String key : new String[] {"fs.defaultFS", "fs.default.name"}) {
            Map<String, String> properties = new HashMap<>();
            properties.put(key, "namenode:8020");
            Assertions.assertDoesNotThrow(() -> HadoopTvfAdmission.check(
                    Access.JAVA_DELETE, "/exports/", properties));
            properties.put(key, "local");
            Assertions.assertThrows(AnalysisException.class, () -> HadoopTvfAdmission.check(
                    Access.JAVA_DELETE, "/exports/", properties));
        }
    }

    @Test
    void singleViewFsTargetsKeepCommasWhileMultiTargetsStillRejectLocal() {
        for (String key : new String[] {"link./data", "linkFallback", "linkMergeSlash"}) {
            Map<String, String> properties = viewFs();
            properties.put("fs.viewfs.mounttable.cluster." + key, "hdfs://remote/base,archive");
            for (Access access : Access.values()) {
                String path = access == Access.HADOOP_READ
                        ? "viewfs://cluster/data/file.csv" : "/data/prefix_";
                Assertions.assertDoesNotThrow(() -> HadoopTvfAdmission.check(access, path, properties));
            }
            Assertions.assertDoesNotThrow(() -> HadoopTvfAdmission.checkReadDiscovery("viewfs://cluster/data/*.csv", properties));
        }
        for (String key : new String[] {"linkMerge./data", "linkNfly.minReplication=2./data"}) {
            Map<String, String> properties = viewFs();
            properties.put("fs.viewfs.mounttable.cluster." + key, "hdfs://remote/base,file:///tmp");
            for (Access access : Access.values()) {
                Assertions.assertThrows(AnalysisException.class, () -> HadoopTvfAdmission.check(
                        access, "viewfs://cluster/data/file.csv", properties));
            }
        }
    }

    @Test
    void nativeRoutesRejectSupplementaryCharactersBeforeJniDecoding() {
        String supplementary = new String(Character.toChars(0x1F600)) + "AAA";
        Map<String, String> properties = viewFs();
        properties.put("fs.viewfs.mounttable.cluster.link./data" + supplementary, "hdfs://remote/base");
        properties.put("fs.viewfs.mounttable.cluster.linkFallback", "file:///tmp");
        Assertions.assertThrows(AnalysisException.class, () -> HadoopTvfAdmission.check(
                Access.NATIVE_WRITE, "/data" + supplementary + "/prefix_", properties));
        // Java deletion does not cross JNI and preserves this otherwise valid URI identity.
        Assertions.assertDoesNotThrow(() -> HadoopTvfAdmission.check(
                Access.JAVA_DELETE, "/data" + supplementary + "/", properties));
        Map<String, String> bmp = viewFs();
        bmp.put("fs.viewfs.mounttable.cluster.link./数据", "hdfs://remote/目录");
        Assertions.assertDoesNotThrow(() -> HadoopTvfAdmission.check(
                Access.NATIVE_WRITE, "/数据/prefix_", bmp));
        Config.enable_local_tvf = true;
        Assertions.assertDoesNotThrow(() -> HadoopTvfAdmission.check(
                Access.NATIVE_WRITE, "/data" + supplementary + "/prefix_", properties));
    }

    @Test
    void nativeCharacterChecksCoverRoutingKeysValuesAndReadPaths() {
        String supplementary = new String(Character.toChars(0x1F600)) + "AAA";
        Map<String, String> properties = viewFs();
        properties.put("fs.viewfs.mounttable.cluster.link./data", "hdfs://remote/base");
        for (String value : new String[] {"hdfs://remote/base" + supplementary, "hdfs://remote/base" + (char) 0xD800}) {
            properties.put("fs.viewfs.mounttable.cluster.link./data", value);
            Assertions.assertThrows(AnalysisException.class, () -> HadoopTvfAdmission.check(
                    Access.NATIVE_WRITE, "/data/prefix_", properties));
            Assertions.assertThrows(AnalysisException.class, () -> HadoopTvfAdmission.check(
                    Access.HADOOP_READ, "viewfs://cluster/data/file.csv", properties));
        }
        properties.put("fs.viewfs.mounttable.cluster.link./data", "hdfs://remote/base");
        Assertions.assertThrows(AnalysisException.class, () -> HadoopTvfAdmission.check(
                Access.HADOOP_READ, "viewfs://cluster/data/" + supplementary + ".csv", properties));
        properties.put("fs.viewfs.mounttable.cluster.link./unused" + supplementary, "hdfs://remote/unused");
        Assertions.assertThrows(AnalysisException.class, () -> HadoopTvfAdmission.check(
                Access.NATIVE_WRITE, "/data/prefix_", properties));
        properties.remove("fs.viewfs.mounttable.cluster.link./unused" + supplementary);
        properties.put("fs.viewfs.mounttable.cluster.link./data", "hdfs://remote/base\0" + supplementary);
        Assertions.assertDoesNotThrow(() -> HadoopTvfAdmission.check(
                Access.NATIVE_WRITE, "/data/prefix_", properties));
    }

    private static Map<String, String> viewFs() {
        Map<String, String> properties = new HashMap<>();
        properties.put("fs.defaultFS", "viewfs://cluster");
        return properties;
    }
}
