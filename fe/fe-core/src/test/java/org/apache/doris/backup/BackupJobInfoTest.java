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

package org.apache.doris.backup;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class BackupJobInfoTest {

    private static String fileName = "job_info.txt";

    @BeforeClass
    public static void createFile() {
        String json = "{\n"
                + "    \"backup_time\": 1522231864000,\n"
                + "    \"name\": \"snapshot1\",\n"
                + "    \"database\": \"db1\",\n"
                + "    \"id\": 10000,\n"
                + "    \"backup_result\": \"succeed\",\n"
                + "    \"backup_objects\": {\n"
                + "        \"table2\": {\n"
                + "            \"partitions\": {\n"
                + "                \"partition1\": {\n"
                + "                    \"indexes\": {\n"
                + "                        \"table2\": {\n"
                + "                            \"id\": 10012,\n"
                + "                            \"schema_hash\": 222222,\n"
                + "                            \"tablets\": {\n"
                + "                                \"10004\": [\"__10030_seg1.dat\", \"__10030_seg2.dat\"],\n"
                + "                                \"10005\": [\"__10031_seg1.dat\", \"__10031_seg2.dat\"]\n"
                + "                            }\n"
                + "                        }\n"
                + "                    },\n"
                + "                    \"id\": 10011,\n"
                + "                    \"version\": 11,\n"
                + "                    \"version_hash\": 123456789\n"
                + "                }\n"
                + "            },\n"
                + "            \"id\": 10010\n"
                + "        },\n"
                + "        \"table1\": {\n"
                + "            \"partitions\": {\n"
                + "                \"partition2\": {\n"
                + "                    \"indexes\": {\n"
                + "                        \"rollup1\": {\n"
                + "                            \"id\": 10009,\n"
                + "                            \"schema_hash\": 333333,\n"
                + "                            \"tablets\": {\n"
                + "                                \"10008\": [\"__10029_seg1.dat\", \"__10029_seg2.dat\"],\n"
                + "                                \"10007\": [\"__10029_seg1.dat\", \"__10029_seg2.dat\"]\n"
                + "                            }\n"
                + "                        },\n"
                + "                        \"table1\": {\n"
                + "                            \"id\": 10001,\n"
                + "                            \"schema_hash\": 444444,\n"
                + "                            \"tablets\": {\n"
                + "                                \"10004\": [\"__10027_seg1.dat\", \"__10027_seg2.dat\"],\n"
                + "                                \"10005\": [\"__10028_seg1.dat\", \"__10028_seg2.dat\"]\n"
                + "                            }\n"
                + "                        }\n"
                + "                    },\n"
                + "                    \"id\": 10007,\n"
                + "                    \"version\": 20,\n"
                + "                    \"version_hash\": 123534645745\n"
                + "                },\n"
                + "                \"partition1\": {\n"
                + "                    \"indexes\": {\n"
                + "                        \"rollup1\": {\n"
                + "                            \"id\": 10009,\n"
                + "                            \"schema_hash\": 333333,\n"
                + "                            \"tablets\": {\n"
                + "                                \"10008\": [\"__10026_seg1.dat\", \"__10026_seg2.dat\"],\n"
                + "                                \"10007\": [\"__10025_seg1.dat\", \"__10025_seg2.dat\"]\n"
                + "                            }\n"
                + "                        },\n"
                + "                        \"table1\": {\n"
                + "                            \"id\": 10001,\n"
                + "                            \"schema_hash\": 444444,\n"
                + "                            \"tablets\": {\n"
                + "                                \"10004\": [\"__10023_seg1.dat\", \"__10023_seg2.dat\"],\n"
                + "                                \"10005\": [\"__10024_seg1.dat\", \"__10024_seg2.dat\"]\n"
                + "                            }\n"
                + "                        }\n"
                + "                    },\n"
                + "                    \"id\": 10002,\n"
                + "                    \"version\": 21,\n"
                + "                    \"version_hash\": 345346234234\n"
                + "                }\n"
                + "            },\n"
                + "            \"id\": 10001\n"
                + "        }\n"
                + "    },\n"
                + "    \"new_backup_objects\": {\n"
                + "        \"views\":[{\n"
                + "            \"name\": \"view1\",\n"
                + "            \"id\": \"10006\"\n"
                + "        }]\n"
                + "    }\n"
                + "}";

        try (PrintWriter out = new PrintWriter(fileName)) {
            out.print(json);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @AfterClass
    public static void deleteFile() {
        File file = new File(fileName);
        if (file.exists()) {
            file.delete();
        }
    }

    @Test
    public void testReadWrite() {
        BackupJobInfo jobInfo = null;
        try {
            jobInfo = BackupJobInfo.fromFile(fileName);
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail();
        }
        Assert.assertNotNull(jobInfo);
        System.out.println(jobInfo.toString());

        Assert.assertEquals(1522231864000L, jobInfo.backupTime);
        Assert.assertEquals("snapshot1", jobInfo.name);
        Assert.assertEquals(2, jobInfo.backupOlapTableObjects.size());

        Assert.assertEquals(2, jobInfo.getOlapTableInfo("table1").partitions.size());
        Assert.assertEquals(2, jobInfo.getOlapTableInfo("table1").getPartInfo("partition1").indexes.size());
        Assert.assertEquals(2,
                            jobInfo.getOlapTableInfo("table1").getPartInfo("partition1").getIdx("rollup1").tablets.size());
        System.out.println(jobInfo.getOlapTableInfo("table1").getPartInfo("partition1").getIdx("rollup1").tablets);
        Assert.assertEquals(2,
                            jobInfo.getOlapTableInfo("table1").getPartInfo("partition1")
                            .getIdx("rollup1").getTabletFiles(10007L).size());

        Assert.assertEquals(1, jobInfo.newBackupObjects.views.size());
        Assert.assertEquals("view1", jobInfo.newBackupObjects.views.get(0).name);

        File tmpFile = new File("./tmp");
        File tmpFile1 = new File("./tmp1");
        try {
            DataOutputStream out = new DataOutputStream(new FileOutputStream(tmpFile));
            jobInfo.write(out);
            out.flush();
            out.close();

            DataInputStream in = new DataInputStream(new FileInputStream(tmpFile));
            BackupJobInfo newInfo = BackupJobInfo.read(in);
            in.close();

            Assert.assertEquals(jobInfo.backupTime, newInfo.backupTime);
            Assert.assertEquals(jobInfo.dbId, newInfo.dbId);
            Assert.assertEquals(jobInfo.dbName, newInfo.dbName);

            Assert.assertEquals(jobInfo.newBackupObjects.views.size(), newInfo.newBackupObjects.views.size());
            Assert.assertEquals("view1", newInfo.newBackupObjects.views.get(0).name);

            out = new DataOutputStream(new FileOutputStream(tmpFile1));
            newInfo.write(out);
            out.flush();
            out.close();

            in = new DataInputStream(new FileInputStream(tmpFile1));
            BackupJobInfo newInfo1 = BackupJobInfo.read(in);
            in.close();

            Assert.assertEquals(
                    newInfo.backupOlapTableObjects.get("table2").getPartInfo("partition1")
                            .indexes.get("table2").sortedTabletInfoList.size(),
                    newInfo1.backupOlapTableObjects.get("table2").getPartInfo("partition1")
                            .indexes.get("table2").sortedTabletInfoList.size());

        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail();
        } finally {
            tmpFile.delete();
            tmpFile1.delete();
        }

    }
}
