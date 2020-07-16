package org.apache.doris.persist;

import org.apache.doris.catalog.DataProperty;
import org.apache.doris.common.AnalysisException;
import com.google.common.collect.Lists;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;

public class BatchModifyPartitionsInfoTest {
    private static String fileName = "./BatchModifyPartitionsInfoTest";

    private final long DB_ID = 10000L;
    private final long TB_ID = 30000L;
    private final long PARTITION_ID_1 = 40000L;
    private final long PARTITION_ID_2 = 40001L;
    private final long PARTITION_ID_3 = 40002L;

    @After
    public void tearDown() {
        File file = new File(fileName);
        file.delete();
    }

    @Test
    public void testSerialzeBatchModifyPartitionsInfo() throws IOException, AnalysisException {
        List<ModifyPartitionInfo> ModifyInfos = Lists.newArrayList();
        // 1. Write objects to file
        File file = new File(fileName);
        file.createNewFile();
        DataOutputStream out = new DataOutputStream(new FileOutputStream(file));

        List<Long> partitionIds = Lists.newArrayList(PARTITION_ID_1, PARTITION_ID_2, PARTITION_ID_3);
        for (long partitionId : partitionIds) {
            ModifyInfos.add(new ModifyPartitionInfo(DB_ID, TB_ID, partitionId,
                    DataProperty.DEFAULT_DATA_PROPERTY, (short) 3, true));
        }

        BatchModifyPartitionsInfo batchModifyPartitionsInfo = new BatchModifyPartitionsInfo(ModifyInfos);
        batchModifyPartitionsInfo.write(out);
        out.flush();
        out.close();

        // 2. Read objects from file
        DataInputStream in = new DataInputStream(new FileInputStream(file));

        BatchModifyPartitionsInfo readBatchModifyPartitionsInfo = BatchModifyPartitionsInfo.read(in);
        Assert.assertEquals(batchModifyPartitionsInfo, readBatchModifyPartitionsInfo);

        in.close();
    }
}
