package org.apache.doris.persist;

import org.apache.doris.common.AnalysisException;

import org.junit.Assert;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class ConsistencyCheckInfoTest {
    @Test
    public void testSerialization() throws IOException, AnalysisException {
        // 1. Write objects to file
        final Path path = Files.createTempFile("consistencyCheckInfo", "tmp");
        DataOutputStream out = new DataOutputStream(Files.newOutputStream(path));

        ConsistencyCheckInfo consistencyCheckInfo1 = new ConsistencyCheckInfo(1L, 2L, 3L, 4L, 5L, 6L, 7L, true);

        consistencyCheckInfo1.write(out);
        out.flush();
        out.close();

        // 2. Read objects from file
        DataInputStream in = new DataInputStream(Files.newInputStream(path));

        ConsistencyCheckInfo consistencyCheckInfo2 = ConsistencyCheckInfo.read(in);

        Assert.assertEquals(consistencyCheckInfo1.getDbId(), consistencyCheckInfo2.getDbId());

        // 3. delete files
        in.close();
        Files.delete(path);
    }
}
