package org.apache.doris.common.io;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

public class CountingDataOutputStreamTest {
    private static final Logger LOG = LogManager.getLogger(CountingDataOutputStreamTest.class);

    @Test
    public void testWrite() {
        File file = new File("/Users/hantongyang/data/meta_test.txt");
        try (CountingDataOutputStream out = new CountingDataOutputStream(Files.newOutputStream(file.toPath()))) {
            byte[] data = String.valueOf('a').getBytes(StandardCharsets.UTF_8);
            out.write(data, 0, data.length);
        } catch (Exception e) {
            LOG.error(e);
        }
    }
}
