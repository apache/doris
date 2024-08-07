package org.apache.doris.persist.meta;

import org.apache.doris.catalog.Env;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

public class MetaWriterTest {

    @Test
    public void testWrite() throws IOException {
        File file = new File("");
        MetaWriter.write(file, Env.getCurrentEnv());
    }
}
