package org.apache.doris.hive;

import org.apache.doris.common.io.Writable;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;

public abstract class HiveReader {

    private static final Logger LOG = LogManager.getLogger(HiveReader.class);
    protected RecordReader<Writable, Writable> reader;
    protected Path path;
    protected FileSystem fileSystem;

    public abstract void open() throws IOException;

}
