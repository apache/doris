/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.orc.mapred;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentImpl;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.orc.OrcConf;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.TypeDescription;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

/**
 * A MapReduce/Hive input format for ORC files.
 */
public class OrcInputFormat<V extends WritableComparable>
    extends FileInputFormat<NullWritable, V> {

  private static final int KRYO_SARG_MAX_BUFFER = 16777216;

  /**
   * Convert a string with a comma separated list of column ids into the
   * array of boolean that match the schemas.
   * @param schema the schema for the reader
   * @param columnsStr the comma separated list of column ids
   * @return a boolean array
   */
  public static boolean[] parseInclude(TypeDescription schema,
                                       String columnsStr) {
    if (columnsStr == null ||
        schema.getCategory() != TypeDescription.Category.STRUCT) {
      return null;
    }

    boolean[] result = new boolean[schema.getMaximumId() + 1];
    result[0] = true;
    if (StringUtils.isBlank(columnsStr)) {
      return result;
    }

    List<TypeDescription> types = schema.getChildren();
    for(String idString: columnsStr.split(",")) {
      TypeDescription type = types.get(Integer.parseInt(idString));
      for(int c=type.getId(); c <= type.getMaximumId(); ++c) {
        result[c] = true;
      }
    }
    return result;
  }

  /**
   * Put the given SearchArgument into the configuration for an OrcInputFormat.
   * @param conf the configuration to modify
   * @param sarg the SearchArgument to put in the configuration
   * @param columnNames the list of column names for the SearchArgument
   */
  public static void setSearchArgument(Configuration conf,
                                       SearchArgument sarg,
                                       String[] columnNames) {
    int bufferSize = (int)OrcConf.KRYO_SARG_BUFFER.getLong(conf);
    Output out = new Output(bufferSize, KRYO_SARG_MAX_BUFFER);
    new Kryo().writeObject(out, sarg);
    OrcConf.KRYO_SARG.setString(conf, Base64.getMimeEncoder().encodeToString(out.toBytes()));
    StringBuilder buffer = new StringBuilder();
    for (int i = 0; i < columnNames.length; ++i) {
      if (i != 0) {
        buffer.append(',');
      }
      buffer.append(columnNames[i]);
    }
    OrcConf.SARG_COLUMNS.setString(conf, buffer.toString());
  }

  /**
   * Build the Reader.Options object based on the JobConf and the range of
   * bytes.
   * @param conf the job configuratoin
   * @param reader the file footer reader
   * @param start the byte offset to start reader
   * @param length the number of bytes to read
   * @return the options to read with
   */
  public static Reader.Options buildOptions(Configuration conf,
                                            Reader reader,
                                            long start,
                                            long length) {
    TypeDescription schema =
        TypeDescription.fromString(OrcConf.MAPRED_INPUT_SCHEMA.getString(conf));
    Reader.Options options = reader.options()
        .range(start, length)
        .useZeroCopy(OrcConf.USE_ZEROCOPY.getBoolean(conf))
        .skipCorruptRecords(OrcConf.SKIP_CORRUPT_DATA.getBoolean(conf))
        .tolerateMissingSchema(OrcConf.TOLERATE_MISSING_SCHEMA.getBoolean(conf));
    if (schema != null) {
      options.schema(schema);
    } else {
      schema = reader.getSchema();
    }
    options.include(parseInclude(schema,
        OrcConf.INCLUDE_COLUMNS.getString(conf)));
    String kryoSarg = OrcConf.KRYO_SARG.getString(conf);
    String sargColumns = OrcConf.SARG_COLUMNS.getString(conf);
    if (kryoSarg != null && sargColumns != null) {
      byte[] sargBytes = Base64.getMimeDecoder().decode(kryoSarg);
      SearchArgument sarg =
          new Kryo().readObject(new Input(sargBytes), SearchArgumentImpl.class);
      options.searchArgument(sarg, sargColumns.split(","));
    }
    return options;
  }

  @Override
  public RecordReader<NullWritable, V> getRecordReader(InputSplit inputSplit,
                                                       JobConf conf,
                                                       Reporter reporter) throws IOException {
    FileSplit split = (FileSplit) inputSplit;
    Reader file = OrcFile.createReader(split.getPath(),
        OrcFile.readerOptions(conf)
            .maxLength(OrcConf.MAX_FILE_LENGTH.getLong(conf)));
    //Mapreduce supports selected vector
    Reader.Options options =  buildOptions(conf, file, split.getStart(), split.getLength())
        .useSelected(true);
    return new OrcMapredRecordReader<>(file, options);
  }

  /**
   * Filter out the 0 byte files, so that we don't generate splits for the
   * empty ORC files.
   * @param job the job configuration
   * @return a list of files that need to be read
   * @throws IOException
   */
  @Override
  protected FileStatus[] listStatus(JobConf job) throws IOException {
    FileStatus[] result = super.listStatus(job);
    List<FileStatus> ok = new ArrayList<>(result.length);
    for(FileStatus stat: result) {
      if (stat.getLen() != 0) {
        ok.add(stat);
      }
    }
    if (ok.size() == result.length) {
      return result;
    } else {
      return ok.toArray(new FileStatus[0]);
    }
  }
}
