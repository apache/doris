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
package org.apache.orc.bench.hive.rowfilter;

import org.apache.commons.lang.reflect.FieldUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcFilterContext;
import org.apache.orc.Reader;
import org.apache.orc.TypeDescription;
import org.apache.orc.bench.core.Utilities;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.io.IOException;
import java.util.Random;

@State(Scope.Thread)
public abstract class RowFilterInputState {

  private static final Path root = new Path(System.getProperty("user.dir"));

  Configuration conf = new Configuration();
  FileSystem fs;
  TypeDescription schema;
  VectorizedRowBatch batch;
  Path path;
  boolean[] include;
  Reader reader;
  Reader.Options readerOptions;
  boolean[] filterValues = null;

  @Setup
  public void setup() throws IOException, IllegalAccessException {
    TypeDescription.RowBatchVersion version =
        (TypeDescription.RowBatchVersion) FieldUtils.readField(this, "version", true);
    TypeDescription.Category benchType =
        (TypeDescription.Category) FieldUtils.readField(this, "benchType", true);
    String filterPerc = (String) FieldUtils.readField(this, "filterPerc", true);
    int filterColsNum = (int) FieldUtils.readField(this, "filterColsNum", true);
    String dataRelativePath = (String) FieldUtils.readField(this, "dataRelativePath", true);
    String schemaName = (String) FieldUtils.readField(this, "schemaName", true);
    String filterColumn = (String) FieldUtils.readField(this, "filterColumn", true);

    fs = FileSystem.getLocal(conf).getRaw();
    path = new Path(root, dataRelativePath);
    schema = Utilities.loadSchema(schemaName);
    batch = schema.createRowBatch(version, 1024);
    include = new boolean[schema.getMaximumId() + 1];
    for (TypeDescription child : schema.getChildren()) {
      if (schema.getFieldNames().get(child.getId() - 1).compareTo(filterColumn) == 0) {
        System.out.println(
            "Apply Filter on column: " + schema.getFieldNames().get(child.getId() - 1));
        include[child.getId()] = true;
      } else if (child.getCategory() == benchType) {
        System.out.println("Skip column(s): " + schema.getFieldNames().get(child.getId() - 1));
        include[child.getId()] = true;
        if (--filterColsNum == 0) break;
      }
    }
    if (filterColsNum != 0) {
      System.err.println("Dataset does not contain type: " + benchType);
      System.exit(-1);
    }
    generateRandomSet(Double.parseDouble(filterPerc));
    reader = OrcFile.createReader(path,
        OrcFile.readerOptions(conf).filesystem(fs));
    // just read the Boolean columns
    readerOptions = reader.options().include(include);
  }

  public void generateRandomSet(double percentage) throws IllegalArgumentException {
    if (percentage > 1.0) {
      throw new IllegalArgumentException("Filter percentage must be < 1.0 but was " + percentage);
    }
    filterValues = new boolean[1024];
    int count = 0;
    while (count < (1024 * percentage)) {
      Random randomGenerator = new Random();
      int randVal = randomGenerator.nextInt(1024);
      if (!filterValues[randVal]) {
        filterValues[randVal] = true;
        count++;
      }
    }
  }

  public void customIntRowFilter(OrcFilterContext batch) {
    int newSize = 0;
    for (int row = 0; row < batch.getSelectedSize(); ++row) {
      if (filterValues[row]) {
        batch.getSelected()[newSize++] = row;
      }
    }
    batch.setSelectedInUse(true);
    batch.setSelectedSize(newSize);
  }

}
