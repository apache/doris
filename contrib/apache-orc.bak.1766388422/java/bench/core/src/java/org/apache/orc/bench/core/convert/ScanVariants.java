/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.orc.bench.core.convert;

import com.google.auto.service.AutoService;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.TypeDescription;
import org.apache.orc.bench.core.CompressionKind;
import org.apache.orc.bench.core.OrcBenchmark;
import org.apache.orc.bench.core.Utilities;

/**
 * A tool to create the different variants that we need to benchmark against.
 */
@AutoService(OrcBenchmark.class)
public class ScanVariants implements OrcBenchmark {


  static CommandLine parseCommandLine(String[] args) throws ParseException {
    Options options = new Options()
        .addOption("h", "help", false, "Provide help")
        .addOption("c", "compress", true, "List of compression")
        .addOption("d", "data", true, "List of data sets")
        .addOption("f", "format", true, "List of formats");
    CommandLine result = new DefaultParser().parse(options, args);
    if (result.hasOption("help") || result.getArgs().length == 0) {
      new HelpFormatter().printHelp("scan <root>", options);
      System.exit(1);
    }
    return result;
  }

  @Override
  public String getName() {
    return "scan";
  }

  @Override
  public String getDescription() {
    return "scan all of the data variants";
  }

  @Override
  public void run(String[] args) throws Exception {
    CommandLine cli = parseCommandLine(args);
    String[] compressList =
        cli.getOptionValue("compress", "snappy,gz,zstd").split(",");
    String[] dataList =
        cli.getOptionValue("data", "taxi,sales,github").split(",");
    String[] formatList =
        cli.getOptionValue("format", "avro,json,orc,parquet").split(",");
    Configuration conf = new Configuration();
    Path root = new Path(cli.getArgs()[0]);
    for(String data: dataList) {
      TypeDescription schema = Utilities.loadSchema(data + ".schema");
      VectorizedRowBatch batch = schema.createRowBatch();
      for (String compress : compressList) {
        CompressionKind compressKind = CompressionKind.fromExtension(compress);
        for (String format : formatList) {
          if (compressKind == CompressionKind.ZSTD && format.equals("json")) {
            continue; // JSON doesn't support ZSTD
          }
          Path filename = Utilities.getVariant(root, data, format,
              compress);
          BatchReader reader = GenerateVariants.createFileReader(filename,
              format, schema, conf, compressKind);
          long rows = 0;
          long batches = 0;
          while (reader.nextBatch(batch)) {
            batches += 1;
            rows += batch.size;
          }
          System.out.println(filename + " rows: " + rows + " batches: "
              + batches);
          reader.close();
        }
      }
    }
  }
}
