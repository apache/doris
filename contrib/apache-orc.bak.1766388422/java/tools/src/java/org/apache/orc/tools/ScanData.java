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
package org.apache.orc.tools;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.StripeInformation;
import org.apache.orc.TypeDescription;

import java.util.ArrayList;
import java.util.List;

/**
 * Scan the contents of an ORC file.
 */
public class ScanData {

  private static final Options OPTIONS = new Options()
      .addOption("v", "verbose", false, "Print exceptions")
      .addOption("s", "schema", false, "Print schema")
      .addOption("h", "help", false, "Provide help");

  static CommandLine parseCommandLine(String[] args) throws ParseException {
    return new DefaultParser().parse(OPTIONS, args);
  }

  static int calculateBestVectorSize(int indexStride) {
    if (indexStride == 0) {
      return 1024;
    }
    // how many 1024 batches do we have in an index stride?
    int batchCount = (indexStride + 1023) / 1024;
    return indexStride / batchCount;
  }

  static class LocationInfo {
    final long firstRow;
    final long followingRow;
    final int stripeId;
    final long row;

    LocationInfo(long firstRow, long followingRow, int stripeId,
        long row) {
      this.firstRow = firstRow;
      this.followingRow = followingRow;
      this.stripeId = stripeId;
      this.row = row;
    }

    public String toString() {
      return String.format("row %d in stripe %d (rows %d-%d)",
          row, stripeId, firstRow, followingRow);
    }
  }

  /**
   * Given a row, find the stripe that contains that row.
   * @param reader the file reader
   * @param row the global row number in the file
   * @return the information about that row in the file
   */
  static LocationInfo findStripeInfo(Reader reader, long row) {
    long firstRow = 0;
    int stripeId = 0;
    for (StripeInformation stripe: reader.getStripes()) {
      long lastRow = firstRow + stripe.getNumberOfRows();
      if (firstRow <= row && row < lastRow) {
        return new LocationInfo(firstRow, lastRow, stripeId, row);
      }
      firstRow = lastRow;
      stripeId += 1;
    }
    return new LocationInfo(reader.getNumberOfRows(),
        reader.getNumberOfRows(), reader.getStripes().size(), row);
  }

  /**
   * Given a failure point, find the first place that the ORC reader can
   * recover.
   * @param reader the ORC reader
   * @param current the position of the failure
   * @param batchSize the size of the batch that we tried to read
   * @return the location that we should recover to
   */
  static LocationInfo findRecoveryPoint(Reader reader, LocationInfo current,
                                        int batchSize) {
    int stride = reader.getRowIndexStride();
    long result;
    // In the worst case, just move to the next stripe
    if (stride == 0 ||
        current.row + batchSize >= current.followingRow) {
      result = current.followingRow;
    } else {
      long rowInStripe = current.row + batchSize - current.firstRow;
      result = Math.min(current.followingRow,
          current.firstRow + (rowInStripe + stride - 1) / stride * stride);
    }
    return findStripeInfo(reader, result);
  }

  static boolean findBadColumns(Reader reader, LocationInfo current, int batchSize,
      TypeDescription column, boolean[] include) {
    include[column.getId()] = true;
    TypeDescription schema = reader.getSchema();
    boolean result = false;
    if (column.getChildren() == null) {
      int row = 0;
      try (RecordReader rows = reader.rows(reader.options().include(include))) {
        rows.seekToRow(current.row);
        VectorizedRowBatch batch = schema.createRowBatch(
            TypeDescription.RowBatchVersion.USE_DECIMAL64, 1);
        for(row=0; row < batchSize; ++row) {
          rows.nextBatch(batch);
        }
      } catch (Throwable t) {
        System.out.printf("Column %d failed at row %d%n", column.getId(),
            current.row + row);
        result = true;
      }
    } else {
      for(TypeDescription child: column.getChildren()) {
        result |= findBadColumns(reader, current, batchSize, child, include);
      }
    }
    include[column.getId()] = false;
    return result;
  }

  static void main(Configuration conf, String[] args) throws ParseException {
    CommandLine cli = parseCommandLine(args);
    if (cli.hasOption('h') || cli.getArgs().length == 0) {
      new HelpFormatter().printHelp("java -jar orc-tools-*.jar scan",
          OPTIONS);
      System.exit(1);
    } else {
      final boolean printSchema = cli.hasOption('s');
      final boolean printExceptions = cli.hasOption('v');
      List<String> badFiles = new ArrayList<>();
      for (String file : cli.getArgs()) {
        try (Reader reader = FileDump.getReader(new Path(file), conf, badFiles)) {
          if (reader != null) {
            TypeDescription schema = reader.getSchema();
            if (printSchema) {
              System.out.println(schema.toJson());
            }
            VectorizedRowBatch batch = schema.createRowBatch(
                TypeDescription.RowBatchVersion.USE_DECIMAL64,
                calculateBestVectorSize(reader.getRowIndexStride()));
            final int batchSize = batch.getMaxSize();
            long badBatches = 0;
            long currentRow = 0;
            long goodRows = 0;
            try (RecordReader rows = reader.rows()) {
              while (currentRow < reader.getNumberOfRows()) {
                currentRow = rows.getRowNumber();
                try {
                  if (!rows.nextBatch(batch)) {
                    break;
                  }
                  goodRows += batch.size;
                } catch (Exception e) {
                  badBatches += 1;
                  LocationInfo current = findStripeInfo(reader, currentRow);
                  LocationInfo recover = findRecoveryPoint(reader, current, batchSize);
                  System.out.println("Unable to read batch at " + current +
                      ", recovery at " + recover);
                  if (printExceptions) {
                    e.printStackTrace();
                  }
                  findBadColumns(reader, current, batchSize, reader.getSchema(),
                      new boolean[reader.getSchema().getMaximumId() + 1]);
                  // If we are at the end of the file, get out
                  if (recover.row >= reader.getNumberOfRows()) {
                    break;
                  } else {
                    rows.seekToRow(recover.row);
                  }
                }
              }
            }
            if (badBatches != 0) {
              badFiles.add(file);
            }
            System.out.printf("File: %s, bad batches: %d, rows: %d/%d%n", file,
                badBatches, goodRows, reader.getNumberOfRows());
          }
        } catch (Exception e) {
          badFiles.add(file);
          System.err.println("Unable to open file: " + file);
          if (printExceptions) {
            e.printStackTrace();
          }
        }
      }
      System.exit(badFiles.size());
    }
  }
}
