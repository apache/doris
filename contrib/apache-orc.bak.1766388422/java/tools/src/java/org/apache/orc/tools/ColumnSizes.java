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

package org.apache.orc.tools;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.orc.ColumnStatistics;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.StripeInformation;
import org.apache.orc.TypeDescription;

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Given a set of paths, finds all of the "*.orc" files under them and
 * prints the sizes of each column, both as a percentage and the number of
 * bytes per a row.
 */
public class ColumnSizes {
  final Configuration conf;
  final TypeDescription schema;
  final long[] columnSizes;
  int goodFiles = 0;
  long rows = 0;
  long padding = 0;
  long totalSize = 0;
  long stripeFooterSize = 0;
  long fileFooterSize = 0;
  long stripeIndex = 0;
  // data bytes that aren't assigned to a specific column
  long stripeData = 0;

  public ColumnSizes(Configuration conf,
                     LocatedFileStatus file) throws IOException {
    this.conf = conf;
    try (Reader reader = OrcFile.createReader(file.getPath(),
                                              OrcFile.readerOptions(conf))) {
      this.schema = reader.getSchema();
      columnSizes = new long[schema.getMaximumId() + 1];
      addReader(file, reader);
    }
  }

  private void checkStripes(LocatedFileStatus file,
                            Reader reader) {
    // Count the magic as file overhead
    long offset = OrcFile.MAGIC.length();
    fileFooterSize += offset;

    for (StripeInformation stripe: reader.getStripes()) {
      padding += stripe.getOffset() - offset;
      stripeIndex += stripe.getIndexLength();
      stripeData += stripe.getDataLength();
      stripeFooterSize += stripe.getFooterLength();
      offset = stripe.getOffset() + stripe.getLength();
    }
    // Add everything else as the file footer
    fileFooterSize += file.getLen() - offset;
  }

  private boolean addReader(LocatedFileStatus file,
                            Reader reader) {
    // Validate that the schemas are the same
    TypeDescription newSchema = reader.getSchema();
    if (schema.equals(newSchema)) {
      goodFiles += 1;
      rows += reader.getNumberOfRows();
      totalSize += file.getLen();
      checkStripes(file, reader);
      ColumnStatistics[] colStats = reader.getStatistics();
      for (int c = 0; c < colStats.length && c < columnSizes.length; c++) {
        columnSizes[c] += colStats[c].getBytesOnDisk();
        // Don't double count. Either count the bytes as stripe data or as
        // part of a column.
        stripeData -= colStats[c].getBytesOnDisk();
      }
    } else {
      System.err.println("Ignoring " + file.getPath()
          + " because of schema mismatch: " + newSchema);
      return false;
    }
    return true;
  }

  public boolean addFile(LocatedFileStatus file) throws IOException {
    try (Reader reader = OrcFile.createReader(file.getPath(),
        OrcFile.readerOptions(conf))) {
      return addReader(file, reader);
    }
  }

  private static class StringLongPair {
    final String name;
    final long size;
    StringLongPair(String name, long size) {
      this.name = name;
      this.size = size;
    }
  }

  private void printResults(PrintStream out) {
    List<StringLongPair> sizes = new ArrayList<>(columnSizes.length + 5);
    for(int column = 0; column < columnSizes.length; ++column) {
      if (columnSizes[column] > 0) {
        sizes.add(new StringLongPair(
            schema.findSubtype(column).getFullFieldName(),
            columnSizes[column]));
      }
    }
    if (padding > 0) {
      sizes.add(new StringLongPair("_padding", padding));
    }
    if (stripeFooterSize > 0) {
      sizes.add(new StringLongPair("_stripe_footer", stripeFooterSize));
    }
    if (fileFooterSize > 0) {
      sizes.add(new StringLongPair("_file_footer", fileFooterSize));
    }
    if (stripeIndex > 0) {
      sizes.add(new StringLongPair("_index", stripeIndex));
    }
    if (stripeData > 0) {
      sizes.add(new StringLongPair("_data", stripeData));
    }
    // sort by descending size, ascending name
    sizes.sort((x, y) -> x.size != y.size ?
        Long.compare(y.size, x.size) : x.name.compareTo(y.name));
    out.println("Percent  Bytes/Row  Name");
    for (StringLongPair item: sizes) {
      out.println(String.format("  %-5.2f  %-9.2f  %s",
          100.0 * item.size / totalSize, (double) item.size / rows, item.name));
    }
  }

  public static void main(Configuration conf, String[] args) throws IOException {
    ColumnSizes result = null;
    int badFiles = 0;
    for(String root: args) {
      Path rootPath = new Path(root);
      FileSystem fs = rootPath.getFileSystem(conf);
      for(RemoteIterator<LocatedFileStatus> itr = fs.listFiles(rootPath, true); itr.hasNext(); ) {
        LocatedFileStatus status = itr.next();
        if (status.isFile() && status.getPath().getName().endsWith(".orc")) {
          try {
            if (result == null) {
              result = new ColumnSizes(conf, status);
            } else {
              if (!result.addFile(status)) {
                badFiles += 1;
              }
            }
          } catch (IOException err) {
            badFiles += 1;
            System.err.println("Failed to read " + status.getPath());
          }
        }
      }
    }
    if (result == null) {
      System.err.println("No files found");
    } else {
      result.printResults(System.out);
    }
    if (badFiles > 0) {
      System.err.println(badFiles + " bad ORC files found.");
      System.exit(1);
    }
  }

  public static void main(String[] args) throws IOException {
    main(new Configuration(), args);
  }
}
