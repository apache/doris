/**
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

#include "ToolsHelper.hh"

#include <iostream>

void scanFile(std::ostream& out, const char* filename, uint64_t batchSize,
              const orc::RowReaderOptions& rowReaderOpts, bool showMetrics) {
  orc::ReaderOptions readerOpts;
  if (showMetrics) {
    readerOpts.setReaderMetrics(orc::getDefaultReaderMetrics());
  }
  std::unique_ptr<orc::Reader> reader =
      orc::createReader(orc::readFile(filename, readerOpts.getReaderMetrics()), readerOpts);
  std::unique_ptr<orc::RowReader> rowReader = reader->createRowReader(rowReaderOpts);
  std::unique_ptr<orc::ColumnVectorBatch> batch = rowReader->createRowBatch(batchSize);

  unsigned long rows = 0;
  unsigned long batches = 0;
  while (rowReader->next(*batch)) {
    batches += 1;
    rows += batch->numElements;
  }
  out << "Rows: " << rows << std::endl;
  out << "Batches: " << batches << std::endl;
  if (showMetrics) {
    printReaderMetrics(out, reader->getReaderMetrics());
  }
}

int main(int argc, char* argv[]) {
  uint64_t batchSize = 1024;
  bool showMetrics = false;
  orc::RowReaderOptions rowReaderOptions;
  bool success = parseOptions(&argc, &argv, &batchSize, &rowReaderOptions, &showMetrics);
  if (argc < 1 || !success) {
    std::cerr << "Usage: orc-scan [options] <filename>...\n";
    printOptions(std::cerr);
    std::cerr << "Scans and displays the row count of the ORC files.\n";
    return 1;
  }
  for (int i = 0; i < argc; ++i) {
    try {
      scanFile(std::cout, argv[i], batchSize, rowReaderOptions, showMetrics);
    } catch (std::exception& ex) {
      std::cerr << "Caught exception in " << argv[i] << ": " << ex.what() << "\n";
      return 1;
    }
  }
  return 0;
}
