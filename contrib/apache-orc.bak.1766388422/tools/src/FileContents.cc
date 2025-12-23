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
#include <memory>
#include <string>

void printContents(const char* filename, const orc::RowReaderOptions& rowReaderOpts) {
  orc::ReaderOptions readerOpts;
  std::unique_ptr<orc::Reader> reader;
  std::unique_ptr<orc::RowReader> rowReader;
  reader = orc::createReader(orc::readFile(std::string(filename), readerOpts.getReaderMetrics()),
                             readerOpts);
  rowReader = reader->createRowReader(rowReaderOpts);

  std::unique_ptr<orc::ColumnVectorBatch> batch = rowReader->createRowBatch(1000);
  std::string line;
  std::unique_ptr<orc::ColumnPrinter> printer =
      createColumnPrinter(line, &rowReader->getSelectedType());

  while (rowReader->next(*batch)) {
    printer->reset(*batch);
    for (unsigned long i = 0; i < batch->numElements; ++i) {
      line.clear();
      printer->printRow(i);
      line += "\n";
      const char* str = line.c_str();
      fwrite(str, 1, strlen(str), stdout);
    }
  }
}

int main(int argc, char* argv[]) {
  uint64_t batchSize;  // not used
  orc::RowReaderOptions rowReaderOptions;
  bool showMetrics = false;
  bool success = parseOptions(&argc, &argv, &batchSize, &rowReaderOptions, &showMetrics);

  if (argc < 1 || !success) {
    std::cerr << "Usage: orc-contents [options] <filename>...\n";
    printOptions(std::cerr);
    std::cerr << "Print contents of ORC files.\n";
    return 1;
  }
  for (int i = 0; i < argc; ++i) {
    try {
      printContents(argv[i], rowReaderOptions);
    } catch (std::exception& ex) {
      std::cerr << "Caught exception in " << argv[i] << ": " << ex.what() << "\n";
      return 1;
    }
  }
  return 0;
}
