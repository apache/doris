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
#include <map>
#include <memory>
#include <string>

class TestMemoryPool : public orc::MemoryPool {
 private:
  std::map<char*, uint64_t> blocks;
  uint64_t totalMemory;
  uint64_t maxMemory;

 public:
  char* malloc(uint64_t size) override {
    char* p = static_cast<char*>(std::malloc(size));
    blocks[p] = size;
    totalMemory += size;
    if (maxMemory < totalMemory) {
      maxMemory = totalMemory;
    }
    return p;
  }

  void free(char* p) override {
    std::free(p);
    totalMemory -= blocks[p];
    blocks.erase(p);
  }

  uint64_t getMaxMemory() {
    return maxMemory;
  }

  TestMemoryPool() : totalMemory(0), maxMemory(0) {}
  ~TestMemoryPool() override;
};

TestMemoryPool::~TestMemoryPool() {}

void processFile(const char* filename, const orc::RowReaderOptions& rowReaderOpts,
                 uint64_t batchSize) {
  orc::ReaderOptions readerOpts;
  std::unique_ptr<orc::MemoryPool> pool(new TestMemoryPool());
  readerOpts.setMemoryPool(*(pool.get()));

  std::unique_ptr<orc::Reader> reader = orc::createReader(
      orc::readFile(std::string(filename), readerOpts.getReaderMetrics()), readerOpts);
  std::unique_ptr<orc::RowReader> rowReader = reader->createRowReader(rowReaderOpts);

  std::unique_ptr<orc::ColumnVectorBatch> batch = rowReader->createRowBatch(batchSize);
  uint64_t readerMemory;
  if (rowReaderOpts.getIndexesSet()) {
    readerMemory = reader->getMemoryUseByFieldId(rowReaderOpts.getInclude());
  } else if (rowReaderOpts.getNamesSet()) {
    readerMemory = reader->getMemoryUseByName(rowReaderOpts.getIncludeNames());
  } else if (rowReaderOpts.getTypeIdsSet()) {
    readerMemory = reader->getMemoryUseByTypeId(rowReaderOpts.getInclude());
  } else {
    // default is to select all columns
    readerMemory = reader->getMemoryUseByName({});
  }

  uint64_t batchMemory = batch->getMemoryUsage();
  while (rowReader->next(*batch)) {
  }
  uint64_t actualMemory = static_cast<TestMemoryPool*>(pool.get())->getMaxMemory();
  std::cout << "Reader memory estimate: " << readerMemory << "\nBatch memory estimate:  ";
  if (batch->hasVariableLength()) {
    std::cout << "Cannot estimate because reading ARRAY or MAP columns";
  } else {
    std::cout << batchMemory << "\nTotal memory estimate:  " << readerMemory + batchMemory;
  }
  std::cout << "\nActual max memory used: " << actualMemory << "\n";
}

int main(int argc, char* argv[]) {
  uint64_t batchSize = 1000;
  orc::RowReaderOptions rowReaderOptions;
  bool showMetrics = false;
  bool success = parseOptions(&argc, &argv, &batchSize, &rowReaderOptions, &showMetrics);
  if (argc < 1 || !success) {
    std::cerr << "Usage: orc-memory [options] <filename>...\n";
    printOptions(std::cerr);
    std::cerr << "Estimate the memory footprint for reading ORC files\n";
    return 1;
  }
  for (int i = 0; i < argc; ++i) {
    try {
      processFile(argv[i], rowReaderOptions, batchSize);
    } catch (std::exception& ex) {
      std::cerr << "Caught exception: " << ex.what() << "\n";
      return 1;
    }
  }
  return 0;
}
