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

#ifndef ORC_OPTIONS_HH
#define ORC_OPTIONS_HH

#include "orc/Int128.hh"
#include "orc/OrcFile.hh"
#include "orc/Reader.hh"

#include <limits>

namespace orc {

  enum ColumnSelection {
    ColumnSelection_NONE = 0,
    ColumnSelection_NAMES = 1,
    ColumnSelection_FIELD_IDS = 2,
    ColumnSelection_TYPE_IDS = 3,
  };

  enum ColumnFilter {
    ColumnFilter_NONE = 0,
    ColumnFilter_NAMES = 1,
    ColumnFilter_FIELD_IDS = 2,
    ColumnFilter_TYPE_IDS = 3,
  };

  /**
   * ReaderOptions Implementation
   */
  struct ReaderOptionsPrivate {
    uint64_t tailLocation;
    std::ostream* errorStream;
    MemoryPool* memoryPool;
    std::string serializedTail;
    ReaderMetrics* metrics;

    ReaderOptionsPrivate() {
      tailLocation = std::numeric_limits<uint64_t>::max();
      errorStream = &std::cerr;
      memoryPool = getDefaultPool();
      metrics = nullptr;
    }
  };

  ReaderOptions::ReaderOptions() : privateBits(std::make_unique<ReaderOptionsPrivate>()) {
    // PASS
  }

  ReaderOptions::ReaderOptions(const ReaderOptions& rhs)
      : privateBits(std::make_unique<ReaderOptionsPrivate>(*(rhs.privateBits.get()))) {
    // PASS
  }

  ReaderOptions::ReaderOptions(ReaderOptions& rhs) {
    // swap privateBits with rhs
    privateBits.swap(rhs.privateBits);
  }

  ReaderOptions& ReaderOptions::operator=(const ReaderOptions& rhs) {
    if (this != &rhs) {
      privateBits.reset(new ReaderOptionsPrivate(*(rhs.privateBits.get())));
    }
    return *this;
  }

  ReaderOptions::~ReaderOptions() {
    // PASS
  }

  ReaderOptions& ReaderOptions::setMemoryPool(MemoryPool& pool) {
    privateBits->memoryPool = &pool;
    return *this;
  }

  MemoryPool* ReaderOptions::getMemoryPool() const {
    return privateBits->memoryPool;
  }

  ReaderOptions& ReaderOptions::setReaderMetrics(ReaderMetrics* metrics) {
    privateBits->metrics = metrics;
    return *this;
  }

  ReaderMetrics* ReaderOptions::getReaderMetrics() const {
    return privateBits->metrics;
  }

  ReaderOptions& ReaderOptions::setTailLocation(uint64_t offset) {
    privateBits->tailLocation = offset;
    return *this;
  }

  uint64_t ReaderOptions::getTailLocation() const {
    return privateBits->tailLocation;
  }

  ReaderOptions& ReaderOptions::setSerializedFileTail(const std::string& value) {
    privateBits->serializedTail = value;
    return *this;
  }

  std::string ReaderOptions::getSerializedFileTail() const {
    return privateBits->serializedTail;
  }

  ReaderOptions& ReaderOptions::setErrorStream(std::ostream& stream) {
    privateBits->errorStream = &stream;
    return *this;
  }

  std::ostream* ReaderOptions::getErrorStream() const {
    return privateBits->errorStream;
  }

  /**
   * RowReaderOptions Implementation
   */

  struct RowReaderOptionsPrivate {
    ColumnSelection selection;
    std::list<uint64_t> includedColumnIndexes;
    std::list<std::string> includedColumnNames;
    ColumnFilter filter;
    std::list<uint64_t> filterColumnIndexes;
    std::list<std::string> filterColumnNames;
    uint64_t dataStart;
    uint64_t dataLength;
    bool throwOnHive11DecimalOverflow;
    int32_t forcedScaleOnHive11Decimal;
    bool enableLazyDecoding;
    std::shared_ptr<SearchArgument> sargs;
    std::string readerTimezone;
    RowReaderOptions::IdReadIntentMap idReadIntentMap;
    bool useTightNumericVector;

    RowReaderOptionsPrivate() {
      selection = ColumnSelection_NONE;
      dataStart = 0;
      dataLength = std::numeric_limits<uint64_t>::max();
      throwOnHive11DecimalOverflow = true;
      forcedScaleOnHive11Decimal = 6;
      enableLazyDecoding = false;
      readerTimezone = "GMT";
      useTightNumericVector = false;
    }
  };

  RowReaderOptions::RowReaderOptions() : privateBits(std::make_unique<RowReaderOptionsPrivate>()) {
    // PASS
  }

  RowReaderOptions::RowReaderOptions(const RowReaderOptions& rhs)
      : privateBits(std::make_unique<RowReaderOptionsPrivate>(*(rhs.privateBits.get()))) {
    // PASS
  }

  RowReaderOptions::RowReaderOptions(RowReaderOptions& rhs) {
    // swap privateBits with rhs
    privateBits.swap(rhs.privateBits);
  }

  RowReaderOptions& RowReaderOptions::operator=(const RowReaderOptions& rhs) {
    if (this != &rhs) {
      privateBits.reset(new RowReaderOptionsPrivate(*(rhs.privateBits.get())));
    }
    return *this;
  }

  RowReaderOptions::~RowReaderOptions() {
    // PASS
  }

  RowReaderOptions& RowReaderOptions::include(const std::list<uint64_t>& include) {
    privateBits->selection = ColumnSelection_FIELD_IDS;
    privateBits->includedColumnIndexes.assign(include.begin(), include.end());
    privateBits->includedColumnNames.clear();
    privateBits->idReadIntentMap.clear();
    return *this;
  }

  RowReaderOptions& RowReaderOptions::include(const std::list<std::string>& include) {
    privateBits->selection = ColumnSelection_NAMES;
    privateBits->includedColumnNames.assign(include.begin(), include.end());
    privateBits->includedColumnIndexes.clear();
    privateBits->idReadIntentMap.clear();
    return *this;
  }

  RowReaderOptions& RowReaderOptions::includeTypes(const std::list<uint64_t>& types) {
    privateBits->selection = ColumnSelection_TYPE_IDS;
    privateBits->includedColumnIndexes.assign(types.begin(), types.end());
    privateBits->includedColumnNames.clear();
    privateBits->idReadIntentMap.clear();
    return *this;
  }

  RowReaderOptions& RowReaderOptions::includeTypesWithIntents(
      const IdReadIntentMap& idReadIntentMap) {
    privateBits->selection = ColumnSelection_TYPE_IDS;
    privateBits->includedColumnIndexes.clear();
    privateBits->idReadIntentMap.clear();
    for (const auto& typeIntentPair : idReadIntentMap) {
      privateBits->idReadIntentMap[typeIntentPair.first] = typeIntentPair.second;
      privateBits->includedColumnIndexes.push_back(typeIntentPair.first);
    }
    privateBits->includedColumnNames.clear();
    return *this;
  }

  RowReaderOptions& RowReaderOptions::filter(const std::list<uint64_t>& filterColIndexes) {
    privateBits->filter = ColumnFilter_FIELD_IDS;
    privateBits->filterColumnIndexes.assign(filterColIndexes.begin(), filterColIndexes.end());
    privateBits->filterColumnNames.clear();
    return *this;
  }

  RowReaderOptions& RowReaderOptions::filter(const std::list<std::string>& filterColNames) {
    privateBits->filter = ColumnFilter_NAMES;
    privateBits->filterColumnNames.assign(filterColNames.begin(), filterColNames.end());
    privateBits->filterColumnIndexes.clear();
    return *this;
  }

  RowReaderOptions& RowReaderOptions::filterTypes(const std::list<uint64_t>& types) {
    privateBits->filter = ColumnFilter_TYPE_IDS;
    privateBits->filterColumnIndexes.assign(types.begin(), types.end());
    privateBits->filterColumnNames.clear();
    return *this;
  }

  RowReaderOptions& RowReaderOptions::range(uint64_t offset, uint64_t length) {
    privateBits->dataStart = offset;
    privateBits->dataLength = length;
    return *this;
  }

  bool RowReaderOptions::getIndexesSet() const {
    return privateBits->selection == ColumnSelection_FIELD_IDS;
  }

  bool RowReaderOptions::getTypeIdsSet() const {
    return privateBits->selection == ColumnSelection_TYPE_IDS;
  }

  const std::list<uint64_t>& RowReaderOptions::getInclude() const {
    return privateBits->includedColumnIndexes;
  }

  bool RowReaderOptions::getNamesSet() const {
    return privateBits->selection == ColumnSelection_NAMES;
  }

  const std::list<std::string>& RowReaderOptions::getIncludeNames() const {
    return privateBits->includedColumnNames;
  }

  const std::list<std::string>& RowReaderOptions::getFilterColNames() const {
    return privateBits->filterColumnNames;
  }

  bool RowReaderOptions::getFilterTypeIdsSet() const {
    return privateBits->filter == ColumnFilter_TYPE_IDS;
  }

  const std::list<uint64_t>& RowReaderOptions::getFilterTypeIds() const {
    return privateBits->filterColumnIndexes;
  }

  uint64_t RowReaderOptions::getOffset() const {
    return privateBits->dataStart;
  }

  uint64_t RowReaderOptions::getLength() const {
    return privateBits->dataLength;
  }

  RowReaderOptions& RowReaderOptions::throwOnHive11DecimalOverflow(bool shouldThrow) {
    privateBits->throwOnHive11DecimalOverflow = shouldThrow;
    return *this;
  }

  bool RowReaderOptions::getThrowOnHive11DecimalOverflow() const {
    return privateBits->throwOnHive11DecimalOverflow;
  }

  RowReaderOptions& RowReaderOptions::forcedScaleOnHive11Decimal(int32_t forcedScale) {
    privateBits->forcedScaleOnHive11Decimal = forcedScale;
    return *this;
  }

  int32_t RowReaderOptions::getForcedScaleOnHive11Decimal() const {
    return privateBits->forcedScaleOnHive11Decimal;
  }

  bool RowReaderOptions::getEnableLazyDecoding() const {
    return privateBits->enableLazyDecoding;
  }

  RowReaderOptions& RowReaderOptions::setEnableLazyDecoding(bool enable) {
    privateBits->enableLazyDecoding = enable;
    return *this;
  }

  RowReaderOptions& RowReaderOptions::searchArgument(std::unique_ptr<SearchArgument> sargs) {
    privateBits->sargs = std::move(sargs);
    return *this;
  }

  std::shared_ptr<SearchArgument> RowReaderOptions::getSearchArgument() const {
    return privateBits->sargs;
  }

  RowReaderOptions& RowReaderOptions::setTimezoneName(const std::string& zoneName) {
    privateBits->readerTimezone = zoneName;
    return *this;
  }

  const std::string& RowReaderOptions::getTimezoneName() const {
    return privateBits->readerTimezone;
  }

  const RowReaderOptions::IdReadIntentMap RowReaderOptions::getIdReadIntentMap() const {
    return privateBits->idReadIntentMap;
  }

  RowReaderOptions& RowReaderOptions::setUseTightNumericVector(bool useTightNumericVector) {
    privateBits->useTightNumericVector = useTightNumericVector;
    return *this;
  }

  bool RowReaderOptions::getUseTightNumericVector() const {
    return privateBits->useTightNumericVector;
  }
}  // namespace orc

#endif
