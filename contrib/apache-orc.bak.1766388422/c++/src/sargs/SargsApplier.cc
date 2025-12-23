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

#include "SargsApplier.hh"
#include <numeric>

namespace orc {

  // find column id from column name
  uint64_t SargsApplier::findColumn(const Type& type, const std::string& colName) {
    for (uint64_t i = 0; i != type.getSubtypeCount(); ++i) {
      // Only STRUCT type has field names
      if (type.getKind() == STRUCT && type.getFieldName(i) == colName) {
        return type.getSubtype(i)->getColumnId();
      } else {
        uint64_t ret = findColumn(*type.getSubtype(i), colName);
        if (ret != INVALID_COLUMN_ID) {
          return ret;
        }
      }
    }
    return INVALID_COLUMN_ID;
  }

  SargsApplier::SargsApplier(const Type& type, const SearchArgument* searchArgument,
                             uint64_t rowIndexStride, WriterVersion writerVersion,
                             ReaderMetrics* metrics, const SchemaEvolution* schemaEvolution)
      : mType(type),
        mSearchArgument(searchArgument),
        mSchemaEvolution(schemaEvolution),
        mRowIndexStride(rowIndexStride),
        mWriterVersion(writerVersion),
        mHasEvaluatedFileStats(false),
        mFileStatsEvalResult(true),
        mMetrics(metrics) {
    const SearchArgumentImpl* sargs = dynamic_cast<const SearchArgumentImpl*>(mSearchArgument);

    // find the mapping from predicate leaves to columns
    const std::vector<PredicateLeaf>& leaves = sargs->getLeaves();
    mFilterColumns.resize(leaves.size(), INVALID_COLUMN_ID);
    for (size_t i = 0; i != mFilterColumns.size(); ++i) {
      if (leaves[i].hasColumnName()) {
        mFilterColumns[i] = findColumn(type, leaves[i].getColumnName());
      } else {
        mFilterColumns[i] = leaves[i].getColumnId();
      }
    }
  }

  bool SargsApplier::pickRowGroups(uint64_t rowsInStripe,
                                   const std::unordered_map<uint64_t, proto::RowIndex>& rowIndexes,
                                   const std::map<uint32_t, BloomFilterIndex>& bloomFilters) {
    // init state of each row group
    uint64_t groupsInStripe = (rowsInStripe + mRowIndexStride - 1) / mRowIndexStride;
    mNextSkippedRows.resize(groupsInStripe);
    mTotalRowsInStripe = rowsInStripe;

    // row indexes do not exist, simply read all rows
    if (rowIndexes.empty()) {
      return true;
    }

    const auto& leaves = dynamic_cast<const SearchArgumentImpl*>(mSearchArgument)->getLeaves();
    std::vector<TruthValue> leafValues(leaves.size(), TruthValue::YES_NO_NULL);
    mHasSelected = false;
    mHasSkipped = false;
    uint64_t nextSkippedRowGroup = groupsInStripe;
    size_t rowGroup = groupsInStripe;
    do {
      --rowGroup;
      for (size_t pred = 0; pred != leaves.size(); ++pred) {
        uint64_t columnIdx = mFilterColumns[pred];
        auto rowIndexIter = rowIndexes.find(columnIdx);
        if (columnIdx == INVALID_COLUMN_ID || rowIndexIter == rowIndexes.cend()) {
          // this column does not exist in current file
          leafValues[pred] = TruthValue::YES_NO_NULL;
        } else if (mSchemaEvolution && !mSchemaEvolution->isSafePPDConversion(columnIdx)) {
          // cannot evaluate predicate when ppd is not safe
          leafValues[pred] = TruthValue::YES_NO_NULL;
        } else {
          // get column statistics
          const proto::ColumnStatistics& statistics =
              rowIndexIter->second.entry(static_cast<int>(rowGroup)).statistics();

          // get bloom filter
          std::shared_ptr<BloomFilter> bloomFilter;
          auto iter = bloomFilters.find(static_cast<uint32_t>(columnIdx));
          if (iter != bloomFilters.cend()) {
            bloomFilter = iter->second.entries.at(rowGroup);
          }

          leafValues[pred] = leaves[pred].evaluate(mWriterVersion, statistics, bloomFilter.get());
        }
      }

      bool needed = isNeeded(mSearchArgument->evaluate(leafValues));
      if (!needed) {
        mNextSkippedRows[rowGroup] = 0;
        nextSkippedRowGroup = rowGroup;
      } else {
        mNextSkippedRows[rowGroup] = (nextSkippedRowGroup == groupsInStripe)
                                         ? rowsInStripe
                                         : (nextSkippedRowGroup * mRowIndexStride);
      }
      mHasSelected |= needed;
      mHasSkipped |= !needed;
    } while (rowGroup != 0);

    // update stats
    uint64_t selectedRGs = 0;
    uint64_t selectedReadRows = 0;

    for (size_t i = 0; i < mNextSkippedRows.size(); ++i) {
      if (mNextSkippedRows[i] > 0) {
        selectedRGs++;
        uint64_t rowGroupStartRow = i * mRowIndexStride;
        uint64_t rowGroupEndRow = std::min((i + 1) * mRowIndexStride, rowsInStripe);
        uint64_t rowsInCurrentRG = rowGroupEndRow - rowGroupStartRow;
        selectedReadRows += rowsInCurrentRG;
      }
    }
    if (mMetrics != nullptr) {
      mMetrics->SelectedRowGroupCount.fetch_add(selectedRGs);
      mMetrics->EvaluatedRowGroupCount.fetch_add(groupsInStripe);
      mMetrics->ReadRowCount.fetch_add(selectedReadRows);
    }

    return mHasSelected;
  }

  bool SargsApplier::evaluateColumnStatistics(const PbColumnStatistics& colStats) const {
    const SearchArgumentImpl* sargs = dynamic_cast<const SearchArgumentImpl*>(mSearchArgument);
    if (sargs == nullptr) {
      throw InvalidArgument("Failed to cast to SearchArgumentImpl");
    }

    const std::vector<PredicateLeaf>& leaves = sargs->getLeaves();
    std::vector<TruthValue> leafValues(leaves.size(), TruthValue::YES_NO_NULL);

    for (size_t pred = 0; pred != leaves.size(); ++pred) {
      uint64_t columnId = mFilterColumns[pred];
      if (columnId != INVALID_COLUMN_ID && colStats.size() > static_cast<int>(columnId)) {
        leafValues[pred] = leaves[pred].evaluate(mWriterVersion,
                                                 colStats.Get(static_cast<int>(columnId)), nullptr);
      }
    }

    return isNeeded(mSearchArgument->evaluate(leafValues));
  }

  bool SargsApplier::evaluateStripeStatistics(const proto::StripeStatistics& stripeStats,
                                              uint64_t stripeRowGroupCount) {
    if (stripeStats.colstats_size() == 0) {
      return true;
    }

    bool ret = evaluateColumnStatistics(stripeStats.colstats());
    if (!ret) {
      // reset mNextSkippedRows when the current stripe does not satisfy the PPD
      mNextSkippedRows.clear();
      if (mMetrics != nullptr) {
        mMetrics->EvaluatedRowGroupCount.fetch_add(stripeRowGroupCount);
      }
    }
    return ret;
  }

  bool SargsApplier::evaluateFileStatistics(const proto::Footer& footer,
                                            uint64_t numRowGroupsInStripeRange) {
    if (!mHasEvaluatedFileStats) {
      if (footer.statistics_size() == 0) {
        mFileStatsEvalResult = true;
      } else {
        mFileStatsEvalResult = evaluateColumnStatistics(footer.statistics());
        if (!mFileStatsEvalResult && mMetrics != nullptr) {
          mMetrics->EvaluatedRowGroupCount.fetch_add(numRowGroupsInStripeRange);
        }
      }
      mHasEvaluatedFileStats = true;
    }
    return mFileStatsEvalResult;
  }
}  // namespace orc
