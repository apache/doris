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

#ifndef ORC_SARGSAPPLIER_HH
#define ORC_SARGSAPPLIER_HH

#include <orc/Common.hh>
#include "orc/BloomFilter.hh"
#include "orc/Reader.hh"
#include "orc/Type.hh"
#include "wrap/orc-proto-wrapper.hh"

#include "sargs/SearchArgument.hh"

#include "SchemaEvolution.hh"

#include <unordered_map>

namespace orc {

  class SargsApplier {
   public:
    SargsApplier(const Type& type, const SearchArgument* searchArgument, uint64_t rowIndexStride,
                 WriterVersion writerVersion, ReaderMetrics* metrics,
                 const SchemaEvolution* schemaEvolution = nullptr);

    /**
     * Evaluate search argument on file statistics
     * If file statistics don't satisfy the sargs,
     * the EvaluatedRowGroupCount of Reader Metrics will be updated.
     * Otherwise, Reader Metrics will not be updated and
     * will require further evaluation.
     * @return true if file statistics satisfy the sargs
     */
    bool evaluateFileStatistics(const proto::Footer& footer, uint64_t numRowGroupsInStripeRange);

    /**
     * Evaluate search argument on stripe statistics
     * If stripe statistics don't satisfy the sargs,
     * the EvaluatedRowGroupCount of Reader Metrics will be updated.
     * Otherwise, Reader Metrics will not be updated and
     * will require further evaluation.
     * @return true if stripe statistics satisfy the sargs
     */
    bool evaluateStripeStatistics(const proto::StripeStatistics& stripeStats,
                                  uint64_t stripeRowGroupCount);

    /**
     * TODO: use proto::RowIndex and proto::BloomFilter to do the evaluation
     * Pick the row groups that we need to load from the current stripe.
     * @return true if any row group is selected
     */
    bool pickRowGroups(uint64_t rowsInStripe,
                       const std::unordered_map<uint64_t, proto::RowIndex>& rowIndexes,
                       const std::map<uint32_t, BloomFilterIndex>& bloomFilters);

    /**
     * Return a vector of the next skipped row for each RowGroup. Each value is the row id
     * in stripe. 0 means the current RowGroup is entirely skipped.
     * Only valid after invoking pickRowGroups().
     */
    const std::vector<uint64_t>& getNextSkippedRows() const {
      return mNextSkippedRows;
    }

    /**
     * Indicate whether any row group is selected in the last evaluation
     */
    bool hasSelected() const {
      return mHasSelected;
    }

    /**
     * Indicate whether any row group is skipped in the last evaluation
     */
    bool hasSkipped() const {
      return mHasSkipped;
    }

    /**
     * Whether any row group from current row in the stripe matches PPD.
     */
    bool hasSelectedFrom(uint64_t currentRowInStripe) const {
      uint64_t rg = currentRowInStripe / mRowIndexStride;
      for (; rg < mNextSkippedRows.size(); ++rg) {
        if (mNextSkippedRows[rg]) {
          return true;
        }
      }
      return false;
    }

    std::pair<uint64_t, uint64_t> getStats() const {
      if (mMetrics != nullptr) {
        return std::make_pair(mMetrics->SelectedRowGroupCount.load(),
                              mMetrics->EvaluatedRowGroupCount.load());
      } else {
        return {0, 0};
      }
    }

   private:
    // evaluate column statistics in the form of protobuf::RepeatedPtrField
    typedef ::google::protobuf::RepeatedPtrField<proto::ColumnStatistics> PbColumnStatistics;
    bool evaluateColumnStatistics(const PbColumnStatistics& colStats) const;

    friend class TestSargsApplier_findColumnTest_Test;
    friend class TestSargsApplier_findArrayColumnTest_Test;
    friend class TestSargsApplier_findMapColumnTest_Test;
    static uint64_t findColumn(const Type& type, const std::string& colName);

   private:
    const Type& mType;
    const SearchArgument* mSearchArgument;
    const SchemaEvolution* mSchemaEvolution;
    uint64_t mRowIndexStride;
    WriterVersion mWriterVersion;
    // column ids for each predicate leaf in the search argument
    std::vector<uint64_t> mFilterColumns;

    // Map from RowGroup index to the next skipped row of the selected range it
    // locates. If the RowGroup is not selected, set the value to 0.
    // Calculated in pickRowGroups().
    std::vector<uint64_t> mNextSkippedRows;
    uint64_t mTotalRowsInStripe;
    bool mHasSelected;
    bool mHasSkipped;
    // store result of file stats evaluation
    bool mHasEvaluatedFileStats;
    bool mFileStatsEvalResult;
    // use the SelectedRowGroupCount and EvaluatedRowGroupCount to
    // keep stats of selected RGs and evaluated RGs
    ReaderMetrics* mMetrics;
  };

}  // namespace orc

#endif  // ORC_SARGSAPPLIER_HH
