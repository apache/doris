// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#ifndef DORIS_BE_SRC_OLAP_CUMULATIVE_COMPACTION_POLICY_H
#define DORIS_BE_SRC_OLAP_CUMULATIVE_COMPACTION_POLICY_H

#include <string>

#include "olap/utils.h"
#include "olap/tablet.h"
#include "olap/tablet_meta.h"
#include "olap/rowset/rowset_meta.h"
#include "olap/rowset/rowset.h"

namespace doris {

class Tablet;

/// This CompactionPolicyType enum is used to represent the type of compaction policy.
/// Now it has two values, CUMULATIVE_ORIGINAL_POLICY and CUMULATIVE_UNIVERSAL_POLICY.
/// CUMULATIVE_ORIGINAL_POLICY means current compaction policy implemented by original policy.
/// CUMULATIVE_UNIVERSAL_POLICY means current comapction policy implemented by universal policy.
enum CompactionPolicyType {
    CUMULATIVE_ORIGINAL_POLICY = 0,
    CUMULATIVE_UNIVERSAL_POLICY = 1,
};

const static std::string CUMULATIVE_ORIGINAL_POLICY_TYPE = "ORIGINAL";
const static std::string CUMULATIVE_UNIVERSAL_POLICY_TYPE = "UNIVERSAL";
/// This class CumulativeCompactionPolicy is the base class of cumulative compaction policy.
/// It defines the policy to do cumulative compaction. It has different derived classes, which implements 
/// concrete cumulative compaction algorithm. The policy is configured by conf::cumulative_compaction_policy.
/// The policy functions is the main steps to do cumulative compaction. For example, how to pick candicate 
/// rowsets from tablet using current policy, how to calculate the cumulative point and how to calculate
/// the tablet cumulative compcation score and so on.
class CumulativeCompactionPolicy {

public:
    /// Constructor function of CumulativeCompactionPolicy, 
    /// it needs tablet pointer to access tablet method. 
    /// param tablet, the shared pointer of tablet
    CumulativeCompactionPolicy(Tablet* tablet) : _tablet(tablet){}

    /// Destructor function of CumulativeCompactionPolicy.
    virtual ~CumulativeCompactionPolicy() {}

    /// Calculate the cumulative compaction score of the tablet. This function uses rowsets meta and current 
    /// cumulative point to calculative the score of tablet. The score depends on the concrete algorithm of policy.
    /// In general, the score represents the segments nums to do cumulative compaction in total rowsets. The more
    /// score tablet gets, the earlier it can do  cumulative compaction.
    /// param all_rowsets, all rowsets in tablet.
    /// param current_cumulative_point, current cumulative point value.
    /// return score, the result score after calculate.
    virtual void calc_cumulative_compaction_score(
            const std::vector<RowsetMetaSharedPtr>& all_rowsets, int64_t current_cumulative_point,
            uint32_t* score) = 0;

    /// This function implements the policy which represents how to pick the candicate rowsets for compaction. 
    /// This base class gives a unified implemention. Its derived classes also can overide this function each other.
    /// param skip_window_sec, it means skipping the rowsets which use create time plus skip_window_sec is greater than now.
    /// param rs_version_map, mapping from version to rowset
    /// param cumulative_point,  current cumulative point of tablet
    /// return candidate_rowsets, the container of candidate rowsets 
    virtual void pick_candicate_rowsets(
            int64_t skip_window_sec,
            std::unordered_map<Version, RowsetSharedPtr, HashOfVersion>& rs_version_map,
            int64_t cumulative_point, std::vector<RowsetSharedPtr>* candidate_rowsets);
    
    /// Pick input rowsets from candidate rowsets for compaction. This function is pure virtual function. 
    /// Its implemention depands on concrete compaction policy.
    /// param candidate_rowsets, the candidate_rowsets vector container to pick input rowsets
    /// return input_rowsets, the vector container as return
    /// return last_delete_version, if has delete rowset, record the delete version from input_rowsets
    /// return compaction_score, calculate the compaction score of picked input rowset
    virtual void pick_input_rowsets(std::vector<RowsetSharedPtr>& candidate_rowsets,
                                    const int64_t max_compaction_score,
                                    std::vector<RowsetSharedPtr>* input_rowsets,
                                    Version* last_delete_version, size_t* compaction_score) = 0;

    /// Update tablet's cumulative point after cumulative compaction finished. This function is pure virtual function.
    /// Each derived has its own update policy which deponds on its concrete algorithm. When the cumulative point moves 
    /// after output rowset, then output rowset will do base compaction next time.
    /// param input_rowsets, the picked input rowset to do compaction just now
    /// param output_rowset, the result rowset after compaction
    virtual void update_cumulative_point(std::vector<RowsetSharedPtr>& input_rowsets,
                                         RowsetSharedPtr output_rowset) = 0;

    /// Calculate tablet's cumulatvie point before compaction. This calculation just executes once when the tablet compacts
    /// first time after BE initialization and then motion of cumulatvie point depends on update_cumulative_point policy.
    /// This function is pure virtual function. In genaral, the cumulative point splits the rowsets into two parts:
    /// base rowsets, cumulative rowsets.
    /// param all_rowsets, all rowsets in the tablet
    /// param kInvalidCumulativePoint, the value to represent whether the cumulative point is initialized
    /// param current_cumulative_point, current cumulative position
    /// return cumulative_point, the result of calculating cumulative point position
    virtual void calculate_cumulative_point(const std::vector<RowsetMetaSharedPtr>& all_rowsets,
                                            const int64_t kInvalidCumulativePoint,
                                            int64_t current_cumulative_point,
                                            int64_t* cumulative_point) = 0;

protected:
    /// tablet pointer
    Tablet* _tablet;
};

/// Original cumulative compcation policy implemention. Original policy which derives CumulativeCompactionPolicy is early 
/// basic algorithm. This policy uses linear structure to compact rowsets. The cumulative rowsets compact only once and 
/// then the output will do base compaction. It can make segments of rowsets in order and compact small rowsets to a bigger one.
class OriginalCumulativeCompactionPolicy : public CumulativeCompactionPolicy {
    
public:
    /// Constructor function of OriginalCumulativeCompactionPolicy, 
    /// it needs tablet pointer to access tablet method. 
    /// param tablet, the shared pointer of tablet
    OriginalCumulativeCompactionPolicy(Tablet* tablet)
            : CumulativeCompactionPolicy(tablet){}

    /// Destructor function of OriginalCumulativeCompactionPolicy.
    ~OriginalCumulativeCompactionPolicy() {}

    /// Original cumulative compaction policy implements pick input rowsets function.
    /// Its main policy is picking rowsets from candidate rowsets by comparing accumulative compaction_score and
    /// max_cumulative_compaction_num_singleton_deltas or checking whether there is delete version rowset.
    void pick_input_rowsets(std::vector<RowsetSharedPtr>& candidate_rowsets,
                            const int64_t max_compaction_score,
                            std::vector<RowsetSharedPtr>* input_rowsets,
                            Version* last_delete_version, size_t* compaction_score) override;

    /// Original cumulative compaction policy implements update cumulative point function.
    /// Its main policy is using the last input version to update the cumulative point. It aims that every rowsets only 
    /// do compact once.
    void update_cumulative_point(std::vector<RowsetSharedPtr>& input_rowsets,
                                 RowsetSharedPtr _output_rowset) override;

    /// Original cumulative compaction policy implements calculate cumulative point function.
    /// When the first time the tablet does compact, this calculation is executed. Its main policy is to find first rowset
    /// which is segments_overlapping type, it represent this rowset is not compacted and use this version as cumulative point. 
    void calculate_cumulative_point(const std::vector<RowsetMetaSharedPtr>& existing_rss,
                                     const int64_t kInvalidCumulativePoint,
                                     int64_t current_cumulative_point,
                                     int64_t* cumulative_point) override;

    /// Original cumulative compaction policy implements calc cumulative compaction score function.
    /// Its main policy is calculating the accumulative compaction score after current cumulative_point in tablet.
    void calc_cumulative_compaction_score(const std::vector<RowsetMetaSharedPtr>& all_rowsets,
                                          int64_t current_cumulative_point,
                                          uint32_t* score) override;

};

class UniversalCumulativeCompactionPolicy : public CumulativeCompactionPolicy {

public:
    UniversalCumulativeCompactionPolicy(Tablet* tablet)
            : CumulativeCompactionPolicy(tablet) {}
    ~UniversalCumulativeCompactionPolicy() {}

    void calculate_cumulative_point(const std::vector<RowsetMetaSharedPtr>& existing_rss,
                                    const int64_t kInvalidCumulativePoint,
                                    int64_t current_cumulative_point, int64_t* cumulative_point) override {
        // todo: to implement
    }

    void pick_input_rowsets(std::vector<RowsetSharedPtr>& candidate_rowsets,
                            const int64_t max_compaction_score,
                            std::vector<RowsetSharedPtr>* input_rowsets,
                            Version* last_delete_version, size_t* compaction_score) override {
        // todo: to implement
    }

    void update_cumulative_point(std::vector<RowsetSharedPtr>& input_rowsets,
                                 RowsetSharedPtr _output_rowset) override {
        // todo: to implement
    }

    void calc_cumulative_compaction_score(const std::vector<RowsetMetaSharedPtr>& all_rowsets,
                                          int64_t current_cumulative_point,
                                          uint32_t* score) override {
        // todo: to implement
    }
};

class CumulativeCompactionPolicyFactory {

public:
    static std::shared_ptr<CumulativeCompactionPolicy> create_cumulative_compaction_policy(std::string policy, Tablet* tablet);

private:
    static void _parse_cumulative_compaction_policy(std::string policy, CompactionPolicyType *policy_type);
};

}
#endif // DORIS_BE_SRC_OLAP_CUMULATIVE_COMPACTION_POLICY_H
