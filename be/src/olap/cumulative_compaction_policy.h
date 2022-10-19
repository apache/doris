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

#pragma once

#include <string>

#include "olap/rowset/rowset.h"
#include "olap/rowset/rowset_meta.h"
#include "olap/tablet.h"
#include "olap/tablet_meta.h"
#include "olap/utils.h"

namespace doris {

class Tablet;

const static std::string CUMULATIVE_SIZE_BASED_POLICY = "SIZE_BASED";

/// This class CumulativeCompactionPolicy is the base class of cumulative compaction policy.
/// It defines the policy to do cumulative compaction. It has different derived classes, which implements
/// concrete cumulative compaction algorithm. The policy is configured by conf::cumulative_compaction_policy.
/// The policy functions is the main steps to do cumulative compaction. For example, how to pick candidate
/// rowsets from tablet using current policy, how to calculate the cumulative point and how to calculate
/// the tablet cumulative compaction score and so on.
class CumulativeCompactionPolicy {
public:
    /// Constructor function of CumulativeCompactionPolicy,
    /// it needs tablet pointer to access tablet method.
    /// param tablet, the shared pointer of tablet
    CumulativeCompactionPolicy() {}

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
            TabletState state, const std::vector<RowsetMetaSharedPtr>& all_rowsets,
            int64_t current_cumulative_point, uint32_t* score) = 0;

    /// This function implements the policy which represents how to pick the candidate rowsets for compaction.
    /// This base class gives a unified implementation. Its derived classes also can override this function each other.
    /// param rs_version_map, mapping from version to rowset
    /// param cumulative_point,  current cumulative point of tablet
    /// return candidate_rowsets, the container of candidate rowsets
    virtual void pick_candidate_rowsets(
            const std::unordered_map<Version, RowsetSharedPtr, HashOfVersion>& rs_version_map,
            int64_t cumulative_point, std::vector<RowsetSharedPtr>* candidate_rowsets);

    /// Pick input rowsets from candidate rowsets for compaction. This function is pure virtual function.
    /// Its implementation depends on concrete compaction policy.
    /// param candidate_rowsets, the candidate_rowsets vector container to pick input rowsets
    /// return input_rowsets, the vector container as return
    /// return last_delete_version, if has delete rowset, record the delete version from input_rowsets
    /// return compaction_score, calculate the compaction score of picked input rowset
    virtual int pick_input_rowsets(Tablet* tablet,
                                   const std::vector<RowsetSharedPtr>& candidate_rowsets,
                                   const int64_t max_compaction_score,
                                   const int64_t min_compaction_score,
                                   std::vector<RowsetSharedPtr>* input_rowsets,
                                   Version* last_delete_version, size_t* compaction_score) = 0;

    /// Update tablet's cumulative point after cumulative compaction finished. This function is pure virtual function.
    /// Each derived has its own update policy which depends on its concrete algorithm. When the cumulative point moves
    /// after output rowset, then output rowset will do base compaction next time.
    /// param input_rowsets, the picked input rowset to do compaction just now
    /// param output_rowset, the result rowset after compaction
    virtual void update_cumulative_point(Tablet* tablet,
                                         const std::vector<RowsetSharedPtr>& input_rowsets,
                                         RowsetSharedPtr output_rowset,
                                         Version& last_delete_version) = 0;

    /// Calculate tablet's cumulatiuve point before compaction. This calculation just executes once when the tablet compacts
    /// first time after BE initialization and then motion of cumulatiuve point depends on update_cumulative_point policy.
    /// This function is pure virtual function. In general, the cumulative point splits the rowsets into two parts:
    /// base rowsets, cumulative rowsets.
    /// param all_rowsets, all rowsets in the tablet
    /// param current_cumulative_point, current cumulative position
    /// return cumulative_point, the result of calculating cumulative point position
    virtual void calculate_cumulative_point(Tablet* tablet,
                                            const std::vector<RowsetMetaSharedPtr>& all_rowsets,
                                            int64_t current_cumulative_point,
                                            int64_t* cumulative_point) = 0;

    /// Fetch cumulative policy name
    virtual std::string name() = 0;
};

/// SizeBased cumulative compaction policy implemention. SizeBased policy which derives CumulativeCompactionPolicy is a optimized
/// version of num based cumulative compaction policy. This policy also uses linear structure to compact rowsets. The cumulative rowsets
/// can do compaction when they are in same level size. And when output rowset exceeds the promotion radio of base size or min promotion
/// size, it will do base compaction. This policy is targeting the use cases requiring lower write amplification, trading off read
/// amplification and space amplification.
class SizeBasedCumulativeCompactionPolicy final : public CumulativeCompactionPolicy {
public:
    /// Constructor function of SizeBasedCumulativeCompactionPolicy,
    /// it needs tablet pointer to access tablet method.
    /// param tablet, the shared pointer of tablet
    SizeBasedCumulativeCompactionPolicy(
            int64_t size_based_promotion_size =
                    config::cumulative_size_based_promotion_size_mbytes * 1024 * 1024,
            double size_based_promotion_ratio = config::cumulative_size_based_promotion_ratio,
            int64_t size_based_promotion_min_size =
                    config::cumulative_size_based_promotion_min_size_mbytes * 1024 * 1024,
            int64_t size_based_compaction_lower_bound_size =
                    config::cumulative_size_based_compaction_lower_size_mbytes * 1024 * 1024);

    /// Destructor function of SizeBasedCumulativeCompactionPolicy.
    ~SizeBasedCumulativeCompactionPolicy() {}

    /// SizeBased cumulative compaction policy implements calculate cumulative point function.
    /// When the first time the tablet does compact, this calculation is executed. Its main policy is to find first rowset
    /// which does not satisfied the promotion conditions.
    void calculate_cumulative_point(Tablet* tablet,
                                    const std::vector<RowsetMetaSharedPtr>& all_rowsets,
                                    int64_t current_cumulative_point,
                                    int64_t* cumulative_point) override;

    /// SizeBased cumulative compaction policy implements pick input rowsets function.
    /// Its main policy is picking rowsets from candidate rowsets by comparing accumulative compaction_score,
    /// max_cumulative_compaction_num_singleton_deltas or checking whether there is delete version rowset,
    /// and choose those rowset in the same level to do cumulative compaction.
    int pick_input_rowsets(Tablet* tablet, const std::vector<RowsetSharedPtr>& candidate_rowsets,
                           const int64_t max_compaction_score, const int64_t min_compaction_score,
                           std::vector<RowsetSharedPtr>* input_rowsets,
                           Version* last_delete_version, size_t* compaction_score) override;

    /// SizeBased cumulative compaction policy implements update cumulative point function.
    /// Its main policy is judging the output rowset size whether satisfied the promotion size.
    /// If it satisfied, this policy will update the cumulative point.
    void update_cumulative_point(Tablet* tablet, const std::vector<RowsetSharedPtr>& input_rowsets,
                                 RowsetSharedPtr _output_rowset,
                                 Version& last_delete_version) override;

    /// Num based cumulative compaction policy implements calc cumulative compaction score function.
    /// Its main policy is calculating the accumulative compaction score after current cumulative_point in tablet.
    void calc_cumulative_compaction_score(TabletState state,
                                          const std::vector<RowsetMetaSharedPtr>& all_rowsets,
                                          int64_t current_cumulative_point,
                                          uint32_t* score) override;

    std::string name() override { return CUMULATIVE_SIZE_BASED_POLICY; }

private:
    /// calculate promotion size using current base rowset meta size and promotion configs
    void _calc_promotion_size(RowsetMetaSharedPtr base_rowset_meta, int64_t* promotion_size);

    /// calculate the disk size belong to which level, the level is divide by power of 2
    /// between cumulative_size_based_promotion_min_size_mbytes
    /// and cumulative_size_based_promotion_size_mbytes
    int _level_size(const int64_t size);

    /// when policy calculate cumulative_compaction_score, update promotion size at the same time
    void _refresh_tablet_size_based_promotion_size(int64_t promotion_size);

private:
    /// cumulative compaction promotion size, unit is byte.
    int64_t _size_based_promotion_size;
    /// cumulative compaction promotion ratio of base rowset total disk size.
    double _size_based_promotion_ratio;
    /// cumulative compaction promotion min size, unit is byte.
    int64_t _size_based_promotion_min_size;
    /// lower bound size to do compaction compaction.
    int64_t _size_based_compaction_lower_bound_size;
    /// record tablet promotion size, it is updated each time when calculate cumulative_compaction_score
    int64_t _tablet_size_based_promotion_size;
    /// levels division of disk size, same level rowsets can do compaction
    std::vector<int64_t> _levels;
};

/// The factory of CumulativeCompactionPolicy, it can product different policy according to the `policy` parameter.
class CumulativeCompactionPolicyFactory {
public:
    /// Static factory function. It can product different policy according to the `policy` parameter and use tablet ptr
    /// to construct the policy. Now it can product size based and num based policies.
    static std::shared_ptr<CumulativeCompactionPolicy> create_cumulative_compaction_policy();
};

} // namespace doris
