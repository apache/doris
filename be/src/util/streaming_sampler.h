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

#ifndef DORIS_BE_SRC_UTIL_STREAMING_SAMPLER_H
#define DORIS_BE_SRC_UTIL_STREAMING_SAMPLER_H

#include <string.h>

#include <iostream>

#include "util/spinlock.h"

namespace doris {

/// A fixed-size sampler to collect samples over time. AddSample should be
/// called periodically with the sampled value. Samples are added at the max
/// resolution possible.  When the sample buffer is full, the current samples
/// are collapsed and the collection period is doubled.
/// The input period and the streaming sampler period do not need to match, the
/// streaming sampler will average values.
/// T is the type of the sample and must be a native numerical type (e.g. int or float).
template <typename T, int MAX_SAMPLES>
class StreamingSampler {
public:
    StreamingSampler(int initial_period = 500)
            : samples_collected_(0),
              period_(initial_period),
              current_sample_sum_(0),
              current_sample_count_(0),
              current_sample_total_time_(0) {}

    /// Initialize the sampler with values.
    StreamingSampler(int period, const std::vector<T>& initial_samples)
            : samples_collected_(initial_samples.size()),
              period_(period),
              current_sample_sum_(0),
              current_sample_count_(0),
              current_sample_total_time_(0) {
        DCHECK_LE(samples_collected_, MAX_SAMPLES);
        memcpy(samples_, &initial_samples[0], sizeof(T) * samples_collected_);
    }

    /// Add a sample to the sampler. 'ms' is the time elapsed since the last time this
    /// was called.
    /// The input value is accumulated into current_*. If the total time elapsed
    /// in current_sample_total_time_ is higher than the storage period, the value is
    /// stored. 'sample' should be interpreted as a representative sample from
    /// (now - ms, now].
    /// TODO: we can make this more complex by taking a weighted average of samples
    /// accumulated in a period.
    void AddSample(T sample, int ms) {
        std::lock_guard<SpinLock> l(lock_);
        ++current_sample_count_;
        current_sample_sum_ += sample;
        current_sample_total_time_ += ms;

        if (current_sample_total_time_ >= period_) {
            samples_[samples_collected_++] = current_sample_sum_ / current_sample_count_;
            current_sample_count_ = 0;
            current_sample_sum_ = 0;
            current_sample_total_time_ = 0;

            if (samples_collected_ == MAX_SAMPLES) {
                /// collapse the samples in half by averaging them and doubling the storage period
                period_ *= 2;
                for (int i = 0; i < MAX_SAMPLES / 2; ++i) {
                    samples_[i] = (samples_[i * 2] + samples_[i * 2 + 1]) / 2;
                }
                samples_collected_ /= 2;
            }
        }
    }

    /// Get the samples collected.  Returns the number of samples and
    /// the period they were collected at.
    /// If lock is non-null, the lock will be taken before returning. The caller
    /// must unlock it.
    const T* GetSamples(int* num_samples, int* period, SpinLock** lock = nullptr) const {
        if (lock != nullptr) {
            lock_.lock();
            *lock = &lock_;
        }
        *num_samples = samples_collected_;
        *period = period_;
        return samples_;
    }

    /// Set the underlying data to period/samples
    void SetSamples(int period, const std::vector<T>& samples) {
        DCHECK_LE(samples.size(), MAX_SAMPLES);

        std::lock_guard<SpinLock> l(lock_);
        period_ = period;
        samples_collected_ = samples.size();
        memcpy(samples_, &samples[0], sizeof(T) * samples_collected_);
        current_sample_sum_ = 0;
        current_sample_count_ = 0;
        current_sample_total_time_ = 0;
    }

    std::string DebugString(const std::string& prefix = "") const {
        std::lock_guard<SpinLock> l(lock_);
        std::stringstream ss;
        ss << prefix << "Period = " << period_ << std::endl
           << prefix << "Num = " << samples_collected_ << std::endl
           << prefix << "Samples = {";
        for (int i = 0; i < samples_collected_; ++i) {
            ss << samples_[i] << ", ";
        }
        ss << prefix << "}" << std::endl;
        return ss.str();
    }

private:
    mutable SpinLock lock_;

    /// Aggregated samples collected. Note: this is not all the input samples from
    /// AddSample(), as logically, those samples get resampled and aggregated.
    T samples_[MAX_SAMPLES];

    /// Number of samples collected <= MAX_SAMPLES.
    int samples_collected_;

    /// Storage period in ms.
    int period_;

    /// The sum of input samples that makes up the next stored sample.
    T current_sample_sum_;

    /// The number of input samples that contribute to current_sample_sum_.
    int current_sample_count_;

    /// The total time that current_sample_sum_ represents
    int current_sample_total_time_;
};

} // namespace doris

#endif
