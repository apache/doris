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
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/src/AggregateFunctions/ReservoirSampler.h
// and modified by Doris

#pragma once

#include <cmath>
#include <cstddef>
#include <functional>
#include <limits>

#include "vec/common/pod_array_fwd.h"
#include "vec/common/string_buffer.hpp"
#include "vec/core/types.h"

namespace doris {

/// Implementing the Reservoir Sampling algorithm. Incrementally selects from the added objects a random subset of the sample_count size.
/// Can approximately get quantiles.
/// Call `quantile` takes O(sample_count log sample_count), if after the previous call `quantile` there was at least one call `insert`. Otherwise O(1).
/// That is, it makes sense to first add, then get quantiles without adding.
/*
 * single stream/sequence (oneseq)
 */

template <typename T>
struct default_multiplier {
    // Not defined for an arbitrary type
};

template <typename T>
struct default_increment {
    // Not defined for an arbitrary type
};

#define PCG_DEFINE_CONSTANT(type, what, kind, constant) \
    template <>                                         \
    struct what##_##kind<type> {                        \
        static constexpr type kind() {                  \
            return constant;                            \
        }                                               \
    };

PCG_DEFINE_CONSTANT(uint8_t, default, multiplier, 141U)
PCG_DEFINE_CONSTANT(uint8_t, default, increment, 77U)

PCG_DEFINE_CONSTANT(uint16_t, default, multiplier, 12829U)
PCG_DEFINE_CONSTANT(uint16_t, default, increment, 47989U)

PCG_DEFINE_CONSTANT(uint32_t, default, multiplier, 747796405U)
PCG_DEFINE_CONSTANT(uint32_t, default, increment, 2891336453U)

PCG_DEFINE_CONSTANT(uint64_t, default, multiplier, 6364136223846793005ULL)
PCG_DEFINE_CONSTANT(uint64_t, default, increment, 1442695040888963407ULL)

template <typename itype>
class oneseq_stream : public default_increment<itype> {
protected:
    static constexpr bool is_mcg = false;

    // Is never called, but is provided for symmetry with specific_stream
    void set_stream(...) { abort(); }

public:
    typedef itype state_type;

    static constexpr itype stream() { return default_increment<itype>::increment() >> 1; }

    static constexpr bool can_specify_stream = false;

    static constexpr size_t streams_pow2() { return 0U; }

protected:
    constexpr oneseq_stream() = default;
};

/*
 * no stream (mcg)
 */

template <typename itype>
class no_stream {
protected:
    static constexpr bool is_mcg = true;

    // Is never called, but is provided for symmetry with specific_stream
    void set_stream(...) { abort(); }

public:
    typedef itype state_type;

    static constexpr itype increment() { return 0; }

    static constexpr bool can_specify_stream = false;

    static constexpr size_t streams_pow2() { return 0U; }

protected:
    constexpr no_stream() = default;
};

template <typename xtype, typename itype, typename output_mixin, bool output_previous = true,
          typename stream_mixin = oneseq_stream<itype>,
          typename multiplier_mixin = default_multiplier<itype>>
class engine : protected output_mixin, public stream_mixin, protected multiplier_mixin {
protected:
    itype state_;

    struct can_specify_stream_tag {};
    struct no_specifiable_stream_tag {};

    using stream_mixin::increment;
    using multiplier_mixin::multiplier;

public:
    typedef xtype result_type;
    typedef itype state_type;

    static constexpr size_t period_pow2() {
        return sizeof(state_type) * 8 - 2 * stream_mixin::is_mcg;
    }

    // It would be nice to use std::numeric_limits for these, but
    // we can't be sure that it'd be defined for the 128-bit types.

    static constexpr result_type min() { return result_type(0UL); }

    static constexpr result_type max() { return result_type(~result_type(0UL)); }

protected:
    itype bump(itype state) { return state * multiplier() + increment(); }

    itype base_generate() { return state_ = bump(state_); }

    itype base_generate0() {
        itype old_state = state_;
        state_ = bump(state_);
        return old_state;
    }

public:
    result_type operator()() {
        if (output_previous) {
            return this->output(base_generate0());
        } else {
            return this->output(base_generate());
        }
    }

    result_type operator()(result_type upper_bound) { return bounded_rand(*this, upper_bound); }

    engine(itype state = itype(0xcafef00dd15ea5e5ULL))
            : state_(this->is_mcg ? state | state_type(3U) : bump(state + this->increment())) {
        // Nothing else to do.
    }

    // This function may or may not exist.  It thus has to be a template
    // to use SFINAE; users don't have to worry about its template-ness.

    template <typename sm = stream_mixin>
    engine(itype state, typename sm::stream_state stream_seed)
            : stream_mixin(stream_seed),
              state_(this->is_mcg ? state | state_type(3U) : bump(state + this->increment())) {
        // Nothing else to do.
    }

    template <typename SeedSeq>
    engine(SeedSeq&& seedSeq,
           typename std::enable_if<!stream_mixin::can_specify_stream &&
                                           !std::is_convertible<SeedSeq, itype>::value &&
                                           !std::is_convertible<SeedSeq, engine>::value,
                                   no_specifiable_stream_tag>::type = {})
            : engine(generate_one<itype>(std::forward<SeedSeq>(seedSeq))) {
        // Nothing else to do.
    }

    template <typename SeedSeq>
    engine(SeedSeq&& seedSeq,
           typename std::enable_if<stream_mixin::can_specify_stream &&
                                           !std::is_convertible<SeedSeq, itype>::value &&
                                           !std::is_convertible<SeedSeq, engine>::value,
                                   can_specify_stream_tag>::type = {})
            : engine(generate_one<itype, 1, 2>(seedSeq), generate_one<itype, 0, 2>(seedSeq)) {
        // Nothing else to do.
    }

    template <typename... Args>
    void seed(Args&&... args) {
        new (this) engine(std::forward<Args>(args)...);
    }

    template <typename CharT, typename Traits, typename xtype1, typename itype1,
              typename output_mixin1, bool output_previous1, typename stream_mixin1,
              typename multiplier_mixin1>
    friend std::basic_ostream<CharT, Traits>& operator<<(
            std::basic_ostream<CharT, Traits>& out,
            const engine<xtype1, itype1, output_mixin1, output_previous1, stream_mixin1,
                         multiplier_mixin1>& rng) {
        auto orig_flags = out.flags(std::ios_base::dec | std::ios_base::left);
        auto space = out.widen(' ');
        auto orig_fill = out.fill();

        out << rng.multiplier() << space << rng.increment() << space << rng.state_;

        out.flags(orig_flags);
        out.fill(orig_fill);
        return out;
    }

    template <typename CharT, typename Traits, typename xtype1, typename itype1,
              typename output_mixin1, bool output_previous1, typename stream_mixin1,
              typename multiplier_mixin1>
    friend std::basic_istream<CharT, Traits>& operator>>(
            std::basic_istream<CharT, Traits>& in,
            engine<xtype1, itype1, output_mixin1, output_previous1, stream_mixin1,
                   multiplier_mixin1>& rng) {
        auto orig_flags = in.flags(std::ios_base::dec | std::ios_base::skipws);

        itype multiplier, increment, state;
        in >> multiplier >> increment >> state;

        if (!in.fail()) {
            bool good = true;
            if (multiplier != rng.multiplier()) {
                good = false;
            } else if (rng.can_specify_stream) {
                rng.set_stream(increment >> 1);
            } else if (increment != rng.increment()) {
                good = false;
            }
            if (good) {
                rng.state_ = state;
            } else {
                in.clear(std::ios::failbit);
            }
        }

        in.flags(orig_flags);
        return in;
    }
};

#ifndef PCG_BITCOUNT_T
typedef uint8_t bitcount_t;
#else
typedef PCG_BITCOUNT_T bitcount_t;
#endif

template <typename xtype, typename itype>
struct xsh_rs_mixin {
    static xtype output(itype internal) {
        constexpr bitcount_t bits = bitcount_t(sizeof(itype) * 8);
        constexpr bitcount_t xtypebits = bitcount_t(sizeof(xtype) * 8);
        constexpr bitcount_t sparebits = bits - xtypebits;
        constexpr bitcount_t opbits = sparebits - 5 >= 64   ? 5
                                      : sparebits - 4 >= 32 ? 4
                                      : sparebits - 3 >= 16 ? 3
                                      : sparebits - 2 >= 4  ? 2
                                      : sparebits - 1 >= 1  ? 1
                                                            : 0;
        constexpr bitcount_t mask = (1 << opbits) - 1;
        constexpr bitcount_t maxrandshift = mask;
        constexpr bitcount_t topspare = opbits;
        constexpr bitcount_t bottomspare = sparebits - topspare;
        constexpr bitcount_t xshift = topspare + (xtypebits + maxrandshift) / 2;
        bitcount_t rshift = opbits ? bitcount_t(internal >> (bits - opbits)) & mask : 0;
        internal ^= internal >> xshift;
        auto result = xtype(internal >> (bottomspare - maxrandshift + rshift));
        return result;
    }
};

template <typename xtype, typename itype, template <typename XT, typename IT> class output_mixin,
          bool output_previous = (sizeof(itype) <= 8)>
using mcg_base =
        engine<xtype, itype, output_mixin<xtype, itype>, output_previous, no_stream<itype>>;

class ReservoirSampler {
public:
    explicit ReservoirSampler(size_t sample_count_ = 8192) : sample_count(sample_count_) {
        rng.seed(123456);
    }

    void insert(const double v) {
        if (std::isnan(v)) {
            return;
        }

        sorted = false;
        ++total_values;
        if (samples.size() < sample_count) {
            samples.push_back(v);
        } else {
            uint64_t rnd = gen_random(total_values);
            if (rnd < sample_count) {
                samples[rnd] = v;
            }
        }
    }

    void clear() {
        samples.clear();
        sorted = false;
        total_values = 0;
        rng.seed(123456);
    }

    double quantileInterpolated(double level) {
        if (samples.empty()) {
            return std::numeric_limits<double>::quiet_NaN();
        }
        sortIfNeeded();

        double index = std::max(0., std::min(samples.size() - 1., level * (samples.size() - 1)));

        /// To get the value of a fractional index, we linearly interpolate between neighboring values.
        auto left_index = static_cast<size_t>(index);
        size_t right_index = left_index + 1;
        if (right_index == samples.size()) {
            return samples[left_index];
        }

        double left_coef = right_index - index;
        double right_coef = index - left_index;
        return samples[left_index] * left_coef + samples[right_index] * right_coef;
    }

    void merge(const ReservoirSampler& b) {
        if (sample_count != b.sample_count) {
            throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                                   "Cannot merge ReservoirSampler's with different sample_count");
        }
        sorted = false;

        if (b.total_values <= sample_count) {
            for (double sample : b.samples) {
                insert(sample);
            }
        } else if (total_values <= sample_count) {
            Array from = std::move(samples);
            samples.assign(b.samples.begin(), b.samples.end());
            total_values = b.total_values;
            for (double i : from) {
                insert(i);
            }
        } else {
            /// Replace every element in our reservoir to the b's reservoir
            /// with the probability of b.total_values / (a.total_values + b.total_values)
            /// Do it more roughly than true random sampling to save performance.
            total_values += b.total_values;
            /// Will replace every frequency'th element in a to element from b.
            double frequency = static_cast<double>(total_values) / b.total_values;
            /// When frequency is too low, replace just one random element with the corresponding probability.
            if (frequency * 2 >= sample_count) {
                uint64_t rnd = gen_random(static_cast<uint64_t>(frequency));
                if (rnd < sample_count) {
                    samples[rnd] = b.samples[rnd];
                }
            } else {
                for (double i = 0; i < sample_count; i += frequency) {
                    auto idx = static_cast<size_t>(i);
                    samples[idx] = b.samples[idx];
                }
            }
        }
    }

    void read(vectorized::BufferReadable& buf) {
        buf.read_binary(sample_count);
        buf.read_binary(total_values);

        size_t size = std::min(total_values, sample_count);
        static constexpr size_t MAX_RESERVOIR_SIZE = 1024 * 1024 * 1024; // 1GB
        if (UNLIKELY(size > MAX_RESERVOIR_SIZE)) {
            throw doris::Exception(ErrorCode::INTERNAL_ERROR, "Too large array size (maximum: {})",
                                   MAX_RESERVOIR_SIZE);
        }

        std::string rng_string;
        buf.read_binary(rng_string);
        std::stringstream rng_buf(rng_string);
        rng_buf >> rng;

        samples.resize(size);
        for (double& sample : samples) {
            buf.read_binary(sample);
        }

        sorted = false;
    }

    void write(vectorized::BufferWritable& buf) const {
        buf.write_binary(sample_count);
        buf.write_binary(total_values);

        std::stringstream rng_buf;
        rng_buf << rng;
        buf.write_binary(rng_buf.str());

        for (size_t i = 0; i < std::min(sample_count, total_values); ++i) {
            buf.write_binary(samples[i]);
        }
    }

private:
    /// We allocate a little memory on the stack - to avoid allocations when there are many objects with a small number of elements.
    using Array = vectorized::PODArrayWithStackMemory<double, 64>;
    using pcg32_fast = mcg_base<uint32_t, uint64_t, xsh_rs_mixin>;

    size_t sample_count;
    size_t total_values = 0;
    Array samples;
    pcg32_fast rng;
    bool sorted = false;

    uint64_t gen_random(uint64_t limit) {
        /// With a large number of values, we will generate random numbers several times slower.
        if (limit <= static_cast<uint64_t>(pcg32_fast::max())) {
            return rng() % limit;
        } else {
            return (static_cast<uint64_t>(rng()) *
                            (static_cast<uint64_t>(pcg32_fast::max()) + 1ULL) +
                    static_cast<uint64_t>(rng())) %
                   limit;
        }
    }

    void sortIfNeeded() {
        if (sorted) {
            return;
        }
        sorted = true;
        std::sort(samples.begin(), samples.end(), std::less<double>());
    }
};

} // namespace doris
