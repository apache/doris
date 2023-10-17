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

#include <memory>
#include <string>
#include <utility>

#include "common/status.h"
#include "runtime/large_int_value.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/columns_number.h"
#include "vec/common/hash_table/hash.h"
#include "vec/common/sip_hash.h"
#include "vec/common/uint128.h"
#include "vec/core/block.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/function.h"
#include "vec/functions/simple_function_factory.h"

namespace doris {
class FunctionContext;
} // namespace doris

namespace doris::vectorized {

// NOTE:
// The implementatin of random generator is inspired by the RandImpl::execute of ClickHouse.
// The ClickHouse RandImpl::execute function provided valuable insights and ideas for the development process.

struct LinearCongruentialGenerator {
    /// Constants from `man lrand48_r`.
    static constexpr UInt64 a = 0x5DEECE66D;
    static constexpr UInt64 c = 0xB;

    /// And this is from `head -c8 /dev/urandom | xxd -p`
    UInt64 current = 0xbcabbed75dfe77cdLL;

    void seed(UInt64 value) { current = value; }

    UInt32 next() {
        current = current * a + c;
        return static_cast<UInt32>(current >> 16);
    }
};

UInt64 calcSeed(UInt64 rand_seed, UInt64 additional_seed) {
    return int_hash64(rand_seed ^ int_hash64(additional_seed));
}

void seed(LinearCongruentialGenerator& generator, UInt64 rand_seed, intptr_t additional_seed) {
    generator.seed(calcSeed(rand_seed, additional_seed));
}

/// The array of random numbers from 'head -c8 /dev/urandom | xxd -p'.
/// Can be used for creating seeds for random generators.
constexpr std::array<UInt64, 32> random_numbers = {
        0x62224b4e764e1560ULL, 0xa79ec6fdbb2ef873ULL, 0xe2862f147d1c0649ULL, 0xc8d47f9a38554cb2ULL,
        0x62b0dd532dcd8a43ULL, 0xef3128a01e7a28bcULL, 0x32e4eb5461fc0f6ULL,  0xd3377ce32d3d9579ULL,
        0x6f129aa32529a57cULL, 0x98dd0ba25301a5a3ULL, 0x457bd29769afabf1ULL, 0x3bb886ea86263d9dULL,
        0xec3e9514dc0bb543ULL, 0x84282031a89ce23eULL, 0x55212b07d1a9a765ULL, 0xe9de69f882aa48afULL,
        0x13a71c9baa9babbbULL, 0x3b7be8b0dd9cb586ULL, 0x1375e8cb773f3e35ULL, 0x9f841693b13e615fULL,
        0xab62458b90fd9aefULL, 0xa9d9fdd187f8e941ULL, 0xca1851150f831eeaULL, 0xa43f586f9078e918ULL,
        0xe336c2883038a257ULL, 0xfebaffc035561545ULL, 0x27c2436d2607840eULL, 0x21bab1489b0ff552ULL,
        0x22ca273c2756bb6cULL, 0x4b6260e129af35f1ULL, 0xeb42b6c0d4322c6fULL, 0xfea0f49cc4e68339ULL,
};

class UuidNumeric : public IFunction {
public:
    static constexpr auto name = "uuid_numeric";
    static constexpr size_t uuid_length = 16; // Int128

    static FunctionPtr create() { return std::make_shared<UuidNumeric>(); }

    String get_name() const override { return name; }

    bool use_default_implementation_for_constants() const override { return false; }

    size_t get_number_of_arguments() const override { return 0; }

    bool is_variadic() const override { return false; }

    // uuid numeric is a Int128 (maybe UInt128 is better but we do not support it now
    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeInt128>();
    }

    // TODO(zhiqiang): May be override open function?

    Status execute_impl(FunctionContext* /*context*/, Block& block,
                        const ColumnNumbers& /*arguments*/, size_t result,
                        size_t input_rows_count) override {
        auto col_res = ColumnInt128::create();
        col_res->resize(input_rows_count);

        GenerateUUIDs(reinterpret_cast<char*>(col_res->get_data().data()),
                      uuid_length * input_rows_count);

        block.replace_by_position(result, std::move(col_res));
        return Status::OK();
    }

private:
    void GenerateUUIDs(char* output, size_t size) {
        LinearCongruentialGenerator generator0;
        LinearCongruentialGenerator generator1;
        LinearCongruentialGenerator generator2;
        LinearCongruentialGenerator generator3;

        UInt64 rand_seed = randomSeed();

        seed(generator0, rand_seed, random_numbers[0] + reinterpret_cast<intptr_t>(output));
        seed(generator1, rand_seed, random_numbers[1] + reinterpret_cast<intptr_t>(output));
        seed(generator2, rand_seed, random_numbers[2] + reinterpret_cast<intptr_t>(output));
        seed(generator3, rand_seed, random_numbers[3] + reinterpret_cast<intptr_t>(output));

        for (const char* end = output + size; output < end; output += 16) {
            unaligned_store<UInt32>(output, generator0.next());
            unaligned_store<UInt32>(output + 4, generator1.next());
            unaligned_store<UInt32>(output + 8, generator2.next());
            unaligned_store<UInt32>(output + 12, generator3.next());
        }
        /// It is guaranteed (by PaddedPODArray) that we can overwrite up to 15 bytes after end.
    }

    UInt64 randomSeed() const {
        struct timespec times {};

        /// Not cryptographically secure as time, pid and stack address can be predictable.

        SipHash hash;
        hash.update(times.tv_nsec);
        hash.update(times.tv_sec);
        hash.update((uintptr_t)pthread_self());

        return hash.get64();
    }
};

void register_function_uuid_numeric(SimpleFunctionFactory& factory) {
    factory.register_function<UuidNumeric>();
}

} // namespace doris::vectorized
