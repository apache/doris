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

#include <gtest/gtest.h>

#include "lance/lance_test_util.h"

// LanceIndexWorkerContractTest: the schema-contract binding model, the
// format-to-canonical mapping table, the guardrail negatives, and the
// trusted-unequal (STALE) vs recompute-untrusted (UNSUPPORTED) boundary.
// Everything here drives the public contract seams: parse_schema_contract,
// recompute_contract, vector_index_shape_supported, and the
// canonical_type_for_format mapping, against real pinned lance datasets shared
// with the FE golden test (amendment G11: a missing fixture fails, never skips).

namespace doris::lance {
namespace {

// The locked cross-language fixture inventory, mirrored by
// LanceSchemaContractGoldenTest on the FE side.
constexpr const char* GOLDEN_FIXTURES[] = {
        "flat_s1_a",
        "flat_s1_b",
        "flat_s2_c",
        "struct_s5_s",
        "struct_s5_b",
        "nested_struct_s18_b",
        "list_s19_l",
        "fsl_f32_s14_v",
        "fsl_nullable_elem_s15_v",
        "fsl_added_s16_v",
        "fsl_f16_s22_v",
        "struct_fsl_mix_r6_v",
        "drop_hole_s11_c",
        "drop_add_s21_d",
        "restore_add_s20_d",
        "restore_pinned_s20_v3_b",
        "rename_s12_b2",
        "map_m1_m",
        "largelist_m2_ll",
        "list_struct_m3_l",
        "scalars_bool",
        "scalars_int8",
        "scalars_int16",
        "scalars_int32",
        "scalars_int64",
        "scalars_uint8",
        "scalars_uint16",
        "scalars_uint32",
        "scalars_uint64",
        "scalars_float16",
        "scalars_float32",
        "scalars_float64",
        "scalars_utf8",
        "scalars_large_utf8",
        "scalars_date32",
        "scalars_date64",
        "decimal128",
        "decimal256",
        "decimal_default_bw",
        "timestamp_sec_nozone",
        "timestamp_ms_utc",
        "timestamp_us_shanghai",
        "timestamp_ns_la",
        "binary",
        "largebinary",
        "fixedsizebinary",
        "time32_sec",
        "time32_ms",
        "time64_us",
        "time64_ns",
        "duration_sec",
        "duration_ms",
        "duration_us",
        "duration_ns",
        "null_column",
        "empty_dataset_a",
        "fsl_uint8_unsupported",
        "fsl_int8_unsupported",
        "nonascii_column_unsupported",
        "fold_duplicate_unsupported",
};

std::optional<std::string> canonical(const char* format, int64_t flags = 0) {
    return canonical_type_for_format(format, flags);
}

// Every golden fixture: pinned open via lance-c, recompute through the contract
// header, verdict-driven assertion. "compare" fixtures must reproduce the FE
// contract slot-for-slot (the cross-language lock, including the corrected
// statistics count arithmetic over fixed-size lists); "unsupported" fixtures
// must fail closed.
TEST(LanceIndexWorkerContractTest, GoldenFixturesRecomputeMatchesTheFeLock) {
    for (const char* name : GOLDEN_FIXTURES) {
        SCOPED_TRACE(name);
        GoldenFixture fixture = load_golden_fixture(name);
        LanceDatasetPtr dataset = open_pinned_dataset(fixture.dataset_dir,
                                                      static_cast<uint64_t>(fixture.version));
        ASSERT_NE(dataset, nullptr)
                << "pinned open failed for " << fixture.dataset_dir << " v" << fixture.version;
        ASSERT_EQ(lance_dataset_version(dataset.get()),
                  static_cast<uint64_t>(fixture.version));

        SchemaContract recomputed;
        ContractStatus status = recompute_contract(dataset.get(), fixture.column, &recomputed);
        if (fixture.verdict == "compare") {
            ASSERT_EQ(status, ContractStatus::OK);
            ASSERT_EQ(recomputed.flds.size(), 1U);
            EXPECT_TRUE(recomputed == fixture.expected)
                    << "recompute drifted from the FE golden contract";
            // The parsed expected contract compares equal to itself (operator==
            // sanity for the STALE boundary below).
            EXPECT_TRUE(fixture.expected == fixture.expected);
        } else if (fixture.verdict == "unsupported") {
            EXPECT_EQ(status, ContractStatus::UNSUPPORTED)
                    << "shape the worker must refuse was recomputed successfully";
        } else {
            FAIL() << "unknown verdict in golden fixture: " << fixture.verdict;
        }
    }
}

// The format-to-canonical mapping table (recon §2.4), row by row, byte-exact —
// including the generic-fallback value grammar and the fail-closed rows that no
// creatable dataset reaches.
TEST(LanceIndexWorkerContractTest, CanonicalMappingCoversEveryProvenRow) {
    // Canonical scalar rows.
    EXPECT_EQ(canonical("b"), "bool");
    EXPECT_EQ(canonical("c"), "int<8>");
    EXPECT_EQ(canonical("s"), "int<16>");
    EXPECT_EQ(canonical("i"), "int<32>");
    EXPECT_EQ(canonical("l"), "int<64>");
    EXPECT_EQ(canonical("C"), "uint<8>");
    EXPECT_EQ(canonical("S"), "uint<16>");
    EXPECT_EQ(canonical("I"), "uint<32>");
    EXPECT_EQ(canonical("L"), "uint<64>");
    EXPECT_EQ(canonical("e"), "float16");
    EXPECT_EQ(canonical("f"), "float32");
    EXPECT_EQ(canonical("g"), "float64");
    EXPECT_EQ(canonical("u"), "utf8");
    EXPECT_EQ(canonical("U"), "large_utf8");
    EXPECT_EQ(canonical("tdD"), "date<day>");
    EXPECT_EQ(canonical("tdm"), "date<ms>");

    // Generic fallback: empty parameter lists render as name().
    EXPECT_EQ(canonical("z"), "binary()");
    EXPECT_EQ(canonical("Z"), "largebinary()");
    EXPECT_EQ(canonical("+l"), "list()");
    EXPECT_EQ(canonical("+L"), "largelist()");
    EXPECT_EQ(canonical("+s"), "struct()");
    EXPECT_EQ(canonical("n"), "null()");

    // Map carries the keysSorted flag bit (boolean rendering true/false).
    constexpr int64_t ARROW_FLAG_MAP_KEYS_SORTED = 4;
    EXPECT_EQ(canonical("+m"), "map(keysSorted=false)");
    EXPECT_EQ(canonical("+m", ARROW_FLAG_MAP_KEYS_SORTED), "map(keysSorted=true)");

    // Time and duration: enum units render Java-uppercased.
    EXPECT_EQ(canonical("tts"), "time(unit=SECOND,bitWidth=32)");
    EXPECT_EQ(canonical("ttm"), "time(unit=MILLISECOND,bitWidth=32)");
    EXPECT_EQ(canonical("ttu"), "time(unit=MICROSECOND,bitWidth=64)");
    EXPECT_EQ(canonical("ttn"), "time(unit=NANOSECOND,bitWidth=64)");
    EXPECT_EQ(canonical("tDs"), "duration(unit=SECOND)");
    EXPECT_EQ(canonical("tDm"), "duration(unit=MILLISECOND)");
    EXPECT_EQ(canonical("tDu"), "duration(unit=MICROSECOND)");
    EXPECT_EQ(canonical("tDn"), "duration(unit=NANOSECOND)");

    // Fixed-size binary carries its byte width.
    EXPECT_EQ(canonical("w:7"), "fixedsizebinary(byteWidth=7)");
    EXPECT_EQ(canonical("w:1024"), "fixedsizebinary(byteWidth=1024)");
    EXPECT_FALSE(canonical("w:").has_value());
    EXPECT_FALSE(canonical("w:x").has_value());

    // The fixed-size list canonical form is the pinned literal; the dimension
    // must be a positive int32.
    EXPECT_EQ(canonical("+w:4"), "fixed_size_list");
    EXPECT_EQ(canonical("+w:768"), "fixed_size_list");
    EXPECT_FALSE(canonical("+w:0").has_value());   // fsd <= 0 fails closed
    EXPECT_FALSE(canonical("+w:-3").has_value());
    EXPECT_FALSE(canonical("+w:").has_value());
    EXPECT_FALSE(canonical("+w:abc").has_value());
    EXPECT_FALSE(canonical("+w:2147483648").has_value()); // beyond int32

    // Decimal: the bit width defaults to 128 when the format omits it.
    EXPECT_EQ(canonical("d:10,2"), "decimal<128>(10,2)");
    EXPECT_EQ(canonical("d:10,2,128"), "decimal<128>(10,2)");
    EXPECT_EQ(canonical("d:38,8,256"), "decimal<256>(38,8)");
    EXPECT_EQ(canonical("d:1,0"), "decimal<128>(1,0)");
    EXPECT_FALSE(canonical("d:10").has_value());
    EXPECT_FALSE(canonical("d:x,2").has_value());
    EXPECT_FALSE(canonical("d:10,x").has_value());
    EXPECT_FALSE(canonical("d:10,2,x").has_value());
    EXPECT_FALSE(canonical("d:").has_value());

    // Timestamp: the timezone-less manifest placeholder "-" (exported by
    // arrow-rs as the empty string) is restored in the canonical form; an
    // IANA-safe zone stays canonical; anything else degrades to the generic
    // form with the raw zone.
    EXPECT_EQ(canonical("tss:"), "timestamp<sec,tz=\"-\">");
    EXPECT_EQ(canonical("tss:-"), "timestamp<sec,tz=\"-\">");
    EXPECT_EQ(canonical("tsm:UTC"), "timestamp<ms,tz=\"UTC\">");
    EXPECT_EQ(canonical("tsu:Asia/Shanghai"), "timestamp<us,tz=\"Asia/Shanghai\">");
    EXPECT_EQ(canonical("tsn:America/Los_Angeles"),
              "timestamp<ns,tz=\"America/Los_Angeles\">");
    EXPECT_EQ(canonical("tss:Etc/GMT+8"), "timestamp<sec,tz=\"Etc/GMT+8\">");
    EXPECT_EQ(canonical("tss:+08:00"), "timestamp(unit=SECOND,timezone=+08:00)");
    EXPECT_EQ(canonical("tsm:bad,tz"), "timestamp(unit=MILLISECOND,timezone=bad,tz)");
    EXPECT_EQ(canonical("tsu:gt>"), "timestamp(unit=MICROSECOND,timezone=gt>)");
    EXPECT_EQ(canonical("tsx:UTC"), std::nullopt); // unknown unit fails closed
    EXPECT_FALSE(canonical("tsz:").has_value());
    EXPECT_FALSE(canonical("ts").has_value());

    // Fail-closed rows: formats outside the proven type set are unboundable.
    // Union formats are among them: the FE generic grammar's typeIds spacing
    // ("union(mode=Dense,typeIds=[0, 1])") has no C-side counterpart because the
    // worker refuses union columns outright (the FE-side rendering is locked by
    // LanceSchemaContractBuilderTest).
    EXPECT_FALSE(canonical("+us:0,1").has_value());
    EXPECT_FALSE(canonical("+ud:0,1").has_value());
    EXPECT_FALSE(canonical("tiM").has_value()); // interval: unproven
    EXPECT_FALSE(canonical("tin").has_value());
    EXPECT_FALSE(canonical("vz").has_value());  // utf8view: unproven
    EXPECT_FALSE(canonical("+vl").has_value()); // listview: unproven
    EXPECT_FALSE(canonical("").has_value());
    EXPECT_FALSE(canonical("x").has_value());
    EXPECT_FALSE(canonical("bogus").has_value());
    EXPECT_FALSE(canonical("+x").has_value());
    EXPECT_FALSE(canonical(nullptr).has_value());
}

// The FE contract JSON parser: Gson-form tolerance, the DROP legacy-record
// rejection, and every malformed-representation rail.
TEST(LanceIndexWorkerContractTest, ParseSchemaContractMatrix) {
    SchemaContract out;

    // The canonical single-field FSL contract in the dispatcher's wire form.
    const std::string fsl_wire =
            R"({"scv":1,"flds":[{"fid":3,"nn":"v","nt":"fixed_size_list","nul":false,"fsd":4,"vet":"float32","ven":true}]})";
    ASSERT_EQ(parse_schema_contract(fsl_wire, &out), ContractStatus::OK);
    ASSERT_EQ(out.flds.size(), 1U);
    EXPECT_EQ(out.flds[0].fid, 3);
    EXPECT_EQ(out.flds[0].nn, "v");
    EXPECT_EQ(out.flds[0].nt, "fixed_size_list");
    EXPECT_FALSE(out.flds[0].nul);
    ASSERT_TRUE(out.flds[0].fsd.has_value());
    EXPECT_EQ(*out.flds[0].fsd, 4);
    ASSERT_TRUE(out.flds[0].vet.has_value());
    EXPECT_EQ(*out.flds[0].vet, "float32");
    ASSERT_TRUE(out.flds[0].ven.has_value());
    EXPECT_TRUE(*out.flds[0].ven);

    // Gson key order is not guaranteed; the parser is order-tolerant.
    const std::string shuffled =
            R"({"flds":[{"nt":"int<64>","nn":"a","fid":0,"nul":true}],"scv":1})";
    ASSERT_EQ(parse_schema_contract(shuffled, &out), ContractStatus::OK);
    EXPECT_EQ(out.flds[0].fid, 0); // field id 0 is legal
    EXPECT_EQ(out.flds[0].nt, "int<64>");
    EXPECT_TRUE(out.flds[0].nul);
    EXPECT_FALSE(out.flds[0].fsd.has_value());
    EXPECT_FALSE(out.flds[0].vet.has_value());
    EXPECT_FALSE(out.flds[0].ven.has_value());

    // DROP legacy record: the empty payload is rejected safely, never skipped.
    EXPECT_EQ(parse_schema_contract("", &out), ContractStatus::UNSUPPORTED);

    // Deep nesting (~400KB inside the frame cap): the iterative parser rejects
    // it as a non-object payload — the default recursive descent would blow the
    // stack on FE-controlled wire data (review M7).
    {
        const size_t depth = 200 * 1024;
        std::string nested(depth, '[');
        nested.append(depth, ']');
        EXPECT_EQ(parse_schema_contract(nested, &out), ContractStatus::UNSUPPORTED);
    }

    // Malformed payloads.
    EXPECT_EQ(parse_schema_contract("not json", &out), ContractStatus::UNSUPPORTED);
    EXPECT_EQ(parse_schema_contract("[1,2]", &out), ContractStatus::UNSUPPORTED);
    EXPECT_EQ(parse_schema_contract("{}", &out), ContractStatus::UNSUPPORTED);
    EXPECT_EQ(parse_schema_contract(R"({"scv":2,"flds":[]})", &out),
              ContractStatus::UNSUPPORTED);
    EXPECT_EQ(parse_schema_contract(R"({"scv":"1","flds":[]})", &out),
              ContractStatus::UNSUPPORTED);
    EXPECT_EQ(parse_schema_contract(R"({"scv":1})", &out), ContractStatus::UNSUPPORTED);
    EXPECT_EQ(parse_schema_contract(R"({"scv":1,"flds":{}})", &out),
              ContractStatus::UNSUPPORTED);
    EXPECT_EQ(parse_schema_contract(R"({"scv":1,"flds":[null]})", &out),
              ContractStatus::UNSUPPORTED);
    // Field-level rails: every required slot must be present and well-formed.
    EXPECT_EQ(parse_schema_contract(R"({"scv":1,"flds":[{"nn":"a","nt":"int<64>","nul":true}]})",
                                    &out),
              ContractStatus::UNSUPPORTED); // missing fid
    EXPECT_EQ(parse_schema_contract(
                      R"({"scv":1,"flds":[{"fid":-1,"nn":"a","nt":"int<64>","nul":true}]})", &out),
              ContractStatus::UNSUPPORTED); // negative fid
    EXPECT_EQ(parse_schema_contract(
                      R"({"scv":1,"flds":[{"fid":0,"nt":"int<64>","nul":true}]})", &out),
              ContractStatus::UNSUPPORTED); // missing nn
    EXPECT_EQ(parse_schema_contract(
                      R"({"scv":1,"flds":[{"fid":0,"nn":"","nt":"int<64>","nul":true}]})", &out),
              ContractStatus::UNSUPPORTED); // empty nn
    EXPECT_EQ(parse_schema_contract(
                      R"({"scv":1,"flds":[{"fid":0,"nn":"a","nul":true}]})", &out),
              ContractStatus::UNSUPPORTED); // missing nt
    EXPECT_EQ(parse_schema_contract(
                      R"({"scv":1,"flds":[{"fid":0,"nn":"a","nt":"","nul":true}]})", &out),
              ContractStatus::UNSUPPORTED); // empty nt
    EXPECT_EQ(parse_schema_contract(
                      R"({"scv":1,"flds":[{"fid":0,"nn":"a","nt":"int<64>"}]})", &out),
              ContractStatus::UNSUPPORTED); // missing nul
    // The three vector slots tolerate ABSENCE (Gson drops nulls) but reject a
    // malformed presence.
    EXPECT_EQ(parse_schema_contract(
                      R"({"scv":1,"flds":[{"fid":0,"nn":"v","nt":"fixed_size_list","nul":false,"fsd":0}]})",
                      &out),
              ContractStatus::UNSUPPORTED); // fsd <= 0
    EXPECT_EQ(parse_schema_contract(
                      R"({"scv":1,"flds":[{"fid":0,"nn":"v","nt":"fixed_size_list","nul":false,"fsd":-3}]})",
                      &out),
              ContractStatus::UNSUPPORTED);
    EXPECT_EQ(parse_schema_contract(
                      R"({"scv":1,"flds":[{"fid":0,"nn":"v","nt":"fixed_size_list","nul":false,"fsd":null}]})",
                      &out),
              ContractStatus::UNSUPPORTED); // explicit null is a malformed presence
    EXPECT_EQ(parse_schema_contract(
                      R"({"scv":1,"flds":[{"fid":0,"nn":"v","nt":"fixed_size_list","nul":false,"vet":""}]})",
                      &out),
              ContractStatus::UNSUPPORTED); // empty vet
    EXPECT_EQ(parse_schema_contract(
                      R"({"scv":1,"flds":[{"fid":0,"nn":"v","nt":"fixed_size_list","nul":false,"ven":1}]})",
                      &out),
              ContractStatus::UNSUPPORTED); // non-bool ven
    // Representation bounds: 64 indexed fields, 1024-byte strings.
    {
        std::string too_many = R"({"scv":1,"flds":[)";
        for (int i = 0; i < 65; ++i) {
            if (i != 0) {
                too_many += ",";
            }
            too_many += R"({"fid":)" + std::to_string(i) +
                    R"(,"nn":"a","nt":"int<64>","nul":true})";
        }
        too_many += "]}";
        EXPECT_EQ(parse_schema_contract(too_many, &out), ContractStatus::UNSUPPORTED);
    }
    {
        std::string long_name(1025, 'x');
        std::string json = R"({"scv":1,"flds":[{"fid":0,"nn":")" + long_name +
                R"(","nt":"int<64>","nul":true}]})";
        EXPECT_EQ(parse_schema_contract(json, &out), ContractStatus::UNSUPPORTED);
    }
    // A nullptr sink is a provider-contract violation, never a parse verdict.
    EXPECT_EQ(parse_schema_contract(fsl_wire, nullptr), ContractStatus::NO_TRUSTED);
}

// Guardrail negatives that real datasets can reach: column resolution and the
// fragment caps. The binding-model rails no lance-c 0.1.9 dataset can violate
// (statistics count != walked nodes, non-ascending statistics ids, dictionary-
// encoded nodes, fixed-size-list child count != 1) are proven unreachable by
// the P1a evidence (35 pinned dumps, D13 counterexample shown unconstructible)
// and remain code-inspection territory; the corrected count arithmetic they
// depend on is pinned by the FSL golden fixtures above.
TEST(LanceIndexWorkerContractTest, RecomputeGuardrailNegatives) {
    // Column resolution: a name that is not a top-level field fails closed.
    {
        LanceDatasetPtr dataset = open_pinned_dataset("s1.lance", 1);
        ASSERT_NE(dataset, nullptr);
        SchemaContract out;
        EXPECT_EQ(recompute_contract(dataset.get(), "nosuch", &out),
                  ContractStatus::UNSUPPORTED);
        // Matching is byte-exact: a differently-cased name is a missing column.
        EXPECT_EQ(recompute_contract(dataset.get(), "A", &out),
                  ContractStatus::UNSUPPORTED);
    }
    // A non-ASCII requested name on a dataset without such a column is likewise
    // a closed miss (the fold-divergence case itself is covered by the
    // nonascii_col golden fixture).
    {
        LanceDatasetPtr dataset = open_pinned_dataset("s1.lance", 1);
        ASSERT_NE(dataset, nullptr);
        SchemaContract out;
        EXPECT_EQ(recompute_contract(dataset.get(), "\xe5\x90\x91\xe9\x87\x8f", &out),
                  ContractStatus::UNSUPPORTED);
    }
    // Fragment caps with tiny injected values (the production defaults would
    // need >4096 fragments to trip).
    {
        LanceDatasetPtr dataset = open_pinned_dataset("s1.lance", 1);
        ASSERT_NE(dataset, nullptr);
        SchemaContract out;
        force_recompute_fragment_caps_for_test(0, RECOMPUTE_MAX_STATS_FRAGMENT_FIELD_PRODUCT);
        EXPECT_EQ(recompute_contract(dataset.get(), "a", &out), ContractStatus::UNSUPPORTED)
                << "a fragment count above the injected cap must fail closed";
        force_recompute_fragment_caps_for_test(RECOMPUTE_MAX_STATS_FRAGMENTS, 1);
        EXPECT_EQ(recompute_contract(dataset.get(), "a", &out), ContractStatus::UNSUPPORTED)
                << "a fragment*field product above the injected cap must fail closed";
        force_recompute_fragment_caps_for_test(RECOMPUTE_MAX_STATS_FRAGMENTS,
                                               RECOMPUTE_MAX_STATS_FRAGMENT_FIELD_PRODUCT);
        EXPECT_EQ(recompute_contract(dataset.get(), "a", &out), ContractStatus::OK)
                << "restored caps must admit the small dataset again";
    }
}

// The trusted-unequal (STALE) vs recompute-untrusted (UNSUPPORTED) boundary the
// worker's step-9 branching maps to wire codes.
TEST(LanceIndexWorkerContractTest, StaleVsUnsupportedBoundary) {
    GoldenFixture fixture = load_golden_fixture("fsl_f32_s14_v");
    LanceDatasetPtr dataset = open_pinned_dataset(fixture.dataset_dir,
                                                  static_cast<uint64_t>(fixture.version));
    ASSERT_NE(dataset, nullptr);
    SchemaContract recomputed;
    ASSERT_EQ(recompute_contract(dataset.get(), fixture.column, &recomputed),
              ContractStatus::OK);

    // Trusted but unequal in any single slot: the admitted contract parses and
    // the recompute succeeds, yet they differ — the STALE_ADMISSION shape.
    auto expect_stale = [&fixture, &recomputed](SchemaContract admitted, const char* what) {
        SCOPED_TRACE(what);
        EXPECT_FALSE(admitted == recomputed);
        EXPECT_FALSE(admitted == fixture.expected);
    };
    {
        SchemaContract mutated = fixture.expected;
        mutated.flds[0].fid += 1;
        expect_stale(mutated, "fid off by one");
    }
    {
        SchemaContract mutated = fixture.expected;
        mutated.flds[0].fsd = *mutated.flds[0].fsd + 1;
        expect_stale(mutated, "fsd off by one");
    }
    {
        SchemaContract mutated = fixture.expected;
        mutated.flds[0].nn = "w";
        expect_stale(mutated, "normalized name differs");
    }
    {
        SchemaContract mutated = fixture.expected;
        mutated.flds[0].ven = !*mutated.flds[0].ven;
        expect_stale(mutated, "element nullability differs");
    }
    {
        SchemaContract mutated = fixture.expected;
        mutated.flds[0].vet = "float16";
        expect_stale(mutated, "element type differs");
    }
    // Recompute-untrusted: the admitted contract parses fine but the column is
    // gone from the pinned snapshot — the UNSUPPORTED shape, never STALE.
    {
        SchemaContract out;
        EXPECT_EQ(recompute_contract(dataset.get(), "missing_column", &out),
                  ContractStatus::UNSUPPORTED);
    }
    // A mutated admitted payload that no longer parses at all is likewise
    // UNSUPPORTED (the parse rail), not STALE.
    {
        const std::string broken =
                R"({"scv":1,"flds":[{"fid":0,"nn":"v","nt":"fixed_size_list","nul":false,"fsd":0}]})";
        SchemaContract out;
        EXPECT_EQ(parse_schema_contract(broken, &out), ContractStatus::UNSUPPORTED);
    }
}

// DROP contract semantics: a legacy record's empty payload is a safe rejection;
// a persisted DROP contract revalidates through the same parse/recompute path.
TEST(LanceIndexWorkerContractTest, DropContractSemantics) {
    SchemaContract out;
    EXPECT_EQ(parse_schema_contract("", &out), ContractStatus::UNSUPPORTED)
            << "empty DROP contract payload must be a safe rejection, never a skip";

    // A normal contract revalidates: parse the golden, recompute from the pinned
    // snapshot, and the two agree (the path a DROP with a persisted contract takes).
    GoldenFixture fixture = load_golden_fixture("flat_s1_a");
    LanceDatasetPtr dataset = open_pinned_dataset(fixture.dataset_dir,
                                                  static_cast<uint64_t>(fixture.version));
    ASSERT_NE(dataset, nullptr);
    ASSERT_EQ(recompute_contract(dataset.get(), fixture.column, &out), ContractStatus::OK);
    EXPECT_TRUE(out == fixture.expected);
}

// The vector-element product matrix at the build-shape rail: float16/float32
// pass, uint8/int8 refuse even though lance supports them natively, and
// num_sub_vectors must divide the dimension.
TEST(LanceIndexWorkerContractTest, VectorIndexShapeMatrix) {
    ContractField f32;
    f32.fid = 0;
    f32.nn = "v";
    f32.nt = "fixed_size_list";
    f32.nul = false;
    f32.fsd = 4;
    f32.vet = "float32";
    f32.ven = true;
    EXPECT_TRUE(vector_index_shape_supported(f32, 1));
    EXPECT_TRUE(vector_index_shape_supported(f32, 2));
    EXPECT_TRUE(vector_index_shape_supported(f32, 4));
    EXPECT_FALSE(vector_index_shape_supported(f32, 3))
            << "num_sub_vectors must divide the fixed-size-list dimension";
    EXPECT_FALSE(vector_index_shape_supported(f32, 0));

    ContractField f16 = f32;
    f16.vet = "float16";
    f16.fsd = 3;
    EXPECT_TRUE(vector_index_shape_supported(f16, 3));
    EXPECT_FALSE(vector_index_shape_supported(f16, 2));

    ContractField u8 = f32;
    u8.vet = "uint<8>";
    EXPECT_FALSE(vector_index_shape_supported(u8, 2));
    ContractField i8 = f32;
    i8.vet = "int<8>";
    EXPECT_FALSE(vector_index_shape_supported(i8, 2));

    ContractField nullable = f32;
    nullable.nul = true;
    EXPECT_FALSE(vector_index_shape_supported(nullable, 2));

    ContractField scalar = f32;
    scalar.nt = "int<64>";
    scalar.fsd.reset();
    scalar.vet.reset();
    scalar.ven.reset();
    EXPECT_FALSE(vector_index_shape_supported(scalar, 2));

    ContractField zero_dim = f32;
    zero_dim.fsd = 0;
    EXPECT_FALSE(vector_index_shape_supported(zero_dim, 2));
}

// External-metadata advancement: the worker's observation is exactly the
// pinned-handle latest_version call pair, tested here through the same FFI on a
// multi-version dataset (the full run_index_worker path can never reach it
// hermetically: every local dataset is rejected at the D11 file gate, and
// lance-c 0.1.9 has no hermetic non-local object store — memory:// state does
// not survive across FFI calls, and http:// has no provider).
TEST(LanceIndexWorkerContractTest, ExternalMetadataAdvancementObservation) {
    {
        LanceDatasetPtr pinned = open_pinned_dataset("s20.lance", 1);
        ASSERT_NE(pinned, nullptr);
        ASSERT_EQ(lance_dataset_version(pinned.get()), 1U);
        uint64_t latest = lance_dataset_latest_version(pinned.get());
        ASSERT_NE(latest, 0U) << "the observation must succeed on the pinned handle";
        EXPECT_GT(latest, 1U);
        // The worker's step-8 semantics on this evidence: advanced = latest > admitted.
        EXPECT_TRUE(latest > 1);
    }
    {
        LanceDatasetPtr pinned = open_pinned_dataset("s20.lance", 4);
        ASSERT_NE(pinned, nullptr);
        uint64_t latest = lance_dataset_latest_version(pinned.get());
        ASSERT_NE(latest, 0U);
        EXPECT_EQ(latest, 4U);
        EXPECT_FALSE(latest > 4);
    }
}

// The saved-error envelope discipline: code first, message copied and freed,
// and the TLS slot cleared by the read — on a real typed provider failure.
TEST(LanceIndexWorkerContractTest, SavedLanceErrorCapturesCodeFirstAndClears) {
    LanceDataset* raw = lance_dataset_open("http://127.0.0.1:1/nope", nullptr, 1);
    ASSERT_EQ(raw, nullptr);
    SavedLanceError saved = save_lance_error();
    EXPECT_EQ(saved.code, LANCE_ERR_INVALID_ARGUMENT)
            << "http:// has no object-store provider in lance-c 0.1.9";
    EXPECT_FALSE(saved.message.empty());
    // The slot was consumed: a second save sees no typed fact.
    SavedLanceError second = save_lance_error();
    EXPECT_EQ(second.code, LANCE_OK);
}

} // namespace
} // namespace doris::lance
