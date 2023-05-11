#include "vec/common/uint128.h"
#include "vec/core/field.h"

#include "gutil/integral_types.h"

#include <gen_cpp/segment_v2.pb.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <climits>
#include <cstdint>

#include "common/config.h"
#include "gen_cpp/data.pb.h"
#include "gtest/gtest_pred_impl.h"
#include "vec/core/types.h"

namespace doris::vectorized {

// Null
TEST(FieldTest, Test_Field_Null) {
    Field field_Null;
    EXPECT_EQ("Null", field_Null.get_type_name());
}

// UInt64
TEST(FieldTest, Test_Field_UInt64) {
    constexpr UInt64 a = -114514;
    
    Field field_UInt64(a);
    UInt64 b = get<UInt64>(field_UInt64);
    
    EXPECT_EQ("UInt64", field_UInt64.get_type_name());
    EXPECT_EQ(a, b);
}

// UInt128
TEST(FieldTest, Test_Field_UInt128) {
    constexpr unsigned __int128 a = -114514;

    UInt128 val((uint64_t)a, (uint64_t)(a >> 64));

    Field field_UInt128(val);
    UInt128 b = get<UInt128>(field_UInt128);
    
    auto U128_to_string=[](unsigned __int128 rhs) -> String {
        std::stringstream ss;
        ss << std::setw(16) << std::setfill('0') << std::hex << (uint64_t)(rhs >> 64) << (uint64_t)rhs;
        return String(ss.str());
    };

    EXPECT_EQ("UInt128", field_UInt128.get_type_name());
    EXPECT_EQ(U128_to_string(a), b.to_hex_string());
    EXPECT_EQ(val, b);
}

// Int64
TEST(FieldTest, Test_Field_Int64) {
    constexpr Int64 a = LLONG_MAX - 114514;
    
    Field field_Int64(a);
    Int64 b = get<Int64>(field_Int64);
    
    EXPECT_EQ("Int64", field_Int64.get_type_name());
    EXPECT_EQ(a, b);
}

// Int128
TEST(FieldTest, Test_Field_Int128) {
    Int128 a = (1ULL << 63);
    a = a * a + 114514;
    
    Field field_Int128(a);
    Int128 b = get<Int128>(field_Int128);
    
    EXPECT_EQ("Int128", field_Int128.get_type_name());
    EXPECT_EQ(a, b);
}

// Float64
TEST(FieldTest, Test_Field_Float64) {
    constexpr Float64 a = 1.2e154;
    
    Field field_Float64(a);
    Float64 b = get<Float64>(field_Float64);

    EXPECT_EQ("Float64", field_Float64.get_type_name());
    EXPECT_EQ(a, b);
}

// String
TEST(FieldTest, Test_Field_String) {
    const String a(("114514"));
    
    Field field_String(a);
    String b = get<String>(field_String);

    EXPECT_EQ("String", field_String.get_type_name());
    EXPECT_EQ(a, b);
}

// Decimal32
TEST(FieldTest, Test_Field_Decimal32) {
    constexpr Int32 val1 = 114514, val2 = 554514;
    const String str_refer("114.514");
    DecimalField<Decimal32> a(val1, UInt32(3));
    DecimalField<Decimal32> aa(val2, UInt32(3));
    
    Field field_Decimal32(a);
    DecimalField<Decimal32> b = get<DecimalField<Decimal32>>(field_Decimal32);

    EXPECT_EQ("Decimal32", field_Decimal32.get_type_name());
    EXPECT_EQ(Int32(114514), a.get_value());
    EXPECT_EQ(UInt32(3), a.get_scale());
    EXPECT_EQ(a, b);
    EXPECT_LE(a, aa);
    EXPECT_EQ(str_refer, b.get_value().to_string(b.get_scale()));
}

/*
    If not the same scale, add 0 at the end of the number is required, 
    which will cause overflow.

    So not the same scale belong to illegal case, 
    need to ensure the same scale to compare the size relationship.
*/
// Decimal64
TEST(FieldTest, Test_Field_Decimal64) {
    constexpr Int64 val1 = 114514000000000000, val2 = 554514000000000000;
    const String str_refer("11451.4000000000000");
    DecimalField<Decimal64> a(val1, UInt32(13));
    DecimalField<Decimal64> aa(val2, UInt32(13));
    
    Field field_Decimal64(a);
    DecimalField<Decimal64> b = get<DecimalField<Decimal64>>(field_Decimal64);

    EXPECT_EQ("Decimal64", field_Decimal64.get_type_name());
    EXPECT_EQ(val1, a.get_value());
    EXPECT_EQ(UInt32(13), a.get_scale());
    EXPECT_EQ(a, b);
    EXPECT_LE(a, aa); // If the scale is not guaranteed to be the same, overflow may occur here
    EXPECT_EQ(str_refer, b.get_value().to_string(b.get_scale()));
}

// Decimal128
TEST(FieldTest, Test_Field_Decimal128) {
    constexpr Int128 val1(114514 * Int128(1000000000000) * Int128(10000000000000));
    constexpr Int128 val2(554514 * Int128(1000000000000) * Int128(10000000000000));
    const String str_refer("11451.40000000000000000000000000");
    DecimalField<Decimal128> a(val1, UInt32(26));
    DecimalField<Decimal128> aa(val2, UInt32(26));
    
    Field field_Decimal128(a);
    DecimalField<Decimal128> b = get<DecimalField<Decimal128>>(field_Decimal128);
    
    EXPECT_EQ("Decimal128", field_Decimal128.get_type_name());
    EXPECT_EQ(val1, a.get_value());
    EXPECT_EQ(UInt32(26), a.get_scale());
    EXPECT_EQ(a, b);
    EXPECT_LE(a, aa);
    EXPECT_EQ(str_refer, b.get_value().to_string(b.get_scale()));
}

// Decimal128I
TEST(FieldTest, Test_Field_Decimal128I) {
    constexpr Int128 val1(114514 * Int128(1000000000000) * Int128(10000000000000));
    constexpr Int128 val2(554514 * Int128(1000000000000) * Int128(10000000000000));
    const String str_refer("11451.40000000000000000000000000");
    DecimalField<Decimal128I> a(val1, UInt32(26));
    DecimalField<Decimal128I> aa(val2, UInt32(26));
    
    Field field_Decimal128I(a);
    DecimalField<Decimal128I> b = get<DecimalField<Decimal128I>>(field_Decimal128I);
    
    EXPECT_EQ("Decimal128I", field_Decimal128I.get_type_name());
    EXPECT_EQ(val1, a.get_value());
    EXPECT_EQ(UInt32(26), a.get_scale());
    EXPECT_EQ(a, b);
    EXPECT_LE(a, aa);
    EXPECT_EQ(str_refer, b.get_value().to_string(b.get_scale()));
}

// Date
TEST(FieldTest, Test_Field_Date) {
    constexpr Date val1(114514);
    Field a(val1);
    
    Field field_Date(a);
    auto b = get<Date>(field_Date);
    
    EXPECT_EQ(a, b);
}

// DateV2
TEST(FieldTest, Test_Field_DateV2) {
    constexpr DateV2 val1(114514);
    Field a(val1);
    
    Field field_DateV2(a);
    auto b = get<DateV2>(field_DateV2);
    
    EXPECT_EQ(a, b);
}

// DateTime
TEST(FieldTest, Test_Field_DateTime) {
    constexpr DateTime val1(114514);
    Field a(val1);
    
    Field field_DateTime(a);
    auto b = get<DateTime>(field_DateTime);
    
    EXPECT_EQ(a, b);
}

// DateTimeV2
TEST(FieldTest, Test_Field_DateTimeV2) {
    constexpr DateTimeV2 val1(114514);
    Field a(val1);
    
    Field field_DateTimeV2(a);
    auto b = get<DateTimeV2>(field_DateTimeV2);
    
    EXPECT_EQ(a, b);
}

}