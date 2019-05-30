#include <gtest/gtest.h>
#include <olap/field_info.h>

namespace doris {
TEST(FieldInfoTest, HandleNoneZeroInput){
    int64_t a_integer = 9223372036854775806;
    int a_fraction = 1;
    decimal12_t a (a_integer, a_fraction);
    decimal12_t b (1,0);
    a +=b;
    ASSERT_EQ(a_integer+1, a.integer);
    ASSERT_EQ(a_fraction, a.fraction);
    
    a.integer = -9223372036854775807;
    a.fraction = -1;
    b.integer = 0;
    a += b;
    ASSERT_EQ(-9223372036854775807, a.integer);
    ASSERT_EQ(-1, a.fraction);
}
}

int main(int argc, char **argv) {
    testing::InitGoogleTest(&argc, argv); 
    return RUN_ALL_TESTS();
}
