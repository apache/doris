#include <gtest/gtest.h>
#include <olap/field_info.h>

TEST_F(){
    decimal12_t a = decimal12_t(INT_MAX + 1, INT_MAX);
    decimal12_t b = decimal12_t(0,0);

    ASSERT_EQ(a, a+=b);
    delete 
}

int main(int argc, char **argv) {
    testing::InitGoogleTest(&argc, argv); 
    return RUN_ALL_TESTS();
}