
#include "runtime/workload_management/workload_condition.h"

#include <gtest/gtest.h>

namespace doris {

class WorkloadConditionTest : public testing::Test {
public:
    WorkloadConditionTest() = default;
    ~WorkloadConditionTest() override = default;
};

TEST_F(WorkloadConditionTest, TestUsernameCondition) {
    // 1. Equal
    {
        WorkloadConditionUsername cond(WorkloadCompareOperator::EQUAL, "test_user");
        EXPECT_TRUE(cond.eval("test_user"));
        EXPECT_FALSE(cond.eval("other_user"));
        EXPECT_EQ(WorkloadMetricType::USERNAME, cond.get_workload_metric_type());
    }

    // 2. Unsupported Operator (GREATER)
    {
        WorkloadConditionUsername cond(WorkloadCompareOperator::GREATER, "a_user");
        EXPECT_FALSE(cond.eval("b_user")); // Not supported, returns false
    }
}

} // namespace doris
