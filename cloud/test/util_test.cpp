
#include <string>
#include <tuple>
#include <vector>

#include "common/config.h"
#include "common/string_util.h"
#include "glog/logging.h"
#include "gtest/gtest.h"

int main(int argc, char** argv) {
    doris::cloud::config::init(nullptr, true);
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

TEST(StringUtilTest, test_string_strip) {
    // clang-format off
    //                       str         expect          to_drop
    std::vector<std::tuple<std::string, std::string, std::string>> leading_inputs {
        {""       , ""      , ""    },
        {""       , ""      , "/"   },
        {"/"      , ""      , "/"   },
        {"\t////" , ""      , "/ \t"},
        {"/a///"  , "a///"  , "/"   },
        {"/a/b/c/", "a/b/c/", "/"   },
        {"a/b/c/" , "a/b/c/", "/"   },
        {"a/b/c/" , "/b/c/" , "a"   },
    };
    int idx = 0;
    for (auto&& i : leading_inputs) {
        doris::cloud::strip_leading(std::get<0>(i), std::get<2>(i));
        EXPECT_EQ(std::get<0>(i), std::get<1>(i)) << " index=" << idx;
        ++idx;
    }

    idx = 0;
    std::vector<std::tuple<std::string, std::string, std::string>> trailing_inputs {
        {""       , ""      , ""    },
        {"/"      , ""      , "/"   },
        {"////\t" , ""      , "/ \t"},
        {"/a///"  , "/a"    ,  "/"  },
        {"/a/b/c/", "/a/b/c", "/"   },
        {"a/b/c/" , "a/b/c" , "/"   },
        {"a/b/c"  , "a/b/c" , "/"   },
        {"a/b/c"  , "a/b/"  , "c"   },
    };
    for (auto&& i : trailing_inputs) {
        doris::cloud::strip_trailing(std::get<0>(i), std::get<2>(i));
        EXPECT_EQ(std::get<0>(i), std::get<1>(i)) << " index=" << idx;
        ++idx;
    }

    idx = 0;
    std::vector<std::tuple<std::string, std::string, std::string>> trim_inputs {
        {""              , ""      , ""     },
        {""              , ""      , "/"    },
        {"/"             , ""      , "/"    },
        {"\t////"        , ""      , "/ \t" },
        {"/a///"         , "a///"  , "/"    },
        {"/a/b/c/"       , "a/b/c/", "/"    },
        {"a/b/c/"        , "a/b/c/", "/"    },
        {"a/b/c"         , ""      , "/ \t" },
        {"\t/bbc///"     , "bbc"   , "/ \t" },
        {"\t////"        , "\t////", "abc"  },
        {"abc"           , ""      , "abc"  },
        {"\t  /a/b/c \t/", "a/b/c" , " \t /"},
    };
    for (auto&& i : leading_inputs) {
        doris::cloud::strip_leading(std::get<0>(i), std::get<2>(i));
        EXPECT_EQ(std::get<0>(i), std::get<1>(i)) << " index=" << idx;
        ++idx;
    }

    // clang-format on
}
