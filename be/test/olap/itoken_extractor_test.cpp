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

#include "olap/itoken_extractor.h"

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include <algorithm>
#include <string>
#include <vector>

#include "gtest/gtest_pred_impl.h"
#include "util/utf8_check.h"

namespace doris {

class TestITokenExtractor : public testing::Test {
public:
    void SetUp() {}
    void TearDown() {}
};

void runNextInString(const ITokenExtractor& extractor, std::string statement,
                     std::vector<std::string> expect) {
    ASSERT_TRUE(validate_utf8(statement.c_str(), statement.length()));

    std::vector<std::string> actual;
    actual.reserve(expect.size());
    size_t pos = 0;
    size_t token_start = 0;
    size_t token_length = 0;
    while (extractor.next_in_string(statement.c_str(), statement.size(), &pos, &token_start,
                                    &token_length)) {
        actual.push_back(statement.substr(token_start, token_length));
    }
    ASSERT_EQ(expect, actual);
}

void runNextInStringLike(const ITokenExtractor& extractor, std::string statement,
                         std::vector<std::string> expect) {
    std::vector<std::string> actual;
    actual.reserve(expect.size());
    size_t pos = 0;
    std::string str;
    while (extractor.next_in_string_like(statement.c_str(), statement.length(), &pos, str)) {
        actual.push_back(str);
    }
    ASSERT_EQ(expect, actual);
}

#if __cplusplus > 201703L
std::string from_u8string(const std::u8string& s) {
    return std::string(s.begin(), s.end());
}
#else
std::string from_u8string(const std::string& s) {
    return std::string(s.begin(), s.end());
}
#endif

TEST_F(TestITokenExtractor, ngram_extractor) {
    std::string statement = from_u8string(u8"预计09发布i13手机。");
    std::vector<std::string> expect = {
            from_u8string(u8"预计"), from_u8string(u8"计0"),  from_u8string(u8"09"),
            from_u8string(u8"9发"),  from_u8string(u8"发布"), from_u8string(u8"布i"),
            from_u8string(u8"i1"),   from_u8string(u8"13"),   from_u8string(u8"3手"),
            from_u8string(u8"手机"), from_u8string(u8"机。")};
    NgramTokenExtractor ngram_extractor(2);
    runNextInString(ngram_extractor, statement, expect);
}

TEST_F(TestITokenExtractor, ngram_like_extractor) {
    NgramTokenExtractor ngram_extractor(2);
    runNextInStringLike(ngram_extractor, from_u8string(u8"%手机%"), {from_u8string(u8"手机")});
    runNextInStringLike(ngram_extractor, from_u8string(u8"%机%"), {});
    runNextInStringLike(ngram_extractor, {from_u8string(u8"i_%手机%")}, {from_u8string(u8"手机")});
    runNextInStringLike(ngram_extractor, {from_u8string(u8"\\_手机%")},
                        {from_u8string(u8"_手"), from_u8string(u8"手机")});
}

TEST_F(TestITokenExtractor, ngram_extractor_empty_input) {
    // Test empty string input, expect no output
    std::string statement = "";
    std::vector<std::string> expect = {};
    NgramTokenExtractor ngram_extractor(2);
    runNextInString(ngram_extractor, statement, expect);
}

TEST_F(TestITokenExtractor, ngram_extractor_single_char) {
    // Only one character, less than n=2, should produce no tokens
    std::string statement = "a";
    std::vector<std::string> expect = {};
    NgramTokenExtractor ngram_extractor(2);
    runNextInString(ngram_extractor, statement, expect);
}

TEST_F(TestITokenExtractor, ngram_extractor_ascii_characters) {
    // Test token extraction for pure ASCII characters
    std::string statement = "abcd";
    // 2-gram tokens: "ab", "bc", "cd"
    std::vector<std::string> expect = {"ab", "bc", "cd"};
    NgramTokenExtractor ngram_extractor(2);
    runNextInString(ngram_extractor, statement, expect);
}

TEST_F(TestITokenExtractor, ngram_extractor_emoji) {
    // Test scenarios that include Emoji and other multi-byte UTF-8 characters
    // Assume n=2. Here "👍" is an emoji (4 bytes), "测" is a Chinese character (3 bytes).
    // String: "👍测A" (3 elements: 1 Emoji, 1 Chinese char, 1 ASCII)
    // For two code points per token:
    // First token: "👍测"
    // Second token: "测A"
    std::string statement = from_u8string(u8"👍测A");
    std::vector<std::string> expect = {from_u8string(u8"👍测"), from_u8string(u8"测A")};
    NgramTokenExtractor ngram_extractor(2);
    runNextInString(ngram_extractor, statement, expect);
}

TEST_F(TestITokenExtractor, ngram_extractor_n_greater_than_length) {
    // When n=3 and the string length is only 2, no 3-character Ngram can be formed
    std::string statement = "ab";
    std::vector<std::string> expect = {};
    NgramTokenExtractor ngram_extractor(3);
    runNextInString(ngram_extractor, statement, expect);
}

TEST_F(TestITokenExtractor, ngram_extractor_chinese_only) {
    // Test pure Chinese characters with multi-byte UTF-8 tokens
    // String: "中国人" (3 Chinese chars, each 3 bytes)
    // n=2, expected tokens: ["中国", "国人"]
    std::string statement = from_u8string(u8"中国人");
    std::vector<std::string> expect = {from_u8string(u8"中国"), from_u8string(u8"国人")};
    NgramTokenExtractor ngram_extractor(2);
    runNextInString(ngram_extractor, statement, expect);
}

TEST_F(TestITokenExtractor, ngram_extractor_mixed_width_characters) {
    // Mixed character widths: English (1 byte), Chinese (3 bytes), Emoji (4 bytes)
    // String: "A中👍B"
    // Code points: 'A'(1), '中'(1), '👍'(1), 'B'(1) total 4 code points
    // n=2 tokens: "A中", "中👍", "👍B"
    std::string statement = from_u8string(u8"A中👍B");
    std::vector<std::string> expect = {from_u8string(u8"A中"), from_u8string(u8"中👍"),
                                       from_u8string(u8"👍B")};
    NgramTokenExtractor ngram_extractor(2);
    runNextInString(ngram_extractor, statement, expect);
}

TEST_F(TestITokenExtractor, ngram_like_extractor_empty_input) {
    // Test empty input for like extraction
    std::string statement = "";
    std::vector<std::string> expect = {};
    NgramTokenExtractor ngram_extractor(2);
    runNextInStringLike(ngram_extractor, statement, expect);
}

TEST_F(TestITokenExtractor, ngram_like_extractor_no_pattern) {
    // No % or _, equivalent to extracting n-length sequences.
    // String: "abc", n=2, theoretically extract "ab", "bc"
    // next_in_string_like requires n code points to return a token.
    // Without % or _, it should still extract normally.
    std::string statement = "abc";
    // n=2: extract "ab", then "bc"
    std::vector<std::string> expect = {"ab", "bc"};
    NgramTokenExtractor ngram_extractor(2);
    runNextInStringLike(ngram_extractor, statement, expect);
}

TEST_F(TestITokenExtractor, ngram_like_extractor_pattern1) {
    // No % or _, equivalent to extracting n-length sequences.
    // String: "abc", n=2, theoretically extract "ab", "bc"
    // next_in_string_like requires n code points to return a token.
    // Without % or _, it should still extract normally.
    std::string statement = "%abc%def%gh%";
    // n=2: extract "ab", then "bc"
    std::vector<std::string> expect = {"ab", "bc", "de", "ef", "gh"};
    NgramTokenExtractor ngram_extractor(2);
    runNextInStringLike(ngram_extractor, statement, expect);
}

TEST_F(TestITokenExtractor, ngram_like_extractor_patterns_only) {
    // String has only '%' and '_', no normal chars to form a 2-gram
    // "%__%", n=2: % and _ are not considered normal token characters
    // Each encounter of % resets the token, so no tokens are generated
    std::string statement = "%__%";
    std::vector<std::string> expect = {};
    NgramTokenExtractor ngram_extractor(2);
    runNextInStringLike(ngram_extractor, statement, expect);
}

TEST_F(TestITokenExtractor, ngram_like_extractor_escaped_characters) {
    // Test scenarios with escape characters: "\\%abc% \\_xyz_"
    // Escaped '%' should be treated as a normal character, similarly for '_'
    // Suppose n=2, for "\\%abc%":
    // Initially encounter '\\%' => escaped '%', include it in token: "%a"
    // Then 'a'(1 byte) 'b'(1 byte) form "ab", 'c'(1 byte) continues...
    // A bit complex example, mainly to demonstrate properly handling escaped chars.
    std::string statement = from_u8string(u8"\\%手机% \\_人_");
    // Analysis:
    // "\\%" -> escaped '%', token gets "%"
    // then "手"(1 code point), "机"(1 code point). Once 2 code points are formed, we have "%手"
    // Move pos. Next token starts from "机":
    // '机'(1 code point)
    // Next is '%', encountering '%', reset token, skip over ' '...
    // Next segment: "\\_人_"
    // "\\_" => escaped '_', token gets "_"
    // '人'(1 code point) + '_' pattern encountered resets token after outputting "_人"
    // Final result: {"%手", "_人"}
    // Note: Based on logic, pattern chars % and _ reset the token. After a token is output,
    // encountering % or _ resets the token to empty, not affecting previously output tokens.
    std::vector<std::string> expect = {"%手", "手机", " _", "_人"};
    NgramTokenExtractor ngram_extractor(2);
    runNextInStringLike(ngram_extractor, statement, expect);
}

TEST_F(TestITokenExtractor, ngram_like_extractor_complex_pattern) {
    // Complex scenario: "abc%中_\\%国%d"
    // n=2 analysis:
    // Start from the beginning: 'a'(1 code point), 'b'(1 code point) => "ab" output
    // Encounter 'c' then '%', at '%' reset token and move forward
    // Next: "中"(1 code point), '_' is pattern reset
    // Then "\\%" => '%'(1 code point), '国'(1 code point) => "%国" output
    // Encounter '%', reset token
    // Finally 'd' alone is not enough to form 2 code points, no output
    std::string statement = from_u8string(u8"abc%中_\\%国%d");
    std::vector<std::string> expect = {"ab", "bc", "%国"};
    NgramTokenExtractor ngram_extractor(2);
    runNextInStringLike(ngram_extractor, statement, expect);
}

TEST_F(TestITokenExtractor, ngram_extractor_different_n) {
    // Test different n values
    // String: "abcd"
    // n=3: extract "abc", "bcd"
    std::string statement = "abcd";
    std::vector<std::string> expect = {"abc", "bcd"};
    NgramTokenExtractor ngram_extractor(3);
    runNextInString(ngram_extractor, statement, expect);
}

std::string get_repetition_info(const std::string& text, size_t n) {
    NgramTokenExtractor ngram_extractor(n);
    std::vector<std::string> tokens;

    {
        size_t pos = 0;
        size_t token_start = 0;
        size_t token_length = 0;
        while (ngram_extractor.next_in_string(text.c_str(), text.size(), &pos, &token_start,
                                              &token_length)) {
            tokens.push_back(text.substr(token_start, token_length));
        }
    }

    std::unordered_map<std::string, int> token_count;
    for (auto& t : tokens) {
        token_count[t]++;
    }

    int total_tokens = static_cast<int>(tokens.size());
    int repeated_tokens = 0;
    for (auto& kv : token_count) {
        if (kv.second > 1) {
            repeated_tokens += kv.second;
        }
    }

    double repetition_rate = 0.0;
    if (total_tokens > 0) {
        repetition_rate = static_cast<double>(repeated_tokens) / total_tokens;
    }

    std::ostringstream oss;
    oss << "Total tokens: " << total_tokens << "\n"
        << "Repeated tokens: " << repeated_tokens << "\n"
        << "Repetition rate: " << repetition_rate << "\n";

    return oss.str();
}

TEST_F(TestITokenExtractor, ngram_extractor_repetition_rate_matchine_text) {
    std::string statement =
            "Exception=System.CannotUnloadAppDomain;\n"
            "HResult=0x00007486;\n"
            "Message=exception happened;\n"
            "Source=BenchmarkLogGenerator;\n"
            "StackTrace:\n"
            " at BenchmarkLogGenerator.Generator.Run(Int32 sizeFactor) in "
            "C:\\Src\\Tools\\BenchmarkLogGenerator\\Generator.cs:line 84\n"
            " at BenchmarkLogGenerator.Generator.<>c__DisplayClass26_0.<RunInBackground>b__0() in "
            "C:\\Src\\Tools\\BenchmarkLogGenerator\\Generator.cs:line 74\n"
            " at System.Threading.ThreadHelper.ThreadStart_Context(Object state)\n"
            " at System.Threading.ExecutionContext.RunInternal(ExecutionContext executionContext)\n"
            " at BenchmarkLogGenerator.Flows.BootFlow.GetLevel(Int64 v) in "
            "C:\\Src\\Tools\\BenchmarkLogGenerator\\Flows\\BootFlow.cs:line 85\n"
            " at BenchmarkLogGenerator.Flows.BootFlow.<IngestionSession>d__1.MoveNext() in "
            "C:\\Src\\Tools\\BenchmarkLogGenerator\\Flows\\BootFlow.cs:line 47\n"
            " at BenchmarkLogGenerator.Scheduler.Flow.NextStep() in "
            "C:\\Src\\Tools\\BenchmarkLogGenerator\\Scheduler.cs:line 74\n"
            " at BenchmarkLogGenerator.Scheduler.Step.EnqueueNextStep(Scheduler scheduler) in "
            "C:\\Src\\Tools\\BenchmarkLogGenerator\\Scheduler.cs:line 112\n"
            " at BenchmarkLogGenerator.Scheduler.FlowDelayStep.Execute(Scheduler scheduler) in "
            "C:\\Src\\Tools\\BenchmarkLogGenerator\\Scheduler.cs:line 137\n"
            " at BenchmarkLogGenerator.Scheduler.Run() in "
            "C:\\Src\\Tools\\BenchmarkLogGenerator\\Scheduler.cs:line 28\n"
            " at BenchmarkLogGenerator.Generator.Run(Int32 sizeFactor) in "
            "C:\\Src\\Tools\\BenchmarkLogGenerator\\Generator.cs:line 84\n"
            " at BenchmarkLogGenerator.Generator.<>c__DisplayClass26_0.<RunInBackground>b__0() in "
            "C:\\Src\\Tools\\BenchmarkLogGenerator\\Generator.cs:line 74\n"
            " at System.Threading.ThreadHelper.ThreadStart_Context(Object state)\n"
            " at System.Threading.ExecutionContext.RunInternal(ExecutionContext executionContext)\n"
            " at BenchmarkLogGenerator.Flows.BootFlow.GetLevel(Int64 v) in "
            "C:\\Src\\Tools\\BenchmarkLogGenerator\\Flows\\BootFlow.cs:line 85\n"
            " at BenchmarkLogGenerator.Flows.BootFlow.<IngestionSession>d__1.MoveNext() in "
            "C:\\Src\\Tools\\BenchmarkLogGenerator\\Flows\\BootFlow.cs:line 47\n"
            " at BenchmarkLogGenerator.Scheduler.Flow.NextStep() in "
            "C:\\Src\\Tools\\BenchmarkLogGenerator\\Scheduler.cs:line 74\n"
            " at BenchmarkLogGenerator.Scheduler.Step.EnqueueNextStep(Scheduler scheduler) in "
            "C:\\Src\\Tools\\BenchmarkLogGenerator\\Scheduler.cs:line 112\n"
            " at BenchmarkLogGenerator.Scheduler.FlowDelayStep.Execute(Scheduler scheduler) in "
            "C:\\Src\\Tools\\BenchmarkLogGenerator\\Scheduler.cs:line 137\n"
            " at BenchmarkLogGenerator.Scheduler.Run() in "
            "C:\\Src\\Tools\\BenchmarkLogGenerator\\Scheduler.cs:line 28\n"
            " at BenchmarkLogGenerator.Generator.Run(Int32 sizeFactor) in "
            "C:\\Src\\Tools\\BenchmarkLogGenerator\\Generator.cs:line 84\n"
            " at BenchmarkLogGenerator.Generator.<>c__DisplayClass26_0.<RunInBackground>b__0() in "
            "C:\\Src\\Tools\\BenchmarkLogGenerator\\Generator.cs:line 74\n"
            " at System.Threading.ThreadHelper.ThreadStart_Context(Object state)\n"
            " at System.Threading.ExecutionContext.RunInternal(ExecutionContext executionContext)\n"
            " at BenchmarkLogGenerator.Flows.BootFlow.GetLevel(Int64 v) in "
            "C:\\Src\\Tools\\BenchmarkLogGenerator\\Flows\\BootFlow.cs:line 85\n"
            " at BenchmarkLogGenerator.Flows.BootFlow.<IngestionSession>d__1.MoveNext() in "
            "C:\\Src\\Tools\\BenchmarkLogGenerator\\Flows\\BootFlow.cs:line 47\n"
            " at BenchmarkLogGenerator.Scheduler.Flow.NextStep() in "
            "C:\\Src\\Tools\\BenchmarkLogGenerator\\Scheduler.cs:line 74\n"
            " at BenchmarkLogGenerator.Scheduler.Step.EnqueueNextStep(Scheduler scheduler) in "
            "C:\\Src\\Tools\\BenchmarkLogGenerator\\Scheduler.cs:line 112\n"
            " at BenchmarkLogGenerator.Scheduler.FlowDelayStep.Execute(Scheduler scheduler) in "
            "C:\\Src\\Tools\\BenchmarkLogGenerator\\Scheduler.cs:line 137\n"
            " at BenchmarkLogGenerator.Scheduler.Run() in "
            "C:\\Src\\Tools\\BenchmarkLogGenerator\\Scheduler.cs:line 28\n"
            " at BenchmarkLogGenerator.Generator.Run(Int32 sizeFactor) in "
            "C:\\Src\\Tools\\BenchmarkLogGenerator\\Generator.cs:line 84\n"
            " at BenchmarkLogGenerator.Generator.<>c__DisplayClass26_0.<RunInBackground>b__0() in "
            "C:\\Src\\Tools\\BenchmarkLogGenerator\\Generator.cs:line 74\n"
            " at System.Threading.ThreadHelper.ThreadStart_Context(Object state)\n"
            " at System.Threading.ExecutionContext.RunInternal(ExecutionContext executionContext)\n"
            " at BenchmarkLogGenerator.Flows.BootFlow.GetLevel(Int64 v) in "
            "C:\\Src\\Tools\\BenchmarkLogGenerator\\Flows\\BootFlow.cs:line 85\n"
            " at BenchmarkLogGenerator.Flows.BootFlow.<IngestionSession>d__1.MoveNext() in "
            "C:\\Src\\Tools\\BenchmarkLogGenerator\\Flows\\BootFlow.cs:line 47\n"
            " at BenchmarkLogGenerator.Scheduler.Flow.NextStep() in "
            "C:\\Src\\Tools\\BenchmarkLogGenerator\\Scheduler.cs:line 74\n"
            " at BenchmarkLogGenerator.Scheduler.Step.EnqueueNextStep(Scheduler scheduler) in "
            "C:\\Src\\Tools\\BenchmarkLogGenerator\\Scheduler.cs:line 112\n"
            " at BenchmarkLogGenerator.Scheduler.FlowDelayStep.Execute(Scheduler scheduler) in "
            "C:\\Src\\Tools\\BenchmarkLogGenerator\\Scheduler.cs:line 137\n"
            " at BenchmarkLogGenerator.Scheduler.Run() in "
            "C:\\Src\\Tools\\BenchmarkLogGenerator\\Scheduler.cs:line 28\n"
            " at BenchmarkLogGenerator.Generator.Run(Int32 sizeFactor) in "
            "C:\\Src\\Tools\\BenchmarkLogGenerator\\Generator.cs:line 84\n"
            " at BenchmarkLogGenerator.Generator.<>c__DisplayClass26_0.<RunInBackground>b__0() in "
            "C:\\Src\\Tools\\BenchmarkLogGenerator\\Generator.cs:line 74\n"
            " at System.Threading.ThreadHelper.ThreadStart_Context(Object state)\n"
            " at System.Threading.ExecutionContext.RunInternal(ExecutionContext executionContext)\n"
            " at BenchmarkLogGenerator.Flows.BootFlow.GetLevel(Int64 v) in "
            "C:\\Src\\Tools\\BenchmarkLogGenerator\\Flows\\BootFlow.cs:line 85\n"
            " at BenchmarkLogGenerator.Flows.BootFlow.<IngestionSession>d__1.MoveNext() in "
            "C:\\Src\\Tools\\BenchmarkLogGenerator\\Flows\\BootFlow.cs:line 47\n"
            " at BenchmarkLogGenerator.Scheduler.Flow.NextStep() in "
            "C:\\Src\\Tools\\BenchmarkLogGenerator\\Scheduler.cs:line 74\n"
            " at BenchmarkLogGenerator.Scheduler.Step.EnqueueNextStep(Scheduler scheduler) in "
            "C:\\Src\\Tools\\BenchmarkLogGenerator\\Scheduler.cs:line 112\n"
            " at BenchmarkLogGenerator.Scheduler.FlowDelayStep.Execute(Scheduler scheduler) in "
            "C:\\Src\\Tools\\BenchmarkLogGenerator\\Scheduler.cs:line 137\n"
            " at BenchmarkLogGenerator.Scheduler.Run() in "
            "C:\\Src\\Tools\\BenchmarkLogGenerator\\Scheduler.cs:line 28\n"
            " at BenchmarkLogGenerator.Generator.Run(Int32 sizeFactor) in "
            "C:\\Src\\Tools\\BenchmarkLogGenerator\\Generator.cs:line 84\n"
            " at BenchmarkLogGenerator.Generator.<>c__DisplayClass26_0.<RunInBackground>b__0() in "
            "C:\\Src\\Tools\\BenchmarkLogGenerator\\Generator.cs:line 74\n"
            " at System.Threading.ThreadHelper.ThreadStart_Context(Object state)\n"
            " at System.Threading.ExecutionContext.RunInternal(ExecutionContext executionContext)\n"
            " at BenchmarkLogGenerator.Flows.BootFlow.GetLevel(Int64 v) in "
            "C:\\Src\\Tools\\BenchmarkLogGenerator\\Flows\\BootFlow.cs:line 85";
    size_t n = 5;
    std::string info = get_repetition_info(statement, n);

    std::cout << info << std::endl;
}

TEST_F(TestITokenExtractor, ngram_extractor_repetition_rate_short_text) {
    std::string statement =
            "I bought these leggings for my daughter @ Christmas along with several other "
            "leggings.  She liked these leggings the best since they were lined and are very warm. "
            " She is 5'3&#34; and 115 lbs. and they fit her very well/comfortable.  The only thing "
            "I disliked about them is that the pattern is not uniform on both legs as it gets to "
            "your upper thigh area.";
    size_t n = 5;
    std::string info = get_repetition_info(statement, n);

    std::cout << info << std::endl;
}

TEST_F(TestITokenExtractor, ngram_extractor_repetition_rate_medium_text) {
    std::string statement =
            "Loving the fabulous and exquisite women's wear for plus size women, because of how "
            "this sweater makes you feel good about yourself, and speaks to her heart with a "
            "positive perspective, given her overall character as well."
            "I bought these leggings for my daughter @ Christmas along with several other "
            "leggings.  She liked these leggings the best since they were lined and are very warm. "
            " She is 5'3&#34; and 115 lbs. and they fit her very well/comfortable.  The only thing "
            "I disliked about them is that the pattern is not uniform on both legs as it gets to "
            "your upper thigh area."
            "Love my boot cuffs I got as a gift. This is one I won’t be re-gifting. People at work "
            "love it, good quality and good value. Love that it’s reversible and I can wear it "
            "with any size boots."
            "Reminds me of being 13 in the early 80's, only these are more attractive. These leg "
            "warmers are exactly as pictured, soft & warm over my jeans to keep out the chill on "
            "this snowy day. Brand new in package & I am very happy with this purchase.  I will "
            "buy another pair to double up the warmth on my bare legs."
            "I couldn't be happier with this dress. It is the epitome of classic WW2 era ladies "
            "fashion.<br /> The material is lightweight, yet very soft and silky. It has a full "
            "lining to it. I would<br />recommend sizing up on this particular<br />style as it "
            "has a way of hugging your<br />curves, and in the midsection .<br /><br />If you have "
            "a perfectly flat stomach, then No worries.<br />But ladies who have a wee bit of a "
            "pouch inFront, this dress may hug you a tad in the tummy.<br />It hangs very nicely "
            "in back, and flows<br />beautifully.  Honestly , i would order one in<br />every "
            "color of the rainbow if they sold<br />them !<br />I love it, Thank You!<br />This is "
            "my 4th dress from this vendor, and<br />by far my favorite."
            "This tie is super cute! I love the color and the design... but that's about it.<br "
            "/><br />The day after receiving it in the mail I strapped it on and wore it to work.  "
            "Within the first few hours I noticed the little white Vs began to fray and frizz.  By "
            "the end if the day most of white threading had completely frayed out.  This tie was "
            "very, very cheaply made.<br /><br />It's a shame, because it is... or was... a very "
            "good-looking bow tie!"
            "The color and pictures looks very good. It fits really nicely with a bit of stretch "
            "in the material. I was afraid after washing it that the colors would fade but it did "
            "not. I highly recommand it t!!!"
            "I just purchased this coat, and I have to say that so far, I am very satisfied with "
            "it.  The belt is a nice added touch, but not necessary to wear.  This coat keeps me "
            "very warm, and with the winter we're having this year, it's been a life saver.  I "
            "have gotten compliments on how it looks as well.  This is replacing another coat that "
            "had a zipper that broke after two winters of wearing it, so I am being extra careful "
            "when zippering up this one.  It's too soon to say how sturdy the zipper is on this "
            "one, but as far as everything else, it's serving its purpose well.  I highly "
            "recommend it for the quality and price."
            "ABSOLUTELY JUNK! wore it about four times then the hood nearly ripped completely off! "
            "The Seam came out completely! DO NOT BUY WOULD LOVE TO HAVE MY MONEY COMPLETELY "
            "REFUNDED!"
            "this  was the worst thing I brought online<br />it was very cheaply made size not "
            "true brought<br />as a gift was so embarrassing the person did not accept the gift<br "
            "/>the fur inside looked real fake I am stuck with this one"
            "Honestly the most comfortable jacket I've ever worn. Will probably buy this jacket "
            "for the rest of my life. End of story"
            "ok Im trying to figure out if this is women or unisex sizing..This has a man wearing "
            "it but it clearly is for a girl. I need to know before I order."
            "Very comfortable and cute! It works well in school uniform and everyday wear. The "
            "light material and zippers on the shoulders are super unique and welcomed addition to "
            "my otherwise drab uniform!"
            "The color is active. THe style is ok.<br />One thing to remember is to order one size "
            "bigger than your regular size. For example, I wear S and the size M is OK ON me"
            "These are actually considered panty hose. Unless you are using under a dress or a "
            "very long sweater dont buy. Leggins is not the right description!!!''"
            "Nice Dress"
            "I am overall happy with the leggings. But be aware that if you are larger then a size "
            "8, these will be too small for you. I am a size 8 and they just fit. The pattern is "
            "stretched out quite a bit, but I think it still looks pretty good even tho the "
            "pattern stretch out is not quite as bright and crisp. No complaints about the length "
            "for me. I am 5'7&#34; and these leggings reach my ankles without the feeling that "
            "they are going to pull off of my hips."
            "I bought these jeans knowing they were marked 'irregular' and thought there would be "
            "a noticeable flaw. But when I received these jeans I was pleasantly surprised. They "
            "look great and I couldn't find a flaw. The only thing I noticed was that the jeans "
            "fit a bit tight around my butt. This is my first pair of big star jeans so it could "
            "just be how they fit but I'm not sure. Other than that, these jeans are great for the "
            "price."
            "great scarf for price, ships quickly, color is more turquoise, than baby blue. really "
            "like the chevron design lots of compliments."
            "The fit of these leggings is excellent, they are extremely comfortable and true to "
            "size. Not a skinny girl's legging, there's room to breathe. The classy, paisley "
            "pattern makes regular black leggings seem boring. Good material and the design is "
            "done nicely. An excellent buy, thanks Amazon."
            "The dress is gorgeous and the mesh hearts are awesome. the material was a little "
            "surprising, but its really cool"
            "It did take long to get though well worth the wait... This was a gift for my daughter "
            "and she loved it!! No issues with the product !"
            "I love this sweater. I bought it for my daughter and she loves it. The colors are "
            "very bright and I will surely be purchasing more from this seller."
            "I bought this sweater in this color and in black in medium.  I wear a medium.  I "
            "tried on the black first and the entire sweater fell apart as I was putting it on!  "
            "It literally came apart at the seams!"
            "This wallet is nice looking and has the strongest chain I have ever seen.  However, "
            "it<br />simply has too few wallets for credit cards, so I sent it back.  Others, "
            "however may like<br />it, so check it out anyway."
            "My husband loves his new scarf, as it is so extremely soft and warm.  He was even "
            "willing to give up his favorite scarf, which he has worn for years, for this one.  It "
            "adds just the right amount of color at the neckline of his black wool overcoat to "
            "wear to the office."
            "This dress appears to be quite beautiful in picture but is not. The materials was not "
            "very nice, looked a bit cheap. as well the overall fit was not very nice. Had the "
            "materials been of slightly better quality, it would have made up for some minor "
            "imperfections. The dress runs very very small. I am an xs/s typically and thought "
            "this was just too too tight and uncomfortable."
            "Very nice scarves. Only complaint would be the description says one is purple but it "
            "is actually a burgandy color."
            "I ordered a large which is my usual size and found the arms to really tight even "
            "without  a winter sweater.<br />Poor quality - strings and &#34;pulls&#34; everywhere"
            "Thank you so much for my my beautiful dress. The fit was perfect. The detail of the "
            "dress was exactly like the picture. Also the dress was delivered before time. Thanks "
            "again and I will be making future purchases very soon.5 stars for sure."
            "this is a great looking shirt but i wish they had it in a medium i would definatley "
            "spend my money if it was smaller"
            "Purchased this for my granddaughter, and she simply loves it!  People tell her, she "
            "looks like a &#34;Pop Star&#34; because of the design and even mention she looks like "
            "Michael Jackson!  All she needs is to learn how to sing and dance!"
            "At first I was worried that they would not stay up, but that was not a problem.  I "
            "wish they were available in a calf  length for boots"
            "I purchased this hat, more for a joke then keeping warm.  The hat and beard are well "
            "made.  Looks cool.  I don't think the beard would really do much to keep your face "
            "warm.  My buddies all got a laugh when I showed up wearing it."
            "The actual shorts and ordering process was great but listed measurements diddnt match "
            "up. I ordered the nxt size up and still too small."
            "If you are looking for stretchy these aren't it so make sure to order right size. "
            "Because of the fleece material inside they slide down constantly. Not too happy. But "
            "they are pretty."
            "So I have a 45+ inch chest and a 31 inch waist. Some would say that I'm athletically "
            "proportioned. I will never find anything that fits me the way that it's supposed to "
            "fit but this  hoodie came damn near close. The US XL is nearly perfect for me. It "
            "tappers around the waist as advertise even for broader guy like myself. My only quirk "
            "is the collar around the hood gives a &#34;no neck&#34; appearance. But it's growing "
            "on me. So as I said &#34;nearly perfect&#34;."
            "This hat was purchased for my nephew for Christmas.  It barely made it through "
            "Christmas Eve. The fabric is extremely flimsy and there was a giant hole in it after "
            "one or two times he put it on.  I was able to get Amazon to refund my money, but not "
            "worth the purchase.  Very flimsy material."
            "Got these for my mom and she wears them all the time. cute and comfy. I will borrow "
            "them from her soon."
            "first, the color is not like the picture above, the material of the shirt looks so "
            "cheap and uncomfortable, the lace also looks so cheap.<br />second, at least use a "
            "better material, the product really don't looks like the picture and not worthy at all"
            "I purchased for my daughter and she loves it!  This is a very high quality product "
            "and worth the cost.  I certainly would not pay $500 as the suggested price but "
            "certainly worth the $160 paid.  It did take nearly one month to arrive."
            "The elastic material is comfortable, fits great on me .  The straps are detachable so "
            "you can have it cross your back or go  bare."
            "This blazer was poorly sewn together.  The metal closure fell off when trying it on "
            "for the first time.  The material was uneven in length. This was a disappointing "
            "purchase."
            "I'm wearing this with my steelers t-shirt when I go to Vegas in a couple of weeks to "
            "represent my team even though we not in the super bowl"
            "I ordered a 3X. Normally a 2X will fit me in most clothing, but I order 3X when "
            "available. This was tight and very,very thin. I returned it."
            "This hood is super adorable and I love the pink/gray combination. There are just 2 "
            "small things that I wasn't thrilled about. 1) The hood itself is just a tad small. 2) "
            "The back part is cut kinda short leaving my neck a tinse exposed but I just pushed "
            "the hood further back on my head and got a bit more coverage out of it. But I can "
            "live with those things because it is super cute!"
            "Love the color, cut and style of these gloves. They keep my hands warm without "
            "restricting the use of my fingers for keying, sorting , etc. I think they are the "
            "smartest buy I've made all winter!"
            "so sucks the quality<br />the color is not like the picture above and the fur makes "
            "it looks so cheap"
            "And they look great on me! LOL They are simple with a classic look to them. I'll "
            "probably pair with similar color shoes."
            "The size was at least two sizes smaller than the printed size.  They do not shape "
            "well.  I was very disappointed.";
    size_t n = 5;
    std::string info = get_repetition_info(statement, n);

    std::cout << info << std::endl;
}
} // namespace doris
