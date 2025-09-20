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

#include "olap/rowset/segment_v2/inverted_index/analyzer/icu/icu_analyzer.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

using namespace lucene::analysis;

namespace doris::segment_v2 {

class ICUTokenizerTest : public ::testing::Test {
protected:
    void tokenize(const std::string& s, std::vector<std::string>& datas) {
        try {
            ICUAnalyzer analyzer;
            analyzer.initDict("./be/dict/icu");
            analyzer.set_lowercase(false);

            auto reader = std::make_shared<lucene::util::SStringReader<char>>();
            reader->init(s.data(), s.size(), false);

            std::unique_ptr<inverted_index::ICUTokenizer> tokenizer;
            tokenizer.reset((inverted_index::ICUTokenizer*)analyzer.tokenStream(L"", reader));

            Token t;
            while (tokenizer->next(&t)) {
                std::string term(t.termBuffer<char>(), t.termLength<char>());
                datas.emplace_back(term);
            }
        } catch (CLuceneError& e) {
            std::cout << e.what() << std::endl;
            throw e;
        }
    }
};

TEST_F(ICUTokenizerTest, TestICUTokenizer) {
    std::vector<std::string> datas;

    // Chinese text
    std::string chineseText =
            "今天天气真好，我们一起去公园散步吧。人工智能正在改变我们的生活方式。这本书的内容非常有"
            "趣，我推荐给你。";
    tokenize(chineseText, datas);
    ASSERT_EQ(datas.size(), 27);
    datas.clear();

    // English text
    std::string englishText =
            "The quick brown fox jumps over the lazy dog. Artificial intelligence is transforming "
            "various industries. Reading books can significantly enhance your knowledge.";
    tokenize(englishText, datas);
    ASSERT_EQ(datas.size(), 22);
    datas.clear();

    // Vietnamese text
    std::string vietnameseText =
            "Hôm nay thời tiết thật đẹp, chúng ta cùng đi dạo công viên nhé. Trí tuệ nhân tạo đang "
            "thay đổi cách sống của chúng ta. Cuốn sách này rất thú vị, tôi muốn giới thiệu cho "
            "bạn.";
    tokenize(vietnameseText, datas);
    ASSERT_EQ(datas.size(), 38);
    datas.clear();

    // Portuguese text
    std::string portugueseText =
            "O tempo está ótimo hoje, vamos dar um passeio no parque. A inteligência artificial "
            "está transformando nossas vidas. Este livro é muito interessante, eu recomendo para "
            "você.";
    tokenize(portugueseText, datas);
    ASSERT_EQ(datas.size(), 27);
    datas.clear();

    // Indonesian text
    std::string indonesianText =
            "Hari ini cuaca sangat bagus, mari kita jalan-jalan ke taman. Kecerdasan buatan sedang "
            "mengubah cara hidup kita. Buku ini sangat menarik, mari kita rekomendasikan.";
    tokenize(indonesianText, datas);
    ASSERT_EQ(datas.size(), 25);
    datas.clear();

    // Spanish text
    std::string spanishText =
            "Hoy hace muy buen tiempo, vamos a pasear por el parque. La inteligencia artificial "
            "está cambiando nuestras vidas. Este libro es muy interesante, te lo recomiendo.";
    tokenize(spanishText, datas);
    ASSERT_EQ(datas.size(), 26);
    datas.clear();

    // Thai text
    std::string thaiText =
            "วันนี้อากาศดีมาก "
            "เราไปเดินเล่นที่สวนสาธารณะกันเถอะปัญญาประดิษฐ์กำลังเปลี่ยนวิถีชีวิตของเราหนังสือเล่มนี้น่าสนใจมาก "
            "ฉันอยากแนะนำให้คุณอ่าน";
    tokenize(thaiText, datas);
    ASSERT_EQ(datas.size(), 34);
    datas.clear();

    // Hindi text
    std::string hindiText =
            "आज मौसम बहुत अच्छा है, चलो पार्क में टहलने चलते हैं। कृत्रिम बुद्धिमत्ता हमारे जीवन को बदल रही है। यह "
            "किताब बहुत दिलचस्प है, मैं इसे आपको सुझाता हूं।";
    tokenize(hindiText, datas);
    ASSERT_EQ(datas.size(), 29);
    datas.clear();
}

TEST_F(ICUTokenizerTest, TestICUTokenizerEmptyText) {
    std::vector<std::string> datas;
    std::string emptyText;
    tokenize(emptyText, datas);
    ASSERT_EQ(datas.size(), 0);
}

TEST_F(ICUTokenizerTest, TestICUTokenizerSingleWord) {
    std::vector<std::string> datas;

    // Chinese word
    std::string chineseText = "天气";
    tokenize(chineseText, datas);
    ASSERT_EQ(datas.size(), 1);
    datas.clear();

    // English word
    std::string englishText = "weather";
    tokenize(englishText, datas);
    ASSERT_EQ(datas.size(), 1);
    datas.clear();

    // Arabic word
    std::string arabicText = "الذكاء";
    tokenize(arabicText, datas);
    ASSERT_EQ(datas.size(), 1);
}

TEST_F(ICUTokenizerTest, TestICUTokenizerMultipleSpaces) {
    std::vector<std::string> datas;
    std::string multipleSpacesText = "The    quick    brown   fox";
    tokenize(multipleSpacesText, datas);
    ASSERT_EQ(datas.size(), 4);
}

TEST_F(ICUTokenizerTest, TestICUTokenizerPunctuation) {
    std::vector<std::string> datas;
    std::string textWithPunctuation = "Hello, world! How's it going?";
    tokenize(textWithPunctuation, datas);
    ASSERT_EQ(datas.size(), 5);
}

TEST_F(ICUTokenizerTest, TestICUTokenizerMixedLanguage) {
    std::vector<std::string> datas;
    std::string mixedText = "Hello, 今天天气真好!";
    tokenize(mixedText, datas);
    ASSERT_EQ(datas.size(), 4);
}

TEST_F(ICUTokenizerTest, TestICUTokenizerUnicode) {
    std::vector<std::string> datas;
    std::string unicodeText = "你好，世界! 😊🌍";
    tokenize(unicodeText, datas);
    ASSERT_EQ(datas.size(), 4);
}

TEST_F(ICUTokenizerTest, TestICUTokenizerNumericText) {
    std::vector<std::string> datas;
    std::string numericText = "The price is 100 dollars.";
    tokenize(numericText, datas);
    ASSERT_EQ(datas.size(), 5);
}

TEST_F(ICUTokenizerTest, TestICUTokenizerLongText) {
    std::vector<std::string> datas;
    std::string longText =
            "Artificial intelligence is rapidly changing various industries around the world. "
            "From healthcare to finance, it is transforming the way we work, live, and interact "
            "with technology.";
    tokenize(longText, datas);
    ASSERT_EQ(datas.size(), 26);
}

TEST_F(ICUTokenizerTest, TestICUTokenizerSpecialCharacters) {
    std::vector<std::string> datas;
    std::string specialCharsText = "@#$%^&*()_+{}[]|:;\"'<>,.?/~`";
    tokenize(specialCharsText, datas);
    ASSERT_EQ(datas.size(), 0);
}

TEST_F(ICUTokenizerTest, TestICUTokenizerLongWords) {
    std::vector<std::string> datas;
    std::string longWordText = "hippopotomonstrosesquipedaliophobia";
    tokenize(longWordText, datas);
    ASSERT_EQ(datas.size(), 1);
}

TEST_F(ICUTokenizerTest, TestICUArmenian) {
    std::vector<std::string> datas;
    std::string longWordText =
            "Վիքիպեդիայի 13 միլիոն հոդվածները (4,600` հայերեն վիքիպեդիայում) գրվել են կամավորների "
            "կողմից ու համարյա բոլոր հոդվածները կարող է խմբագրել ցանկաց մարդ ով կարող է բացել "
            "Վիքիպեդիայի կայքը։";
    tokenize(longWordText, datas);
    std::vector<std::string> result = {
            "Վիքիպեդիայի",   "13",    "միլիոն",     "հոդվածները",  "4,600",  "հայերեն",
            "վիքիպեդիայում", "գրվել", "են",         "կամավորների", "կողմից", "ու",
            "համարյա",       "բոլոր", "հոդվածները", "կարող",       "է",      "խմբագրել",
            "ցանկաց",        "մարդ",  "ով",         "կարող",       "է",      "բացել",
            "Վիքիպեդիայի",   "կայքը"};
    for (size_t i = 0; i < datas.size(); i++) {
        ASSERT_EQ(datas[i], result[i]);
    }
}

TEST_F(ICUTokenizerTest, TestICUAmharic) {
    std::vector<std::string> datas;
    std::string longWordText = "ዊኪፔድያ የባለ ብዙ ቋንቋ የተሟላ ትክክለኛና ነጻ መዝገበ ዕውቀት (ኢንሳይክሎፒዲያ) ነው። ማንኛውም";
    tokenize(longWordText, datas);
    std::vector<std::string> result = {"ዊኪፔድያ", "የባለ",  "ብዙ",   "ቋንቋ",       "የተሟላ", "ትክክለኛና",
                                       "ነጻ",    "መዝገበ", "ዕውቀት", "ኢንሳይክሎፒዲያ", "ነው",   "ማንኛውም"};
    for (size_t i = 0; i < datas.size(); i++) {
        ASSERT_EQ(datas[i], result[i]);
    }
}

TEST_F(ICUTokenizerTest, TestICUArabic) {
    std::vector<std::string> datas;
    std::string longWordText =
            "الفيلم الوثائقي الأول عن ويكيبيديا يسمى \"الحقيقة بالأرقام: قصة ويكيبيديا\" "
            "(بالإنجليزية: Truth in Numbers: The Wikipedia Story)، سيتم إطلاقه في 2008.";
    tokenize(longWordText, datas);
    std::vector<std::string> result = {
            "الفيلم",   "الوثائقي",  "الأول",     "عن",          "ويكيبيديا", "يسمى", "الحقيقة",
            "بالأرقام", "قصة",       "ويكيبيديا", "بالإنجليزية", "Truth",     "in",   "Numbers",
            "The",      "Wikipedia", "Story",     "سيتم",        "إطلاقه",    "في",   "2008"};
    for (size_t i = 0; i < datas.size(); i++) {
        ASSERT_EQ(datas[i], result[i]);
    }
}

TEST_F(ICUTokenizerTest, TestICUAramaic) {
    std::vector<std::string> datas;
    std::string longWordText =
            "ܘܝܩܝܦܕܝܐ (ܐܢܓܠܝܐ: Wikipedia) ܗܘ ܐܝܢܣܩܠܘܦܕܝܐ ܚܐܪܬܐ ܕܐܢܛܪܢܛ ܒܠܫܢ̈ܐ ܣܓܝܐ̈ܐ܂ ܫܡܗ ܐܬܐ ܡܢ "
            "ܡ̈ܠܬܐ ܕ\"ܘܝܩܝ\" ܘ\"ܐܝܢܣܩܠܘܦܕܝܐ\"܀";
    tokenize(longWordText, datas);
    std::vector<std::string> result = {
            "ܘܝܩܝܦܕܝܐ", "ܐܢܓܠܝܐ", "Wikipedia", "ܗܘ",  "ܐܝܢܣܩܠܘܦܕܝܐ", "ܚܐܪܬܐ",
            "ܕܐܢܛܪܢܛ",  "ܒܠܫܢ̈ܐ",  "ܣܓܝܐ̈ܐ",     "ܫܡܗ", "ܐܬܐ",         "ܡܢ",
            "ܡ̈ܠܬܐ",     "ܕ",      "ܘܝܩܝ",      "ܘ",   "ܐܝܢܣܩܠܘܦܕܝܐ"};
    for (size_t i = 0; i < datas.size(); i++) {
        ASSERT_EQ(datas[i], result[i]);
    }
}

TEST_F(ICUTokenizerTest, TestICUBengali) {
    std::vector<std::string> datas;
    std::string longWordText =
            "এই বিশ্বকোষ পরিচালনা করে উইকিমিডিয়া ফাউন্ডেশন (একটি অলাভজনক সংস্থা)। উইকিপিডিয়ার শুরু ১৫ "
            "জানুয়ারি, ২০০১ সালে। এখন পর্যন্ত ২০০টিরও বেশী ভাষায় উইকিপিডিয়া রয়েছে।܀";
    tokenize(longWordText, datas);
    std::vector<std::string> result = {"এই",         "বিশ্বকোষ", "পরিচালনা", "করে",   "উইকিমিডিয়া",
                                       "ফাউন্ডেশন",   "একটি",    "অলাভজনক",  "সংস্থা", "উইকিপিডিয়ার",
                                       "শুরু",         "১৫",      "জানুয়ারি",  "২০০১",  "সালে",
                                       "এখন",        "পর্যন্ত",   "২০০টিরও",  "বেশী",  "ভাষায়",
                                       "উইকিপিডিয়া", "রয়েছে"};
    for (size_t i = 0; i < datas.size(); i++) {
        ASSERT_EQ(datas[i], result[i]);
    }
}

TEST_F(ICUTokenizerTest, TestICUFarsi) {
    std::vector<std::string> datas;
    std::string longWordText =
            "ویکی پدیای انگلیسی در تاریخ ۲۵ دی ۱۳۷۹ به صورت مکملی برای دانشنامهٔ تخصصی نوپدیا نوشته "
            "شد.";
    tokenize(longWordText, datas);
    std::vector<std::string> result = {"ویکی",     "پدیای", "انگلیسی", "در",    "تاریخ", "۲۵",
                                       "دی",       "۱۳۷۹",  "به",      "صورت",  "مکملی", "برای",
                                       "دانشنامهٔ", "تخصصی", "نوپدیا",  "نوشته", "شد"};
    for (size_t i = 0; i < datas.size(); i++) {
        ASSERT_EQ(datas[i], result[i]);
    }
}

TEST_F(ICUTokenizerTest, TestICUGreek) {
    std::vector<std::string> datas;
    std::string longWordText =
            "Γράφεται σε συνεργασία από εθελοντές με το λογισμικό wiki, κάτι που σημαίνει ότι "
            "άρθρα μπορεί να προστεθούν ή να αλλάξουν από τον καθένα.";
    tokenize(longWordText, datas);
    std::vector<std::string> result = {"Γράφεται", "σε",         "συνεργασία", "από",   "εθελοντές",
                                       "με",       "το",         "λογισμικό",  "wiki",  "κάτι",
                                       "που",      "σημαίνει",   "ότι",        "άρθρα", "μπορεί",
                                       "να",       "προστεθούν", "ή",          "να",    "αλλάξουν",
                                       "από",      "τον",        "καθένα"};
    for (size_t i = 0; i < datas.size(); i++) {
        ASSERT_EQ(datas[i], result[i]);
    }
}

TEST_F(ICUTokenizerTest, TestICUKhmer) {
    std::vector<std::string> datas;
    std::string longWordText = "ផ្ទះស្កឹមស្កៃបីបួនខ្នងនេះ";
    tokenize(longWordText, datas);
    std::vector<std::string> result = {"ផ្ទះ", "ស្កឹមស្កៃ", "បី", "បួន", "ខ្នង", "នេះ"};
    for (size_t i = 0; i < datas.size(); i++) {
        ASSERT_EQ(datas[i], result[i]);
    }
}

TEST_F(ICUTokenizerTest, TestICULao) {
    std::vector<std::string> datas;
    std::string longWordText = "ກວ່າດອກ ພາສາລາວ";
    tokenize(longWordText, datas);
    std::vector<std::string> result = {"ກວ່າ", "ດອກ", "ພາສາ", "ລາວ"};
    for (size_t i = 0; i < datas.size(); i++) {
        ASSERT_EQ(datas[i], result[i]);
    }
}

TEST_F(ICUTokenizerTest, TestICUMyanmar) {
    std::vector<std::string> datas;
    std::string longWordText = "သက်ဝင်လှုပ်ရှားစေပြီး";
    tokenize(longWordText, datas);
    std::vector<std::string> result = {"သက်ဝင်", "လှုပ်ရှား", "စေ", "ပြီး"};
    for (size_t i = 0; i < datas.size(); i++) {
        ASSERT_EQ(datas[i], result[i]);
    }
}

TEST_F(ICUTokenizerTest, TestICUThai) {
    std::vector<std::string> datas;
    std::string longWordText = "การที่ได้ต้องแสดงว่างานดี. แล้วเธอจะไปไหน? ๑๒๓๔";
    tokenize(longWordText, datas);
    std::vector<std::string> result = {"การ", "ที่",   "ได้",  "ต้อง", "แสดง", "ว่า",  "งาน",
                                       "ดี",   "แล้ว", "เธอ", "จะ",  "ไป",   "ไหน", "๑๒๓๔"};
    for (size_t i = 0; i < datas.size(); i++) {
        ASSERT_EQ(datas[i], result[i]);
    }
}

TEST_F(ICUTokenizerTest, TestICUTibetan) {
    std::vector<std::string> datas;
    std::string longWordText = "སྣོན་མཛོད་དང་ལས་འདིས་བོད་ཡིག་མི་ཉམས་གོང་འཕེལ་དུ་གཏོང་བར་ཧ་ཅང་དགེ་མཚན་མཆིས་སོ། །";
    tokenize(longWordText, datas);
    std::vector<std::string> result = {"སྣོན", "མཛོད", "དང", "ལས",  "འདིས", "བོད",  "ཡིག",
                                       "མི",  "ཉམས", "གོང", "འཕེལ", "དུ",   "གཏོང", "བར",
                                       "ཧ",  "ཅང",  "དགེ", "མཚན", "མཆིས", "སོ"};
    for (size_t i = 0; i < datas.size(); i++) {
        ASSERT_EQ(datas[i], result[i]);
    }
}

TEST_F(ICUTokenizerTest, TestICUChinese) {
    std::vector<std::string> datas;
    std::string longWordText = "我是中国人。 １２３４ Ｔｅｓｔｓ ";
    tokenize(longWordText, datas);
    std::vector<std::string> result = {"我是", "中国人", "１２３４", "Ｔｅｓｔｓ"};
    for (size_t i = 0; i < datas.size(); i++) {
        ASSERT_EQ(datas[i], result[i]);
    }
}

TEST_F(ICUTokenizerTest, TestICUHebrew) {
    {
        std::vector<std::string> datas;
        std::string longWordText = "דנקנר תקף את הדו\"ח";
        tokenize(longWordText, datas);
        std::vector<std::string> result = {"דנקנר", "תקף", "את", "הדו\"ח"};
        for (size_t i = 0; i < datas.size(); i++) {
            ASSERT_EQ(datas[i], result[i]);
        }
    }
    {
        std::vector<std::string> datas;
        std::string longWordText = "חברת בת של מודי'ס";
        tokenize(longWordText, datas);
        std::vector<std::string> result = {"חברת", "בת", "של", "מודי'ס"};
        for (size_t i = 0; i < datas.size(); i++) {
            ASSERT_EQ(datas[i], result[i]);
        }
    }
}

TEST_F(ICUTokenizerTest, TestICUEmpty) {
    std::vector<std::string> datas;
    std::string longWordText = " . ";
    tokenize(longWordText, datas);
    std::vector<std::string> result = {};
    for (size_t i = 0; i < datas.size(); i++) {
        ASSERT_EQ(datas[i], result[i]);
    }
}

TEST_F(ICUTokenizerTest, TestICULUCENE1545) {
    std::vector<std::string> datas;
    std::string longWordText = "moͤchte";
    tokenize(longWordText, datas);
    std::vector<std::string> result = {"moͤchte"};
    for (size_t i = 0; i < datas.size(); i++) {
        ASSERT_EQ(datas[i], result[i]);
    }
}

TEST_F(ICUTokenizerTest, TestICUAlphanumericSA) {
    std::vector<std::string> datas;
    std::string longWordText = "B2B 2B";
    tokenize(longWordText, datas);
    std::vector<std::string> result = {"B2B", "2B"};
    for (size_t i = 0; i < datas.size(); i++) {
        ASSERT_EQ(datas[i], result[i]);
    }
}

TEST_F(ICUTokenizerTest, TestICUDelimitersSA) {
    std::vector<std::string> datas;
    std::string longWordText = "some-dashed-phrase dogs,chase,cats ac/dc";
    tokenize(longWordText, datas);
    std::vector<std::string> result = {"some",  "dashed", "phrase", "dogs",
                                       "chase", "cats",   "ac",     "dc"};
    for (size_t i = 0; i < datas.size(); i++) {
        ASSERT_EQ(datas[i], result[i]);
    }
}

TEST_F(ICUTokenizerTest, TestICUApostrophesSA) {
    std::vector<std::string> datas;
    std::string longWordText = "O'Reilly you're she's Jim's don't O'Reilly's";
    tokenize(longWordText, datas);
    std::vector<std::string> result = {"O'Reilly", "you're", "she's",
                                       "Jim's",    "don't",  "O'Reilly's"};
    for (size_t i = 0; i < datas.size(); i++) {
        ASSERT_EQ(datas[i], result[i]);
    }
}

TEST_F(ICUTokenizerTest, TestICUNumericSA) {
    std::vector<std::string> datas;
    std::string longWordText = "21.35 R2D2 C3PO 216.239.63.104 216.239.63.104";
    tokenize(longWordText, datas);
    std::vector<std::string> result = {"21.35", "R2D2", "C3PO", "216.239.63.104", "216.239.63.104"};
    for (size_t i = 0; i < datas.size(); i++) {
        ASSERT_EQ(datas[i], result[i]);
    }
}

TEST_F(ICUTokenizerTest, TestICUTextWithNumbersSA) {
    std::vector<std::string> datas;
    std::string longWordText = "David has 5000 bones";
    tokenize(longWordText, datas);
    std::vector<std::string> result = {"David", "has", "5000", "bones"};
    for (size_t i = 0; i < datas.size(); i++) {
        ASSERT_EQ(datas[i], result[i]);
    }
}

TEST_F(ICUTokenizerTest, TestICUVariousTextSA) {
    std::vector<std::string> datas;
    std::string longWordText =
            "C embedded developers wanted foo bar FOO BAR foo      bar .  FOO <> BAR \"QUOTED\" "
            "word";
    tokenize(longWordText, datas);
    std::vector<std::string> result = {"C",   "embedded", "developers", "wanted", "foo",
                                       "bar", "FOO",      "BAR",        "foo",    "bar",
                                       "FOO", "BAR",      "QUOTED",     "word"};
    for (size_t i = 0; i < datas.size(); i++) {
        ASSERT_EQ(datas[i], result[i]);
    }
}

TEST_F(ICUTokenizerTest, TestICUKoreanSA) {
    std::vector<std::string> datas;
    std::string longWordText = "안녕하세요 한글입니다";
    tokenize(longWordText, datas);
    std::vector<std::string> result = {"안녕하세요", "한글입니다"};
    for (size_t i = 0; i < datas.size(); i++) {
        ASSERT_EQ(datas[i], result[i]);
    }
}

TEST_F(ICUTokenizerTest, TestICUReusableTokenStream) {
    std::vector<std::string> datas;
    std::string longWordText = "སྣོན་མཛོད་དང་ལས་འདིས་བོད་ཡིག་མི་ཉམས་གོང་འཕེལ་དུ་གཏོང་བར་ཧ་ཅང་དགེ་མཚན་མཆིས་སོ། །";
    tokenize(longWordText, datas);
    std::vector<std::string> result = {"སྣོན", "མཛོད", "དང", "ལས",  "འདིས", "བོད",  "ཡིག",
                                       "མི",  "ཉམས", "གོང", "འཕེལ", "དུ",   "གཏོང", "བར",
                                       "ཧ",  "ཅང",  "དགེ", "མཚན", "མཆིས", "སོ"};
    for (size_t i = 0; i < datas.size(); i++) {
        ASSERT_EQ(datas[i], result[i]);
    }
}

TEST_F(ICUTokenizerTest, TestICUOffsets) {
    std::vector<std::string> datas;
    std::string longWordText = "David has 5000 bones";
    tokenize(longWordText, datas);
    std::vector<std::string> result = {"David", "has", "5000", "bones"};
    for (size_t i = 0; i < datas.size(); i++) {
        ASSERT_EQ(datas[i], result[i]);
    }
}

TEST_F(ICUTokenizerTest, TestICUKorean) {
    std::vector<std::string> datas;
    std::string longWordText = "훈민정음";
    tokenize(longWordText, datas);
    std::vector<std::string> result = {"훈민정음"};
    for (size_t i = 0; i < datas.size(); i++) {
        ASSERT_EQ(datas[i], result[i]);
    }
}

TEST_F(ICUTokenizerTest, TestICUJapanese) {
    std::vector<std::string> datas;
    std::string longWordText = "仮名遣い カタカナ";
    tokenize(longWordText, datas);
    std::vector<std::string> result = {"仮名遣い", "カタカナ"};
    for (size_t i = 0; i < datas.size(); i++) {
        ASSERT_EQ(datas[i], result[i]);
    }
}

TEST_F(ICUTokenizerTest, TestICUEmoji) {
    std::vector<std::string> datas;
    std::string longWordText =
            "💩 💩💩 👩‍❤️‍👩 👨🏼‍⚕️ 🇺🇸🇺🇸 #️⃣ 3️⃣ "
            "🏴";
    tokenize(longWordText, datas);
    std::vector<std::string> result = {
            "💩", "💩", "💩", "👩‍❤️‍👩", "👨🏼‍⚕️", "🇺🇸", "🇺🇸",
            "#️⃣",  "3️⃣",  "🏴"};
    for (size_t i = 0; i < datas.size(); i++) {
        ASSERT_EQ(datas[i], result[i]);
    }
}

TEST_F(ICUTokenizerTest, TestICUEmojiTokenization) {
    std::vector<std::string> datas;
    std::string longWordText = "poo💩poo 💩中國💩";
    tokenize(longWordText, datas);
    std::vector<std::string> result = {"poo", "💩", "poo", "💩", "中國", "💩"};
    for (size_t i = 0; i < datas.size(); i++) {
        ASSERT_EQ(datas[i], result[i]);
    }
}

TEST_F(ICUTokenizerTest, TestICUScriptExtensions) {
    std::vector<std::string> datas;
    std::string longWordText = "𑅗० 𑅗ा 𑅗᪾";
    tokenize(longWordText, datas);
    std::vector<std::string> result = {"𑅗०", "𑅗ा", "𑅗᪾"};
    for (size_t i = 0; i < datas.size(); i++) {
        ASSERT_EQ(datas[i], result[i]);
    }
}

} // namespace doris::segment_v2
