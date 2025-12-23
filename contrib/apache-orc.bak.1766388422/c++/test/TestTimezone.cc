/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "Adaptor.hh"
#include "Timezone.hh"
#include "wrap/gtest-wrapper.h"

#include <iostream>
#include <vector>

namespace orc {

  bool isLeap(int64_t year);

  TEST(TestTimezone, isLeap) {
    EXPECT_TRUE(isLeap(2000));
    EXPECT_FALSE(isLeap(2001));
    EXPECT_TRUE(isLeap(2004));
    EXPECT_FALSE(isLeap(2100));
    EXPECT_FALSE(isLeap(2200));
    EXPECT_FALSE(isLeap(2300));
    EXPECT_TRUE(isLeap(2400));
  }

  int64_t binarySearch(const std::vector<int64_t>& array, int64_t target);

  TEST(TestTimezone, testBinarySearch) {
    std::vector<int64_t> vect;
    EXPECT_EQ(-1, binarySearch(vect, 0));
    vect.push_back(0);
    EXPECT_EQ(-1, binarySearch(vect, -5));
    EXPECT_EQ(0, binarySearch(vect, 0));
    EXPECT_EQ(0, binarySearch(vect, 5));
    vect.push_back(2);
    EXPECT_EQ(-1, binarySearch(vect, -1));
    EXPECT_EQ(0, binarySearch(vect, 0));
    EXPECT_EQ(0, binarySearch(vect, 1));
    EXPECT_EQ(1, binarySearch(vect, 2));
    EXPECT_EQ(1, binarySearch(vect, 3));
  }

  /**
   * Parse a future rule string and return the parsed rule as a string.
   */
  std::string stringifyRule(const std::string& ruleString) {
    std::shared_ptr<FutureRule> rule = parseFutureRule(ruleString);
    std::stringstream buffer;
    rule->print(buffer);
    return buffer.str();
  }

  TEST(TestTimezone, parseFutureRule) {
    EXPECT_EQ("  Future rule: FOO0\n  standard FOO 0\n", stringifyRule("FOO0"));
    EXPECT_EQ("  Future rule: <FOO+->010:02\n  standard <FOO+-> -36120\n",
              stringifyRule("<FOO+->010:02"));
    // unclosed '<'
    EXPECT_THROW(stringifyRule("<FOO12"), TimezoneError);
    // empty name
    EXPECT_THROW(stringifyRule("+8"), TimezoneError);
    // missing offset
    EXPECT_THROW(stringifyRule("FOO"), TimezoneError);
    EXPECT_EQ("  Future rule: FOO-123:45:67\n  standard FOO 445567\n",
              stringifyRule("FOO-123:45:67"));
    EXPECT_EQ("  Future rule: FOO+8\n  standard FOO -28800\n", stringifyRule("FOO+8"));
    EXPECT_EQ(("  Future rule: FOO8BAR,J10,20\n  standard FOO -28800\n"
               "  dst BAR -25200 (dst)\n  start julian 10 at 2:0:0\n"
               "  end day 20 at 2:0:0\n"),
              stringifyRule("FOO8BAR,J10,20"));
    EXPECT_EQ(("  Future rule: FOO+8BAR-0:30,M3.1.0,M10.5.6\n"
               "  standard FOO -28800\n"
               "  dst BAR 1800 (dst)\n"
               "  start month 3 week 1 day 0 at 2:0:0\n"
               "  end month 10 week 5 day 6 at 2:0:0\n"),
              stringifyRule("FOO+8BAR-0:30,M3.1.0,M10.5.6"));
    EXPECT_EQ(("  Future rule: FOO10BAR1,3,4\n"
               "  standard FOO -36000\n"
               "  dst BAR -3600 (dst)\n"
               "  start day 3 at 2:0:0\n"
               "  end day 4 at 2:0:0\n"),
              stringifyRule("FOO10BAR1,3,4"));
    // missing transition times
    EXPECT_THROW(stringifyRule("FOO8BAR"), TimezoneError);
    // check left over text
    EXPECT_THROW(stringifyRule("FOO8BAR,10,20x"), TimezoneError);
    EXPECT_EQ(("  Future rule: FOO8BAR,3/3,4/4:30\n"
               "  standard FOO -28800\n"
               "  dst BAR -25200 (dst)\n"
               "  start day 3 at 3:0:0\n"
               "  end day 4 at 4:30:0\n"),
              stringifyRule("FOO8BAR,3/3,4/4:30"));
    EXPECT_EQ(("  Future rule: FOO-8BAR,J3/3,M4.5.6/00:04:0007\n"
               "  standard FOO 28800\n"
               "  dst BAR 32400 (dst)\n"
               "  start julian 3 at 3:0:0\n"
               "  end month 4 week 5 day 6 at 0:4:7\n"),
              stringifyRule("FOO-8BAR,J3/3,M4.5.6/00:04:0007"));
    // too many fields in offset
    EXPECT_THROW(stringifyRule("FOO8BAR,10,20/4:30:20:10"), TimezoneError);

    EXPECT_FALSE(parseFutureRule("")->isDefined());
    EXPECT_TRUE(parseFutureRule("FOO12")->isDefined());
  }

  const std::string& getZoneFromRule(FutureRule* rule, const std::string& date) {
    tm timeStruct;
    if (strptime(date.c_str(), "%Y-%m-%d %H:%M:%S", &timeStruct) == nullptr) {
      throw TimezoneError("bad time " + date);
    }
    return rule->getVariant(timegm(&timeStruct)).name;
  }

  TEST(TestTimezone, useFutureRule) {
    std::shared_ptr<FutureRule> rule = parseFutureRule("FOO8BAR,M3.2.0,M11.1.0");
    // 1970
    EXPECT_EQ("FOO", getZoneFromRule(rule.get(), "1970-01-01 00:00:00"));
    EXPECT_EQ("FOO", getZoneFromRule(rule.get(), "1970-03-08 09:59:59"));
    EXPECT_EQ("BAR", getZoneFromRule(rule.get(), "1970-03-08 10:00:00"));
    EXPECT_EQ("BAR", getZoneFromRule(rule.get(), "1970-04-01 00:00:00"));
    EXPECT_EQ("BAR", getZoneFromRule(rule.get(), "1970-11-01 08:59:59"));
    EXPECT_EQ("FOO", getZoneFromRule(rule.get(), "1970-11-01 09:00:00"));
    EXPECT_EQ("FOO", getZoneFromRule(rule.get(), "1970-12-31 00:00:00"));

    // 2369
    EXPECT_EQ("FOO", getZoneFromRule(rule.get(), "2369-01-01 00:00:00"));
    EXPECT_EQ("FOO", getZoneFromRule(rule.get(), "2369-03-09 09:59:59"));
    EXPECT_EQ("BAR", getZoneFromRule(rule.get(), "2369-03-09 10:00:00"));
    EXPECT_EQ("BAR", getZoneFromRule(rule.get(), "2369-11-02 08:59:59"));
    EXPECT_EQ("FOO", getZoneFromRule(rule.get(), "2369-11-02 09:00:00"));
    EXPECT_EQ("FOO", getZoneFromRule(rule.get(), "2369-12-31 00:00:00"));

    // 2370
    EXPECT_EQ("FOO", getZoneFromRule(rule.get(), "2370-01-01 00:00:00"));
    EXPECT_EQ("FOO", getZoneFromRule(rule.get(), "2370-03-08 09:59:59"));
    EXPECT_EQ("BAR", getZoneFromRule(rule.get(), "2370-03-08 10:00:00"));
    EXPECT_EQ("BAR", getZoneFromRule(rule.get(), "2370-04-01 00:00:00"));
    EXPECT_EQ("BAR", getZoneFromRule(rule.get(), "2370-11-01 08:59:59"));
    EXPECT_EQ("FOO", getZoneFromRule(rule.get(), "2370-11-01 09:00:00"));
    EXPECT_EQ("FOO", getZoneFromRule(rule.get(), "2370-12-31 00:00:00"));

    rule = parseFutureRule("FOO8BAR,J10,J360");
    // 1970
    EXPECT_EQ("FOO", getZoneFromRule(rule.get(), "1970-01-11 09:59:59"));
    EXPECT_EQ("BAR", getZoneFromRule(rule.get(), "1970-01-11 10:00:00"));
    EXPECT_EQ("BAR", getZoneFromRule(rule.get(), "1970-12-27 08:59:59"));
    EXPECT_EQ("FOO", getZoneFromRule(rule.get(), "1970-12-27 09:00:00"));
    // 1972
    EXPECT_EQ("FOO", getZoneFromRule(rule.get(), "1972-01-11 09:59:59"));
    EXPECT_EQ("BAR", getZoneFromRule(rule.get(), "1972-01-11 10:00:00"));
    EXPECT_EQ("BAR", getZoneFromRule(rule.get(), "1972-12-27 08:59:59"));
    EXPECT_EQ("FOO", getZoneFromRule(rule.get(), "1972-12-27 09:00:00"));

    rule = parseFutureRule("FOO8BAR,10,360");
#ifdef HAS_PRE_1970
    EXPECT_EQ("FOO", getZoneFromRule(rule.get(), "1969-01-11 09:59:59"));
    EXPECT_EQ("BAR", getZoneFromRule(rule.get(), "1969-01-11 10:00:00"));
    EXPECT_EQ("BAR", getZoneFromRule(rule.get(), "1969-12-27 08:59:59"));
    EXPECT_EQ("FOO", getZoneFromRule(rule.get(), "1969-12-27 09:00:00"));
#endif
    // 1970
    EXPECT_EQ("FOO", getZoneFromRule(rule.get(), "1970-01-11 09:59:59"));
    EXPECT_EQ("BAR", getZoneFromRule(rule.get(), "1970-01-11 10:00:00"));
    EXPECT_EQ("BAR", getZoneFromRule(rule.get(), "1970-12-27 08:59:59"));
    EXPECT_EQ("FOO", getZoneFromRule(rule.get(), "1970-12-27 09:00:00"));
    // 1972
    EXPECT_EQ("FOO", getZoneFromRule(rule.get(), "1972-01-11 09:59:59"));
    EXPECT_EQ("BAR", getZoneFromRule(rule.get(), "1972-01-11 10:00:00"));
    EXPECT_EQ("BAR", getZoneFromRule(rule.get(), "1972-12-26 08:59:59"));
    EXPECT_EQ("FOO", getZoneFromRule(rule.get(), "1972-12-26 09:00:00"));

    // test a southern hemisphere timezone
    rule = parseFutureRule("FOO8BAR,360,10");
    // 1970
    EXPECT_EQ("BAR", getZoneFromRule(rule.get(), "1970-01-11 08:59:59"));
    EXPECT_EQ("FOO", getZoneFromRule(rule.get(), "1970-01-11 09:00:00"));
    EXPECT_EQ("FOO", getZoneFromRule(rule.get(), "1970-12-27 09:59:59"));
    EXPECT_EQ("BAR", getZoneFromRule(rule.get(), "1970-12-27 10:00:00"));
    // 1972
    EXPECT_EQ("BAR", getZoneFromRule(rule.get(), "1972-01-11 08:59:59"));
    EXPECT_EQ("FOO", getZoneFromRule(rule.get(), "1972-01-11 09:00:00"));
    EXPECT_EQ("FOO", getZoneFromRule(rule.get(), "1972-12-26 09:59:59"));
    EXPECT_EQ("BAR", getZoneFromRule(rule.get(), "1972-12-26 10:00:00"));

    rule = parseFutureRule("FOO8BAR,J10/3,J360/3:30");
    // 1970
    EXPECT_EQ("FOO", getZoneFromRule(rule.get(), "1970-01-11 10:59:59"));
    EXPECT_EQ("BAR", getZoneFromRule(rule.get(), "1970-01-11 11:00:00"));
    EXPECT_EQ("BAR", getZoneFromRule(rule.get(), "1970-12-27 10:29:59"));
    EXPECT_EQ("FOO", getZoneFromRule(rule.get(), "1970-12-27 10:30:00"));
  }

  unsigned char decodeBase64Char(unsigned char ch) {
    switch (ch >> 4) {
      case 4:
      case 5:
        return static_cast<unsigned char>(ch - 'A');
      case 6:
      case 7:
        return static_cast<unsigned char>(ch - 'a' + 26);
      case 3:
        return static_cast<unsigned char>(ch - '0' + 52);
      case 2:
        return static_cast<unsigned char>(ch == '+' ? 62 : 63);
      default:
        return 255;
    }
  }

  std::vector<unsigned char> decodeBase64(const std::string& input) {
    std::vector<unsigned char> result;
    result.reserve(3 * (input.size() / 4));
    uint32_t accumulator = 0;
    uint32_t bits = 0;
    for (uint32_t c = 0; c < input.size() && input[c] != '='; ++c) {
      accumulator = (accumulator << 6) | decodeBase64Char(static_cast<unsigned char>(input[c]));
      bits += 6;
      if (bits >= 8) {
        bits -= 8;
        result.push_back(static_cast<unsigned char>(accumulator >> bits));
        accumulator &= ~(0xffffffff << bits);
      }
    }
    return result;
  }

  static const char* LA_VER1 =
      ("VFppZgAAAAAAAAAAAAAAAAAAAAAAAAAEAAAABAAAAAAAAAC5AAAABAAAABCepkig"
       "n7sVkKCGKqChmveQy4kaoNIj9HDSYSYQ1v50INiArZDa/tGg28CQENzes6DdqayQ3r6Vo"
       "N+JjpDgnneg4WlwkOJ+WaDjSVKQ5F47oOUpNJDmR1gg5xJREOgnOiDo8jMQ6gccIOrSFR"
       "Dr5v4g7LH3EO3G4CDukdkQ76/8oPBxuxDxj96g8n/BkPNvwKD0X6OQ9U+ioPY/hZD3L4S"
       "g+CiiEPkPZqD6CIQQ+viDIPvoZhD82GUg/chIEP64RyD/qCoQAJgpIAGIDBACeAsgA3Eo"
       "kARhJ6AFUQqQBkEJoAcw7JAHjUOgCRDOkAmtvyAK8LCQC+CvoAzZzRANwJGgDrmvEA+pr"
       "iAQmZEQEYmQIBJ5cxATaXIgFFlVEBVJVCAWOTcQFyk2IBgiU5AZCRggGgI1kBryNKAb4h"
       "eQHNIWoB3B+ZAesfigH6HbkCB2KyAhgb2QIlYNICNq2hAkNe8gJUq8ECYV0SAnKp4QJ/7"
       "toCkKgBAp3s+gKupiECu+saAs036QLZ6ToC6zYJAvfnWgMJNCkDFnkiAycySQM0d0IDRT"
       "BpA1J1YgNjLokDcHOCA4HAUQOOcaIDn75xA6xvwgO9vJEDywGKA9u6sQPo/6oD+bjRBAb"
       "9ygQYSpkEJPvqBDZIuQRC+goEVEbZBF89MgRy2KEEfTtSBJDWwQSbOXIErtThBLnLOgTN"
       "ZqkE18laBOtkyQT1x3oFCWLpBRPFmgUnYQkFMcO6BUVfKQVPwdoFY11JBW5TogWB7xEFj"
       "FHCBZ/tMQWqT+IFvetRBchOAgXb6XEF5kwiBfnnkQYE3eoGGHlZBiLcCgY2d3kGQNoqBl"
       "R1mQZe2EoGcnO5BnzWagaQcdkGmtSKBq5v+Qa5ZlIGzQHBBtdkcgbq/+EG9WKSBwj+AQc"
       "TYLIHJvwhBzFe0gdE+kEHT/CaB2OMCQdt7roHgYopB4vs2gefiEkHqer6B72GaQfH6RoH"
       "24SJB+XnOgf5gqkAABAAECAwEAAQABAAEAAQABAAEAAQABAAEAAQABAAEAAQABAAEAAQA"
       "BAAEAAQABAAEAAQABAAEAAQABAAEAAQABAAEAAQABAAEAAQABAAEAAQABAAEAAQABAAEA"
       "AQABAAEAAQABAAEAAQABAAEAAQABAAEAAQABAAEAAQABAAEAAQABAAEAAQABAAEAAQABA"
       "AEAAQABAAEAAQABAAEAAQABAAEAAQABAAEAAQABAAEAAQABAAEAAQAB//+dkAEA//+PgA"
       "AE//+dkAEI//+dkAEMUERUAFBTVABQV1QAUFBUAAAAAAEAAAAB");

  static const char* LA_VER2 =
      ("VFppZjIAAAAAAAAAAAAAAAAAAAAAAAAFAAAABQAAAAAAAAC6AAAABQAAABSAAAAAnqZIoJ"
       "+7FZCghiqgoZr3kMuJGqDSI/Rw0mEmENb+dCDYgK2Q2v7RoNvAkBDc3rOg3amskN6+laD"
       "fiY6Q4J53oOFpcJDiflmg40lSkOReO6DlKTSQ5kdYIOcSURDoJzog6PIzEOoHHCDq0hUQ"
       "6+b+IOyx9xDtxuAg7pHZEO+v/KDwcbsQ8Y/eoPJ/wZDzb8Cg9F+jkPVPoqD2P4WQ9y+Eo"
       "PgoohD5D2ag+giEEPr4gyD76GYQ/NhlIP3ISBD+uEcg/6gqEACYKSABiAwQAngLIANxKJ"
       "AEYSegBVEKkAZBCaAHMOyQB41DoAkQzpAJrb8gCvCwkAvgr6AM2c0QDcCRoA65rxAPqa4"
       "gEJmREBGJkCASeXMQE2lyIBRZVRAVSVQgFjk3EBcpNiAYIlOQGQkYIBoCNZAa8jSgG+IX"
       "kBzSFqAdwfmQHrH4oB+h25AgdisgIYG9kCJWDSAjatoQJDXvICVKvBAmFdEgJyqeECf+7"
       "aApCoAQKd7PoCrqYhArvrGgLNN+kC2ek6Aus2CQL351oDCTQpAxZ5IgMnMkkDNHdCA0Uw"
       "aQNSdWIDYy6JA3BzggOBwFEDjnGiA5++cQOsb8IDvbyRA8sBigPburED6P+qA/m40QQG/"
       "coEGEqZBCT76gQ2SLkEQvoKBFRG2QRfPTIEctihBH07UgSQ1sEEmzlyBK7U4QS5yzoEzW"
       "apBNfJWgTrZMkE9cd6BQli6QUTxZoFJ2EJBTHDugVFXykFT8HaBWNdSQVuU6IFge8RBYx"
       "RwgWf7TEFqk/iBb3rUQXITgIF2+lxBeZMIgX555EGBN3qBhh5WQYi3AoGNnd5BkDaKgZU"
       "dZkGXthKBnJzuQZ81moGkHHZBprUigaub/kGuWZSBs0BwQbXZHIG6v/hBvVikgcI/gEHE"
       "2CyByb8IQcxXtIHRPpBB0/wmgdjjAkHbe66B4GKKQeL7NoHn4hJB6nq+ge9hmkHx+kaB9"
       "uEiQfl5zoH+YKpACAQIBAgMEAgECAQIBAgECAQIBAgECAQIBAgECAQIBAgECAQIBAgECA"
       "QIBAgECAQIBAgECAQIBAgECAQIBAgECAQIBAgECAQIBAgECAQIBAgECAQIBAgECAQIBAg"
       "ECAQIBAgECAQIBAgECAQIBAgECAQIBAgECAQIBAgECAQIBAgECAQIBAgECAQIBAgECAQI"
       "BAgECAQIBAgECAQIBAgECAQIBAgECAQIBAgECAQIBAgECAQIBAgECAQL//5EmAAD//52Q"
       "AQT//4+AAAj//52QAQz//52QARBMTVQAUERUAFBTVABQV1QAUFBUAAAAAAABAAAAAAFUW"
       "mlmMgAAAAAAAAAAAAAAAAAAAAAAAAUAAAAFAAAAAAAAALsAAAAFAAAAFPgAAAAAAAAA//"
       "///14EGsD/////nqZIoP////+fuxWQ/////6CGKqD/////oZr3kP/////LiRqg/////9I"
       "j9HD/////0mEmEP/////W/nQg/////9iArZD/////2v7RoP/////bwJAQ/////9zes6D/"
       "////3amskP/////evpWg/////9+JjpD/////4J53oP/////haXCQ/////+J+WaD/////4"
       "0lSkP/////kXjug/////+UpNJD/////5kdYIP/////nElEQ/////+gnOiD/////6PIzEP"
       "/////qBxwg/////+rSFRD/////6+b+IP/////ssfcQ/////+3G4CD/////7pHZEP/////"
       "vr/yg//////BxuxD/////8Y/eoP/////yf8GQ//////NvwKD/////9F+jkP/////1T6Kg"
       "//////Y/hZD/////9y+EoP/////4KKIQ//////kPZqD/////+giEEP/////6+IMg/////"
       "/voZhD//////NhlIP/////9yEgQ//////64RyD//////6gqEAAAAAAAmCkgAAAAAAGIDB"
       "AAAAAAAngLIAAAAAADcSiQAAAAAARhJ6AAAAAABVEKkAAAAAAGQQmgAAAAAAcw7JAAAAA"
       "AB41DoAAAAAAJEM6QAAAAAAmtvyAAAAAACvCwkAAAAAAL4K+gAAAAAAzZzRAAAAAADcCR"
       "oAAAAAAOua8QAAAAAA+priAAAAAAEJmREAAAAAARiZAgAAAAABJ5cxAAAAAAE2lyIAAAA"
       "AAUWVUQAAAAABVJVCAAAAAAFjk3EAAAAAAXKTYgAAAAABgiU5AAAAAAGQkYIAAAAAAaAj"
       "WQAAAAABryNKAAAAAAG+IXkAAAAAAc0hagAAAAAB3B+ZAAAAAAHrH4oAAAAAAfoduQAAA"
       "AACB2KyAAAAAAIYG9kAAAAAAiVg0gAAAAACNq2hAAAAAAJDXvIAAAAAAlSrwQAAAAACYV"
       "0SAAAAAAJyqeEAAAAAAn/u2gAAAAACkKgBAAAAAAKd7PoAAAAAAq6mIQAAAAACu+saAAA"
       "AAALNN+kAAAAAAtnpOgAAAAAC6zYJAAAAAAL351oAAAAAAwk0KQAAAAADFnkiAAAAAAMn"
       "MkkAAAAAAzR3QgAAAAADRTBpAAAAAANSdWIAAAAAA2MuiQAAAAADcHOCAAAAAAOBwFEAA"
       "AAAA45xogAAAAADn75xAAAAAAOsb8IAAAAAA728kQAAAAADywGKAAAAAAPburEAAAAAA+"
       "j/qgAAAAAD+bjRAAAAAAQG/coAAAAABBhKmQAAAAAEJPvqAAAAAAQ2SLkAAAAABEL6CgA"
       "AAAAEVEbZAAAAAARfPTIAAAAABHLYoQAAAAAEfTtSAAAAAASQ1sEAAAAABJs5cgAAAAAE"
       "rtThAAAAAAS5yzoAAAAABM1mqQAAAAAE18laAAAAAATrZMkAAAAABPXHegAAAAAFCWLpA"
       "AAAAAUTxZoAAAAABSdhCQAAAAAFMcO6AAAAAAVFXykAAAAABU/B2gAAAAAFY11JAAAAAA"
       "VuU6IAAAAABYHvEQAAAAAFjFHCAAAAAAWf7TEAAAAABapP4gAAAAAFvetRAAAAAAXITgI"
       "AAAAABdvpcQAAAAAF5kwiAAAAAAX555EAAAAABgTd6gAAAAAGGHlZAAAAAAYi3AoAAAAA"
       "BjZ3eQAAAAAGQNoqAAAAAAZUdZkAAAAABl7YSgAAAAAGcnO5AAAAAAZ81moAAAAABpBx2"
       "QAAAAAGmtSKAAAAAAaub/kAAAAABrlmUgAAAAAGzQHBAAAAAAbXZHIAAAAABur/4QAAAA"
       "AG9WKSAAAAAAcI/gEAAAAABxNgsgAAAAAHJvwhAAAAAAcxXtIAAAAAB0T6QQAAAAAHT/C"
       "aAAAAAAdjjAkAAAAAB23uugAAAAAHgYopAAAAAAeL7NoAAAAAB5+ISQAAAAAHqer6AAAA"
       "AAe9hmkAAAAAB8fpGgAAAAAH24SJAAAAAAfl5zoAAAAAB/mCqQAAIBAgECAwQCAQIBAgE"
       "CAQIBAgECAQIBAgECAQIBAgECAQIBAgECAQIBAgECAQIBAgECAQIBAgECAQIBAgECAQIB"
       "AgECAQIBAgECAQIBAgECAQIBAgECAQIBAgECAQIBAgECAQIBAgECAQIBAgECAQIBAgECA"
       "QIBAgECAQIBAgECAQIBAgECAQIBAgECAQIBAgECAQIBAgECAQIBAgECAQIBAgECAQIBAg"
       "ECAQIBAgECAQIBAgECAQIBAv//kSYAAP//nZABBP//j4AACP//nZABDP//nZABEExNVAB"
       "QRFQAUFNUAFBXVABQUFQAAAAAAAEAAAAAAQpQU1Q4UERULE0zLjIuMCxNMTEuMS4wCg==");

  const std::string& getVariantFromZone(const Timezone& zone, const std::string& date) {
    tm timeStruct;
    if (strptime(date.c_str(), "%Y-%m-%d %H:%M:%S", &timeStruct) == nullptr) {
      throw TimezoneError("bad time " + date);
    }
    return zone.getVariant(timegm(&timeStruct)).name;
  }

  TEST(TestTimezone, testParser) {
    std::unique_ptr<Timezone> la = getTimezone("America/Los_Angeles", decodeBase64(LA_VER1));
    EXPECT_EQ(1, la->getVersion());
    EXPECT_EQ("PST", getVariantFromZone(*la, "1974-01-06 09:59:59"));
    EXPECT_EQ("PDT", getVariantFromZone(*la, "1974-01-06 10:00:00"));
    // v1 won't have information past 2038
    EXPECT_EQ("PST", getVariantFromZone(*la, "2100-03-14 09:59:59"));
    EXPECT_EQ("PST", getVariantFromZone(*la, "2100-03-14 10:00:00"));

    la = getTimezone("America/Los_Angeles", decodeBase64(LA_VER2));
    EXPECT_EQ(2, la->getVersion());
    EXPECT_EQ("PST", getVariantFromZone(*la, "1974-01-06 09:59:59"));
    EXPECT_EQ("PDT", getVariantFromZone(*la, "1974-01-06 10:00:00"));
    EXPECT_EQ("PST", getVariantFromZone(*la, "2100-03-14 09:59:59"));
    EXPECT_EQ("PDT", getVariantFromZone(*la, "2100-03-14 10:00:00"));
  }

  TEST(TestTimezone, testZoneCache) {
    const Timezone* la1 = &getTimezoneByName("America/Los_Angeles");
    const Timezone* ny1 = &getTimezoneByName("America/New_York");
    const Timezone* la2 = &getTimezoneByName("America/Los_Angeles");
    const Timezone* ny2 = &getTimezoneByName("America/New_York");
    EXPECT_EQ(la1, la2);
    EXPECT_EQ(ny1, ny2);
    EXPECT_EQ("PST", getVariantFromZone(*la1, "1974-01-06 09:59:59"));
    EXPECT_EQ("PDT", getVariantFromZone(*la1, "1974-01-06 10:00:00"));
    EXPECT_EQ("PDT", getVariantFromZone(*la1, "1974-10-27 08:59:59"));
    EXPECT_EQ("PST", getVariantFromZone(*la1, "1974-10-27 09:00:00"));
    EXPECT_EQ("EST", getVariantFromZone(*ny1, "1974-01-06 06:59:59"));
    EXPECT_EQ("EDT", getVariantFromZone(*ny1, "1974-01-06 07:00:00"));
    EXPECT_EQ("EDT", getVariantFromZone(*ny1, "1974-10-27 05:59:59"));
    EXPECT_EQ("EST", getVariantFromZone(*ny1, "1974-10-27 06:00:00"));
  }

  TEST(TestTimezone, testGMTv1) {
    const char GMT[] =
        ("VFppZgAAAAAAAAAAAAAAAAAAAAAAAAABAAAAAQAAAA"
         "AAAAAAAAAAAQAAAAQAAAAAAABHTVQAAAA=");
    std::unique_ptr<Timezone> gmt = getTimezone("GMT", decodeBase64(GMT));
    EXPECT_EQ(1, gmt->getVersion());
    EXPECT_EQ("GMT", getVariantFromZone(*gmt, "1974-01-06 09:59:59"));
    EXPECT_EQ("GMT", getVariantFromZone(*gmt, "2015-06-06 12:34:56"));
  }
}  // namespace orc
