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

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include <cstdint>
#include <cstdio>
#include <iostream>

#include "gtest/gtest_pred_impl.h"
#include "olap/rowset/segment_v2/ngram_bloom_filter.h"
#include "testutil/any_type.h"
#include "util/hash/city.h"

namespace doris {

void test(uint64_t result, uint64_t result_with_seed, const std::string& str) {
    EXPECT_EQ(result, util_hash::CityHash64(str.data(), str.size()));
    EXPECT_EQ(result_with_seed, util_hash::CityHash64WithSeed(str.data(), str.size(), SEED_GEN));
}

TEST(city_hash, simple) {
    test(200641142423602673ULL, 14284967837249502203ULL, "Testing 1, 2, 3");
    test(15001808493323962774ULL, 11424039407453860401ULL, "Moscow");
    test(8034095133228533394ULL, 18132648334508667393ULL, "Moscow is the capital of Russia");
    test(13956251726120965457ULL, 1381326629274895150ULL,
         "Moscow is the capital of Russia, and the largest city in the country");
    test(2261632006729419242ULL, 17084064757690202946ULL,
         "Moscow is the capital of Russia, and the largest city in the country, with a population "
         "of over 12 million people");
    test(6469445502724539237ULL, 11343158389890087495ULL,
         "Moscow is the capital of Russia, and the largest city in the country, with a population "
         "of over 12 million people, and the largest city in Europe");
    test(4443578057564282258ULL, 8388333080872437400ULL, "one");
}

TEST(city_hash, long_text) {
    test(2603920979253371939ULL, 17286059257277065140ULL,
         "Testing 1, 2, 3Testing 1, 2, 3Testing 1, 2, 3Testing 1, 2, 3Testing 1, 2, 3Testing 1, 2, "
         "3Testing 1, 2, 3Testing 1, 2, 3Testing 1, 2, 3Testing 1, 2, 3Testing 1, 2, 3Testing 1, "
         "2, 3");
    test(3019762420123308560ULL, 17640672461655093518ULL,
         "MoscowMoscowMoscowMoscowMoscowMoscowMoscowMoscowMoscowMoscowMoscowMoscowMoscowMoscowMosco"
         "wMoscowMoscowMoscowMoscowMoscowMoscowMoscowMoscowMoscowMoscowMoscowMoscowMoscowMoscowMosc"
         "owMoscowMoscowMoscowMoscowMoscowMoscowMoscowMoscowMoscowMoscowMoscowMoscowMoscowMoscowMos"
         "cowMoscowMoscowMoscowMoscowMoscowMoscowMoscowMoscowMoscowMoscowMoscowMoscowMoscowMoscowMo"
         "scowMoscowMoscowMoscowMoscowMoscowMoscowMoscowMoscowMoscowMoscowMoscowMoscowMoscowMoscowM"
         "oscowMoscowMoscowMoscowMoscowMoscowMoscowMoscowMoscowMoscowMoscowMoscowMoscowMoscowMoscow"
         "MoscowMoscowMoscowMoscowMoscowMoscowMoscowMoscowMoscowMoscowMoscowMoscowMoscowMoscowMosco"
         "wMoscowMoscowMoscowMoscowMoscowMoscowMoscow");
    test(15852398506477355940ULL, 7555841253624036420ULL,
         "Moscow is the capital of RussiaMoscow is the capital of RussiaMoscow is the capital of "
         "RussiaMoscow is the capital of RussiaMoscow is the capital of RussiaMoscow is the "
         "capital of RussiaMoscow is the capital of RussiaMoscow is the capital of RussiaMoscow is "
         "the capital of RussiaMoscow is the capital of RussiaMoscow is the capital of "
         "RussiaMoscow is the capital of RussiaMoscow is the capital of RussiaMoscow is the "
         "capital of RussiaMoscow is the capital of RussiaMoscow is the capital of RussiaMoscow is "
         "the capital of RussiaMoscow is the capital of RussiaMoscow is the capital of "
         "RussiaMoscow is the capital of RussiaMoscow is the capital of RussiaMoscow is the "
         "capital of RussiaMoscow is the capital of RussiaMoscow is the capital of RussiaMoscow is "
         "the capital of RussiaMoscow is the capital of RussiaMoscow is the capital of "
         "RussiaMoscow is the capital of RussiaMoscow is the capital of Russia");
    test(1035060799968064901ULL, 16530320359523728196ULL,
         "Moscow is the capital of Russia, and the largest city in the countryMoscow is the "
         "capital of Russia, and the largest city in the countryMoscow is the capital of Russia, "
         "and the largest city in the countryMoscow is the capital of Russia, and the largest city "
         "in the countryMoscow is the capital of Russia, and the largest city in the countryMoscow "
         "is the capital of Russia, and the largest city in the countryMoscow is the capital of "
         "Russia, and the largest city in the countryMoscow is the capital of Russia, and the "
         "largest city in the countryMoscow is the capital of Russia, and the largest city in the "
         "countryMoscow is the capital of Russia, and the largest city in the countryMoscow is the "
         "capital of Russia, and the largest city in the countryMoscow is the capital of Russia, "
         "and the largest city in the countryMoscow is the capital of Russia, and the largest city "
         "in the countryMoscow is the capital of Russia, and the largest city in the countryMoscow "
         "is the capital of Russia, and the largest city in the countryMoscow is the capital of "
         "Russia, and the largest city in the countryMoscow is the capital of Russia, and the "
         "largest city in the countryMoscow is the capital of Russia, and the largest city in the "
         "countryMoscow is the capital of Russia, and the largest city in the countryMoscow is the "
         "capital of Russia, and the largest city in the countryMoscow is the capital of Russia, "
         "and the largest city in the countryMoscow is the capital of Russia, and the largest city "
         "in the countryMoscow is the capital of Russia, and the largest city in the countryMoscow "
         "is the capital of Russia, and the largest city in the countryMoscow is the capital of "
         "Russia, and the largest city in the countryMoscow is the capital of Russia, and the "
         "largest city in the countryMoscow is the capital of Russia, and the largest city in the "
         "countryMoscow is the capital of Russia, and the largest city in the country");
    test(6905655253535205836ULL, 5560247534938243804ULL,
         "Moscow is the capital of Russia, and the largest city in the country, with a population "
         "of over 12 million peopleMoscow is the capital of Russia, and the largest city in the "
         "country, with a population "
         "of over 12 million peopleMoscow is the capital of Russia, and the largest city in the "
         "country, with a population "
         "of over 12 million peopleMoscow is the capital of Russia, and the largest city in the "
         "country, with a population "
         "of over 12 million peopleMoscow is the capital of Russia, and the largest city in the "
         "country, with a population "
         "of over 12 million peopleMoscow is the capital of Russia, and the largest city in the "
         "country, with a population "
         "of over 12 million peopleMoscow is the capital of Russia, and the largest city in the "
         "country, with a population "
         "of over 12 million peopleMoscow is the capital of Russia, and the largest city in the "
         "country, with a population "
         "of over 12 million peopleMoscow is the capital of Russia, and the largest city in the "
         "country, with a population "
         "of over 12 million peopleMoscow is the capital of Russia, and the largest city in the "
         "country, with a population "
         "of over 12 million peopleMoscow is the capital of Russia, and the largest city in the "
         "country, with a population "
         "of over 12 million peopleMoscow is the capital of Russia, and the largest city in the "
         "country, with a population "
         "of over 12 million peopleMoscow is the capital of Russia, and the largest city in the "
         "country, with a population "
         "of over 12 million peopleMoscow is the capital of Russia, and the largest city in the "
         "country, with a population "
         "of over 12 million peopleMoscow is the capital of Russia, and the largest city in the "
         "country, with a population "
         "of over 12 million peopleMoscow is the capital of Russia, and the largest city in the "
         "country, with a population "
         "of over 12 million peopleMoscow is the capital of Russia, and the largest city in the "
         "country, with a population "
         "of over 12 million peopleMoscow is the capital of Russia, and the largest city in the "
         "country, with a population "
         "of over 12 million peopleMoscow is the capital of Russia, and the largest city in the "
         "country, with a population "
         "of over 12 million peopleMoscow is the capital of Russia, and the largest city in the "
         "country, with a population "
         "of over 12 million peopleMoscow is the capital of Russia, and the largest city in the "
         "country, with a population "
         "of over 12 million peopleMoscow is the capital of Russia, and the largest city in the "
         "country, with a population "
         "of over 12 million peopleMoscow is the capital of Russia, and the largest city in the "
         "country, with a population "
         "of over 12 million peopleMoscow is the capital of Russia, and the largest city in the "
         "country, with a population "
         "of over 12 million peopleMoscow is the capital of Russia, and the largest city in the "
         "country, with a population "
         "of over 12 million peopleMoscow is the capital of Russia, and the largest city in the "
         "country, with a population "
         "of over 12 million peopleMoscow is the capital of Russia, and the largest city in the "
         "country, with a population "
         "of over 12 million peopleMoscow is the capital of Russia, and the largest city in the "
         "country, with a population "
         "of over 12 million peopleMoscow is the capital of Russia, and the largest city in the "
         "country, with a population "
         "of over 12 million people");
    test(17180533981300533381ULL, 985389469184223326ULL,
         "Moscow is the capital of Russia, and the largest city in the country, with a population "
         "of over 12 million people, and the largest city in EuropeMoscow is the capital of "
         "Russia, and the largest city in the country, with a population "
         "of over 12 million people, and the largest city in EuropeMoscow is the capital of "
         "Russia, and the largest city in the country, with a population "
         "of over 12 million people, and the largest city in EuropeMoscow is the capital of "
         "Russia, and the largest city in the country, with a population "
         "of over 12 million people, and the largest city in EuropeMoscow is the capital of "
         "Russia, and the largest city in the country, with a population "
         "of over 12 million people, and the largest city in EuropeMoscow is the capital of "
         "Russia, and the largest city in the country, with a population "
         "of over 12 million people, and the largest city in EuropeMoscow is the capital of "
         "Russia, and the largest city in the country, with a population "
         "of over 12 million people, and the largest city in EuropeMoscow is the capital of "
         "Russia, and the largest city in the country, with a population "
         "of over 12 million people, and the largest city in EuropeMoscow is the capital of "
         "Russia, and the largest city in the country, with a population "
         "of over 12 million people, and the largest city in EuropeMoscow is the capital of "
         "Russia, and the largest city in the country, with a population "
         "of over 12 million people, and the largest city in EuropeMoscow is the capital of "
         "Russia, and the largest city in the country, with a population "
         "of over 12 million people, and the largest city in EuropeMoscow is the capital of "
         "Russia, and the largest city in the country, with a population "
         "of over 12 million people, and the largest city in EuropeMoscow is the capital of "
         "Russia, and the largest city in the country, with a population "
         "of over 12 million people, and the largest city in EuropeMoscow is the capital of "
         "Russia, and the largest city in the country, with a population "
         "of over 12 million people, and the largest city in EuropeMoscow is the capital of "
         "Russia, and the largest city in the country, with a population "
         "of over 12 million people, and the largest city in EuropeMoscow is the capital of "
         "Russia, and the largest city in the country, with a population "
         "of over 12 million people, and the largest city in EuropeMoscow is the capital of "
         "Russia, and the largest city in the country, with a population "
         "of over 12 million people, and the largest city in EuropeMoscow is the capital of "
         "Russia, and the largest city in the country, with a population "
         "of over 12 million people, and the largest city in EuropeMoscow is the capital of "
         "Russia, and the largest city in the country, with a population "
         "of over 12 million people, and the largest city in EuropeMoscow is the capital of "
         "Russia, and the largest city in the country, with a population "
         "of over 12 million people, and the largest city in EuropeMoscow is the capital of "
         "Russia, and the largest city in the country, with a population "
         "of over 12 million people, and the largest city in Europe");
    test(15470213656373493785ULL, 18337102184905563192ULL,
         "ooneoneoneoneoneoneoneoneoneoneoneoneoneoneoneoneoneoneoneoneoneoneoneoneoneoneoneoneoneo"
         "neoneoneoneoneoneoneoneoneoneoneoneoneoneoneoneoneoneoneoneoneoneoneoneoneoneoneoneoneone"
         "oneoneoneoneoneoneoneoneoneoneoneoneoneoneoneoneoneoneoneoneoneoneoneoneoneoneoneoneoneon"
         "eoneoneonene");
}

TEST(city_hash, short_text8) {
    test(2548571168015014692ULL, 284905140919628274ULL, "a");
    test(2070361765919599426ULL, 9025766345180298939ULL, "bb");
    test(14141338468832955152ULL, 10552519853845467035ULL, "ccc");
    test(18138810160329292616ULL, 5334357042905201868ULL, "dddd");
    test(425873312578527799ULL, 17471752968193695125ULL, "aaaaa");
    test(16631617225600905903ULL, 8316778257275536170ULL, "aaaccc");
    test(12719358095561328225ULL, 17872985103380866073ULL, "abcdefh");
    test(17174388733642667936ULL, 11830283950305971712ULL, "abcdeffh");
}

TEST(city_hash, short_text16) {
    test(5762505351139159907ULL, 5576230494162395002ULL, "a32146547");
    test(5384428512347131045ULL, 15753090810004664388ULL, "b32146547b");
    test(12829545899344666879ULL, 3848967114221885261ULL, "c32146547cc");
    test(16893894660113381599ULL, 5947978601334952997ULL, "dd32146547dd");
    test(17429098434560484596ULL, 6398107568024805321ULL, "aaaa32146547a");
    test(10726875353743957364ULL, 2240715260928806674ULL, "aaa32146547ccc");
    test(6679348617805124815ULL, 9286469086836368089ULL, "abc32146547defh");
    test(6691049816525186727ULL, 7961087717381234556ULL, "abcd32146547effh");
}

} // namespace doris
