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

suite("test_multi_string_search") {
    qt_select "select multi_match_any('mpnsguhwsitzvuleiwebwjfitmsg', ['wbirxqoabpblrnvvmjizj', 'cfcxhuvrexyzyjsh', 'oldhtubemyuqlqbwvwwkwin', 'bumoozxdkjglzu', 'intxlfohlxmajjomw', 'dxkeghohv', 'arsvmwwkjeopnlwnan', 'ouugllgowpqtaxslcopkytbfhifaxbgt', 'hkedmjlbcrzvryaopjqdjjc', 'tbqkljywstuahzh', 'o', 'wowoclosyfcuwotmvjygzuzhrery', 'vpefjiffkhlggntcu', 'ytdixvasrorhripzfhjdmlhqksmctyycwp'])"
    qt_select "select multi_match_any('qjjzqexjpgkglgxpzrbqbnskq', ['vaiatcjacmlffdzsejpdareqzy', 'xspcfzdufkmecud', 'bcvtbuqtctq', 'nkcopwbfytgemkqcfnnno', 'dylxnzuyhq', 'tno', 'scukuhufly', 'cdyquzuqlptv', 'ohluyfeksyxepezdhqmtfmgkvzsyph', 'ualzwtahvqvtijwp', 'jg', 'gwbawqlngzcknzgtmlj', 'qimvjcgbkkp', 'eaedbcgyrdvv', 'qcwrncjoewwedyyewcdkh', 'uqcvhngoqngmitjfxpznqomertqnqcveoqk', 'ydrgjiankgygpm', 'axepgap'])"
    qt_select "select multi_match_any('fdkmtqmxnegwvnjhghjq', ['vynkybvdmhgeezybbdqfrukibisj', 'knazzamgjjpavwhvdkwigykh', 'peumnifrmdhhmrqqnemw', 'lmsnyvqoisinlaqobxojlwfbi', 'oqwfzs', 'dymudxxeodwjpgbibnkvr', 'vomtfsnizkplgzktqyoiw', 'yoyfuhlpgrzds', 'cefao', 'gi', 'srpgxfjwl', 'etsjusdeiwbfe', 'ikvtzdopxo', 'ljfkavrau', 'soqdhxtenfrkmeic', 'ktprjwfcelzbup', 'pcvuoddqwsaurcqdtjfnczekwni', 'agkqkqxkfbkfgyqliahsljim'])"
    qt_select "select multi_match_any('khljxzxlpcrxpkrfybbfk', ['', 'lpc', 'rxpkrfybb', 'crxp', '', 'pkr', 'jxzxlpcrxpkrf', '', 'xzxlpcr', 'xpk', 'fyb', 'xzxlpcrxpkrfybbfk', 'k', 'lpcrxp', 'ljxzxlpcr', 'r', 'pkr', 'fk'])"
    qt_select "select multi_match_any('rbrizgjbigvzfnpgmpkqxoqxvdj', ['ee', 'cohqnb', 'msol', 'yhlujcvhklnhuomy', 'ietn', 'vgmnlkcsybtokrepzrm', 'wspiryefojxysgrzsxyrluykxfnnbzdstcel', 'mxisnsivndbefqxwznimwgazuulupbaihavg', 'vpzdjvqqeizascxmzdhuq', 'pgvncohlxcqjhfkm', 'mbaypcnfapltsegquurahlsruqvipfhrhq', 'ioxjbcyyqujfveujfhnfdfokfcrlsincjbdt', 'cnvlujyowompdrqjwjx', 'wobwed', 'kdfhaoxiuifotmptcmdbk', 'leoamsnorcvtlmokdomkzuo', 'jjw', 'ogugysetxuqmvggneosbsfbonszepsatq'])"
    qt_select "select multi_match_any('uymwxzyjbfegbhgswiqhinf', ['lizxzbzlwljkr', 'ukxygktlpzuyijcqeqktxenlaqi', 'onperabgbdiafsxwbvpjtyt', 'xfqgoqvhqph', 'aflmcwabtwgmajmmqelxwkaolyyhmdlc', 'yfz', 'meffuiaicvwed', 'hhzvgmifzamgftkifaeowayjrnnzw', 'nwewybtajv', 'ectiye', 'epjeiljegmqqjncubj', 'zsjgftqjrn', 'pssng', 'raqoarfhdoeujulvqmdo'])"
    qt_select "select multi_match_any('omgghgnzjmecpzqmtcvw', ['fjhlzbszodmzavzg', 'gfofrnwrxprkfiokv', 'jmjiiqpgznlmyrxwewzqzbe', 'pkyrsqkltlmxr', 'crqgkgqkkyujcyoc', 'endagbcxwqhueczuasykmajfsvtcmh', 'xytmxtrnkdysuwltqomehddp', 'etmdxyyfotfyifwvbykghijvwv', 'mwqtgrncyhkfhjdg', 'iuvymofrqpp', 'pgllsdanlhzqhkstwsmzzftp', 'disjylcceufxtjdvhy'])"
    qt_select "select multi_match_any('mznihnmshftvnmmhnrulizzpslq', ['nrul', 'mshftvnmmhnr', 'z', 'mhnrulizzps', 'hftvnmmhnrul', 'ihnmshftvnmmhnrulizzp', 'izz', '', 'uli', 'nihnmshftvnmmhnru', 'hnrulizzp', 'nrulizz'])"
    qt_select "select multi_match_any('ruqmqrsxrbftvruvahonradau', ['uqmqrsxrbft', 'ftv', 'tvruvahonrad', 'mqrsxrbftvruvahon', 'rbftvruvah', 'qrsxrbftvru', 'o', 'ahonradau', 'a', 'ft', '', 'u', 'rsxrbftvruvahonradau', 'ruvahon', 'bftvruvahonradau', 'qrsxrbftvru', 't', 'vahonrada', 'vruvahonradau', 'onra'])"
    qt_select "select multi_match_any('gpsevxtcoeexrltyzduyidmtzxf', ['exrltyzduyid', 'vxtcoeexrltyz', 'xr', 'ltyzduyidmt', 'yzduy', 'exr', 'coeexrltyzduy', 'coeexrltyzduy', 'rlty', 'rltyzduyidm', 'exrltyz', 'xtcoeexrlty', 'vxtcoeexrltyzduyidm', '', 'coeexrl', 'sevxtcoeexrltyzdu', 'dmt', ''])"
    qt_select "select multi_match_any('dyhycfhzyewaikgursyxfkuv', ['sktnofpugrmyxmbizzrivmhn', 'fhlgadpoqcvktbfzncxbllvwutdawmw', 'eewzjpcgzrqmltbgmhafwlwqb', 'tpogbkyj', 'rtllntxjgkzs', 'mirbvsqexscnzglogigbujgdwjvcv', 'iktwpgjsakemewmahgqza', 'xgfvzkvqgiuoihjjnxwwpznxhz', 'nxaumpaknreklbwynvxdsmatjekdlxvklh', 'zadzwqhgfxqllihuudozxeixyokhny', 'tdqpgfpzexlkslodps', 'slztannufxaabqfcjyfquafgfhfb', 'xvjldhfuwurvkb', 'aecv', 'uycfsughpikqsbcmwvqygdyexkcykhbnau', 'jr'])"
    qt_select "select multi_match_any('vbcsettndwuntnruiyclvvwoo', ['dwuntnru', '', 'ttndwuntnruiyclvv', 'ntnr', 'nruiyclvvw', 'wo', '', 'bcsettndwuntnruiycl', 'yc', 'untnruiyclvvw', 'csettndwuntnr', 'ntnruiyclvvwo'])"
    qt_select "select multi_match_any('pqqnugshlczcuxhpjxjbcnro', ['dpeedqy', 'rtsc', 'jdgla', 'qkgudqjiyzvlvsj', 'xmfxawhijgxxtydbd', 'ebgzazqthb', 'wyrjhvhwzhmpybnylirrn', 'iviqbyuclayqketooztwegtkgwnsezfl', 'bhvidy', 'hijctxxweboq', 't', 'osnzfbziidteiaifgaanm'])"
    qt_select "select multi_match_any('loqchlxspwuvvccucskuytr', ['', 'k', 'qchlxspwu', 'u', 'hlxspwuvv', 'wuvvccucsku', 'vcc', 'uyt', 'uvv', 'spwu', 'ytr', 'wuvvccucs', 'xspwuv', 'lxspwuvvccuc', 'spwuvvccu', 'oqchlxspwuvvccucskuy'])"
    qt_select "select multi_match_any('pjjyzupzwllshlnatiujmwvaofr', ['lnatiujmwvao', '', 'zupzwllsh', 'nati', 'wllshl', 'hlnatiujmwv', 'mwvao', 'shlnat', 'ati', 'wllshlnatiujmwvao', 'wllshlnatiujmwvaofr', 'nat'])"
    qt_select "select multi_match_any('iketunkleyaqaxdlocci', ['nkleyaqaxd', 'etunkleyaq', 'yaqaxdlocci', 'tunkleyaq', 'eyaqaxdlocc', 'leyaq', 'nkleyaqaxdl', 'tunkleya', 'kleyaqa', 'etunkleya', 'leyaqa', 'dlo', 'yaqa', 'leyaqaxd', 'etunkleyaq', ''])"
    qt_select "select multi_match_any('drqianqtangmgbdwruvblkqd', ['wusajejyucamkyl', 'wsgibljugzrpkniliy', 'lhwqqiuafwffyersqjgjvvvfurx', 'jfokpzzxfdonelorqu', 'ccwkpcgac', 'jmyulqpndkmzbfztobwtm', 'rwrgfkccgxht', 'ggldjecrgbngkonphtcxrkcviujihidjx', 'spwweavbiokizv', 'lv', 'krb', 'vstnhvkbwlqbconaxgbfobqky', 'pvxwdc', 'thrl', 'ahsblffdveamceonqwrbeyxzccmux', 'yozji', 'oejtaxwmeovtqtz', 'zsnzznvqpxdvdxhznxrjn', 'hse', 'kcmkrccxmljzizracxwmpoaggywhdfpxkq'])"
    qt_select "select multi_match_any('yasnpckniistxcejowfijjsvkdajz', ['slkpxhtsmrtvtm', 'crsbq', 'rdeshtxbfrlfwpsqojassxmvlfbzefldavmgme', 'ipetilcbpsfroefkjirquciwtxhrimbmwnlyv', 'knjpwkmdwbvdbapuyqbtsw', 'horueidziztxovqhsicnklmharuxhtgrsr', 'ofohrgpz', 'oneqnwyevbaqsonrcpmxcynflojmsnix', 'shg', 'nglqzczevgevwawdfperpeytuodjlf'])"
    qt_select "select multi_match_any('ueptpscfgxhplwsueckkxs', ['ohhygchclbpcdwmftperprn', 'dvpjdqmqckekndvcerqrpkxen', 'lohhvarnmyi', 'zppd', 'qmqxgfewitsunbuhffozcpjtc', 'hsjbioisycsrawktqssjovkmltxodjgv', 'dbzuunwbkrtosyvctdujqtvaawfnvuq', 'gupbvpqthqxae', 'abjdmijaaiasnccgxttmqdsz', 'uccyumqoyqe', 'kxxliepyzlc', 'wbqcqtbyyjbqcgdbpkmzugksmcxhvr', 'piedxm', 'uncpphzoif', 'exkdankwck', 'qeitzozdrqopsergzr', 'hesgrhaftgesnzflrrtjdobxhbepjoas', 'wfpexx'])"
    qt_select "select multi_match_any('ldrzgttlqaphekkkdukgngl', ['gttlqaphekkkdukgn', 'ekkkd', 'gttlqaphe', 'qaphek', 'h', 'kdu', 'he', 'phek', '', 'drzgttlqaphekkkd'])"
    qt_select "select multi_match_any('ololo', ['ololo', 'ololo', 'ololo'])"
    qt_select "select multi_match_any('khljxzxlpcrxpkrfybbfk', ['k'])"
    qt_select "select multi_match_any(NULL, ['A', 'bcd']);"
    qt_select "select multi_match_any('abc', NULL);"
    qt_select "select multi_match_any(NULL, NULL);"

    sql """drop table if exists test_multi_string_search_test;"""

    sql """CREATE TABLE `test_multi_string_search_test` (
                `id` int(11) NULL COMMENT "",
                `c_array` string NULL COMMENT "",
                `c_array1` array<string> NULL COMMENT ""
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`)
            COMMENT "OLAP"
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "in_memory" = "false",
            "storage_format" = "V2"
            );
        """

    sql """INSERT INTO `test_multi_string_search_test` VALUES (1, "['1','2','3','4','5']", ['1','2','3','4','5']);"""
    sql """INSERT INTO `test_multi_string_search_test` VALUES (2, NULL, ['1','2','3','4']);"""
    sql """INSERT INTO `test_multi_string_search_test` VALUES (3, "['1','2','3', 5']", NULL);"""
    sql """INSERT INTO `test_multi_string_search_test` VALUES (4,'{"question12":{"answer":["1","2","4"],"content":"自己输入的内容"}}', NULL);"""

    qt_select "select *,multi_match_any(c_array,c_array1) from test_multi_string_search_test order by id;"

    qt_select "select *,multi_match_any(c_array,['2']) from test_multi_string_search_test order by id;"

    qt_select "select *,multi_match_any(c_array,['10']) from test_multi_string_search_test order by id;"

    qt_select "select * from test_multi_string_search_test where 1 = multi_match_any(jsonb_extract_string(c_array, '\$.question12.answer'),['3']) order by id;"

    qt_select "select * from test_multi_string_search_test where 1 = multi_match_any(jsonb_extract_string(c_array, '\$.question12.answer'),['2']) order by id;"

    sql """drop table if exists test_multi_string_search_test;"""

    sql """CREATE TABLE `test_multi_string_search_test` (
                `id` int(11) NULL COMMENT "",
                `c_array` string not NULL COMMENT "",
                `c_array1` array<string> not NULL COMMENT ""
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`)
            COMMENT "OLAP"
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "in_memory" = "false",
            "storage_format" = "V2"
            );
        """

    sql """INSERT INTO `test_multi_string_search_test` VALUES (1, "['1','2','3','4','5']", ['1','2','3','4','5']);"""
    sql """INSERT INTO `test_multi_string_search_test` VALUES (2, "['100','200']", ['1','2','3','4']);"""
    sql """INSERT INTO `test_multi_string_search_test` VALUES (3, "['1','2','3', 5']", ['100','200']);"""
    sql """INSERT INTO `test_multi_string_search_test` VALUES (4,'{"question12":{"answer":["1","2","4"],"content":"自己输入的内容"}}', ['0']);"""

    qt_select "select *,multi_match_any(c_array,c_array1) from test_multi_string_search_test order by id;"

    qt_select "select *,multi_match_any(c_array,['2']) from test_multi_string_search_test order by id;"

    qt_select "select *,multi_match_any(c_array,['10']) from test_multi_string_search_test order by id;"

    qt_select "select * from test_multi_string_search_test where 1 = multi_match_any(jsonb_extract_string(c_array, '\$.question12.answer'),['3']) order by id;"

    qt_select "select * from test_multi_string_search_test where 1 = multi_match_any(jsonb_extract_string(c_array, '\$.question12.answer'),['2']) order by id;"

    sql """drop table if exists test_multi_string_search_test;"""
}
