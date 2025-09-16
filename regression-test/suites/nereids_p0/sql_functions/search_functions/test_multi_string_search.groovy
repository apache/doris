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
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"

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

    test {
        sql '''
            select MULTI_MATCH_ANY(cast('aaaaa' as STRING), cast(['aaaaa', '12345!@#$%^&*()_', '', '中文测试', '123', 'abs ss 123'] as ARRAY < STRING > ));
        '''
        exception "some expressions may be illegal"
    }
}