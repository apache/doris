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

suite("test_multi_string_position") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_vectorized_engine=true"
    sql "SET enable_fallback_to_original_planner=false" 
    // Nereids does't support array function
    // qt_select "select multi_search_all_positions('jmdqwjbrxlbatqeixknricfk', ['qwjbrxlba', 'jmd', '', 'mdqwjbrxlbatqe', 'jbrxlbatqeixknric', 'jmdqwjbrxlbatqeixknri', '', 'fdtmnwtts', 'qwjbrxlba', '', 'qeixknricfk', 'hzjjgrnoilfkvzxaemzhf', 'lb', 'kamz', 'ixknr', 'jbrxlbatq'])"
    // Nereids does't support array function
    // qt_select "select multi_search_all_positions('coxcctuehmzkbrsmodfvx', ['bkhnp', 'nlypjvriuk', 'rkslxwfqjjivcwdexrdtvjdtvuu', 'oxcctuehm', 'xcctuehmzkbrsm', 'kfrieuocovykjmkwxbdlkgwctwvcuh', 'coxc', 'lbwvetgxyndxjqqwthtkgasbafii', 'ctuehmzkbrsmodfvx', 'obzldxjldxowk', 'ngfikgigeyll', 'wdaejjukowgvzijnw', 'zkbr', 'mzkb', 'tuehm', 'ue'])"
    // Nereids does't support array function
    // qt_select "select multi_search_all_positions('mpswgtljbbrmivkcglamemayfn', ['', 'm', 'saejhpnfgfq', 'rzanrkdssmmkanqjpfi', 'oputeneprgoowg', 'mp', '', '', 'wgtljbbrmivkcglamemay', 'cbpthtrgrmgfypizi', 'tl', 'tlj', 'xuhs', 'brmivkcglamemayfn', '', 'gtljb'])"
    // Nereids does't support array function
    // qt_select "select multi_search_all_positions('arbphzbbecypbzsqsljurtddve', ['arbphzb', 'mnrboimjfijnti', 'cikcrd', 'becypbz', 'z', 'uocmqgnczhdcrvtqrnaxdxjjlhakoszuwc', 'bbe', '', 'bp', 'yhltnexlpdijkdzt', 'jkwjmrckvgmccmmrolqvy', 'vdxmicjmfbtsbqqmqcgtnrvdgaucsgspwg', 'witlfqwvhmmyjrnrzttrikhhsrd', 'pbzsqsljurt'])"
    // Nereids does't support array function
    // qt_select "select multi_search_all_positions('aizovxqpzcbbxuhwtiaaqhdqjdei', ['qpzcbbxuhw', 'jugrpglqbm', 'dspwhzpyjohhtizegrnswhjfpdz', 'pzcbbxuh', 'vayzeszlycke', 'i', 'gvrontcpqavsjxtjwzgwxugiyhkhmhq', 'gyzmeroxztgaurmrqwtmsxcqnxaezuoapatvu', 'xqpzc', 'mjiswsvlvlpqrhhptqq', 'iz', 'hmzjxxfjsvcvdpqwtrdrp', 'zovxqpzcbbxuhwtia', 'ai'])"
    // Nereids does't support array function
    // qt_select "select multi_search_all_positions('ydfgiluhyxwqdfiwtzobwzscyxhuov', ['srsoubrgghleyheujsbwwwykerzlqphgejpxvog', 'axchkyleddjwkvbuyhmekpbbbztxdlm', 'zqodzvlkmfe', 'obwz', 'fi', 'zsc', 'xwq', 'pvmurvrd', 'uulcdtexckmrsokmgdpkstlkoavyrmxeaacvydxf', 'dfi', 'mxcngttujzgtlssrmluaflmjuv', 'hyxwqdfiwtzobwzscyxhu'])"
    // Nereids does't support array function
    // qt_select "select multi_search_all_positions('pyepgwainvmwekwhhqxxvzdjw', ['w', '', '', 'gvvkllofjnxvcu', 'kmwwhboplctvzazcyfpxhwtaddfnhekei', 'gwainv', 'pyepgwain', 'ekpnogkzzmbpfynsunwqp', 'invmwe', 'hrxpiplfplqjsstuybksuteoz', 'gwa', 'akfpyduqrwosxcbdemtxrxvundrgse', 'yepgwainvmw', 'wekwhhqxxvzdjw', 'fyimzvedmyriubgoznmcav', 'whhq', 'ozxowbwdqfisuupyzaqynoprgsjhkwlum', 'vpoufrofekajksdp'])"
    // Nereids does't support array function
    // qt_select "select multi_search_all_positions('lqwahffxurkbhhzytequotkfk', ['rwjqudpuaiufle', 'livwgbnflvy', 'hffxurkbhh', '', '', 'xcajwbqbttzfzfowjubmmgnmssat', 'zytequ', 'lq', 'h', 'rkbhh', 'a', 'immejthwgdr', '', 'llhhnlhcvnxxorzzjt', 'w', 'cvjynqxcivmmmvc', 'wexjomdcmursppjtsweybheyxzleuz', 'fzronsnddfxwlkkzidiknhpjipyrcrzel'])"
    // Nereids does't support array function
    // qt_select "select multi_search_all_positions('nkddriylnakicwgdwrfxpodqea', ['izwdpgrgpmjlwkanjrffgela', '', 'kicw', 'hltmfymgmrjckdiylkzjlvvyuleksikdjrg', 'yigveskrbidknjxigwilmkgyizewikh', 'xyvzhsnqmuec', 'odcgzlavzrwesjks', 'oilvfgliktoujukpgzvhmokdgkssqgqot', 'llsfsurvimbahwqtbqbp', 'nxj', 'pimydixeobdxmdkvhcyzcgnbhzsydx', 'couzmvxedobuohibgxwoxvmpote', 'driylnakicwgdwrf', 'nkddr'])"
    // Nereids does't support array function
    // qt_select "select multi_search_all_positions('jnckhtjqwycyihuejibqmddrdxe', ['tajzx', 'vuddoylclxatcjvinusdwt', 'spxkhxvzsljkmnzpeubszjnhqczavgtqopxn', 'ckhtjqwycyi', 'xlbfzdxspldoes', 'u', 'czosfebeznt', 'gzhabdsuyreisxvyfrfrkq', 'yihuejibqmd', 'jqwycyihuejibqm', 'cfbvprgzx', 'hxu', 'vxbhrfpzacgd', 'afoaij', 'htjqwycyihu', 'httzbskqd'])"
    // Nereids does't support array function
    // qt_select "select multi_search_all_positions('dzejajvpoojdkqbnayahygidyrjmb', ['khwxxvtnqhobbvwgwkpusjlhlzifiuclycml', 'nzvuhtwdaivo', 'dkqbnayahygidyr', 'jajvpoo', 'j', 'wdtbvwmeqgyvetu', 'kqbn', 'idyrjmb', 'tsnxuxevsxrxpgpfdgrkhwqpkse', '', 'efsdgzuefhdzkmquxu', 'zejajvpoojdkqbnayahyg', 'ugwfuighbygrxyctop', 'fcbxzbdugc', 'dxmzzrcplob', 'ejaj', 'wmmupyxrylvawsyfccluiiene', 'ohzmsqhpzbafvbzqwzftbvftei'])"
    // Nereids does't support array function
    // qt_select "select multi_search_all_positions('ffaujlverosspbzaqefjzql', ['lvero', 'erossp', 'f', 'ujlverosspbz', 'btfimgklzzxlbkbuqyrmnud', 'osspb', 'muqexvtjuaar', 'f', 'bzaq', 'lprihswhwkdhqciqhfaowarn', 'ffaujlve', 'uhbbjrqjb', 'jlver', 'umucyhbbu', 'pjthtzmgxhvpbdphesnnztuu', 'xfqhfdfsbbazactpastzvzqudgk', 'lvovjfoatc', 'z', 'givejzhoqsd', ''])"
}
