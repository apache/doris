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

import org.slf4j.Logger
import org.slf4j.LoggerFactory

suite("nereids_fn") {
    sql "set enable_nereids_planner=true"
    sql "set enable_fallback_to_original_planner=false"

    final Logger logger = LoggerFactory.getLogger(this.class)

    // ddl begin
    sql "drop table if exists t"

    sql """
        CREATE TABLE IF NOT EXISTS `t` (
            `kbool` boolean null,
            `ktint` tinyint(4) null,
            `ksint` smallint(6) null,
            `kint` int(11) null,
            `kbint` bigint(20) null,
            `klint` largeint(40) null,
            `kfloat` float null,
            `kdbl` double null,
            `kdcmls1` decimal(9, 3) null,
            `kdcmls2` decimal(15, 5) null,
            `kdcmls3` decimal(27, 9) null,
            `kdcmlv3s1` decimalv3(9, 3) null,
            `kdcmlv3s2` decimalv3(15, 5) null,
            `kdcmlv3s3` decimalv3(27, 9) null,
            `kchrs1` char(5) null,
            `kchrs2` char(20) null,
            `kchrs3` char(50) null,
            `kvchrs1` varchar(10) null,
            `kvchrs2` varchar(20) null,
            `kvchrs3` varchar(50) null,
            `kstr` string null,
            `kdt` date null,
            `kdtv2` datev2 null,
            `kdtm` datetime null,
            `kdtmv2s1` datetimev2(0) null,
            `kdtmv2s2` datetimev2(4) null,
            `kdtmv2s3` datetimev2(6) null,
            `kabool` array<boolean> null,
            `katint` array<tinyint> null,
            `kasint` array<smallint> null,
            `kaint` array<int> null,
            `kabint` array<bigint> null,
            `kalint` array<largeint> null,
            `kafloat` array<float> null,
            `kadbl` array<double> null,
            `kadt` array<date> null,
            `kadtm` array<datetime> null,
            `kadtv2` array<datev2> null,
            `kadtmv2` array<datetimev2> null,
            `kachr` array<char> null,
            `kavchr` array<varchar> null,
            `kastr` array<string> null,
            `kadcml` array<decimal> null
        ) engine=olap
        DISTRIBUTED BY HASH(`ktint`) BUCKETS 5
        properties("replication_num" = "1")
    """

    sql """
    insert into t values
    (0,1,1980,423524523,9223372036854775807,63546353464356436345346345,211.4,4235.432523,25344.543,4324.32523,23352.23524,25344.543,4324.32523,23352.23524,dsgs,Sdgsdfsdg,Nfwkfhwukbvkwbfsejfbwebf,dsgs,Sdgsdfsdg,Nfwkfhwukbvkwbfsejfbwebf,Nfwkfhwukbvkwbfsejfbwebf,1903-12-24,1903-12-24,2051-12-23 13:04:45,2051-12-23 13:04:45,2051-12-20 13:04:45.3436,2051-12-20 13:04:45.34536,2051-12-20 13:04:45.34536,[0],[1],[1980],[423524523],[9223372036854775807],[63546353464356436345346345],[211.4],[1903-12-24,1903-12-24],[2051-12-20 13:04:45],[1903-12-24,1903-12-24],[2051-12-20 13:04:45,2051-12-20 13:04:45,2051-12-20 13:04:45.3436,2051-12-20 13:04:45.34536],[dsgs,Sdgsdfsdg,Nfwkfhwukbvkwbfsejfbwebf],[dsgs,Sdgsdfsdg,Nfwkfhwukbvkwbfsejfbwebf,dsgs,Sdgsdfsdg,Nfwkfhwukbvkwbfsejfbwebf],[Sdgsdfsdg,Nfwkfhwukbvkwbfsejfbwebf,dsgs,Sdgsdfsdg,Nfwkfhwukbvkwbfsejfbwebf,Nfwkfhwukbvkwbfsejfbwebf],[4324.32523,23352.23523523]),
(0,2,1986,2207483647,-9223372036854775807,-54673967938678953673967348967,0.2,534.52352,64564.235,325262.435,235423626.4,64564.235,325262.435,235423626.4,Jehdjd,Gsdgwgsg,Fbwekfbwcweubfwkebcwefewfwefbw,Jehdjd,Gsdgwgsg,Fbwekfbwcweubfwkebcwefewfwefbw,Fbwekfbwcweubfwkebcwefewfwefbw,9999-12-21,9999-12-21,2011-12-23 13:04:45,2011-12-23 13:04:00,2011-12-20 13:04:45.7667,2011-12-20 13:04:45.76567,2011-12-20 13:04:45.76567,[0],[2],[1986],[2207483647],[-9223372036854775807],[-54673967938678953673967348967],[0.2],[9999-12-21,9999-12-21],[2011-12-20 13:04:45],[9999-12-21,9999-12-21],[2011-12-20 13:04:45,2011-12-20 13:04:00,2011-12-20 13:04:45.7667,2011-12-20 13:04:45.76567],[Jehdjd,Gsdgwgsg,Fbwekfbwcweubfwkebcwefewfwefbw],[Jehdjd,Gsdgwgsg,Fbwekfbwcweubfwkebcwefewfwefbw,Jehdjd,Gsdgwgsg,Fbwekfbwcweubfwkebcwefewfwefbw],[Gsdgwgsg,Fbwekfbwcweubfwkebcwefewfwefbw,Jehdjd,Gsdgwgsg,Fbwekfbwcweubfwkebcwefewfwefbw,Fbwekfbwcweubfwkebcwefewfwefbw],[325262.435,235423626.434353]),
(0,3,1992,-2207483647,4359080345834638390,86904358903467903579034679304579304,241.154,-35245.52352,52623.25,3543.35,3463634.544,52623.25,3543.35,3463634.544,Fhdr,Gsgwy,Wefbksncweukbfkwbffwegwfcwef,Fhdr,Gsgwy,Wefbksncweukbfkwbffwegwfcwef,Wefbksncweukbfkwbffwegwfcwef,0001-12-21,0001-12-21,1371-12-22 13:04:45,2051-12-23 13:04:45,2051-12-20 13:04:45.3436,2051-12-20 13:04:45.34536,2051-12-20 13:04:45.34536,[0],[3],[1992],[-2207483647],[4359080345834638390],[86904358903467903579034679304579304],[241.154],[0001-12-23,0001-12-23],[1371-12-20 13:04:45],[0001-12-23,0001-12-23],[1371-12-20 13:04:45,2051-12-20 13:04:45,2051-12-20 13:04:45.3436,2051-12-20 13:04:45.34536],[Fhdr,Gsgwy,Wefbksncweukbfkwbffwegwfcwef],[Fhdr,Gsgwy,Wefbksncweukbfkwbffwegwfcwef,Fhdr,Gsgwy,Wefbksncweukbfkwbffwegwfcwef],[Gsgwy,Wefbksncweukbfkwbffwegwfcwef,Fhdr,Gsgwy,Wefbksncweukbfkwbffwegwfcwef,Wefbksncweukbfkwbffwegwfcwef],[3543.35,3463634.543636]),
(0,4,1998,54534242,3476903769356803,-890235723904023750237409237590232,-1241.45,-5325.325235,0.35,-4324.32523,-45463.53464,0.35,-4324.32523,-45463.53464,Dfhdf,Fsgsdrwgh,Fwefbnckbewvbfwkubcwebfwec,Dfhdf,Fsgsdrwgh,Fwefbnckbewvbfwkubcwebfwec,Fwefbnckbewvbfwkubcwebfwec,2420-01-12,2420-01-12,1331-12-22 13:04:45,2011-12-23 13:04:45,2011-12-20 13:04:45.7657,2011-12-20 13:04:45.76567,2011-12-20 13:04:45.76567,[0],[4],[1998],[54534242],[3476903769356803],[-890235723904023750237409237590232],[-1241.45],[2420-01-12,2420-01-12],[1331-12-20 13:04:45],[2420-01-12,2420-01-12],[1331-12-20 13:04:45,2011-12-20 13:04:45,2011-12-20 13:04:45.7657,2011-12-20 13:04:45.76567],[Dfhdf,Fsgsdrwgh,Fwefbnckbewvbfwkubcwebfwec],[Dfhdf,Fsgsdrwgh,Fwefbnckbewvbfwkubcwebfwec,Dfhdf,Fsgsdrwgh,Fwefbnckbewvbfwkubcwebfwec],[Fsgsdrwgh,Fwefbnckbewvbfwkubcwebfwec,Dfhdf,Fsgsdrwgh,Fwefbnckbewvbfwkubcwebfwec,Fwefbnckbewvbfwkubcwebfwec],[-4324.32523,-45463.53463773]),
(0,5,32767,234252,-4643347364763456,234325893724892375823742523,124.41,23523.45232,4364.6,-325262.435,23352.23524,4364.6,-325262.435,23352.23524,Heed,Fwnjwbfw,Wehnckwewebvwefwcwe,Heed,Fwnjwbfw,Wehnckwewebvwefwcwe,Wehnckwewebvwefwcwe,1903-12-24,1903-12-24,1291-12-21 13:04:45,2051-12-23 13:04:45,2051-12-20 13:04:45.3436,2051-12-20 13:04:45.34536,2051-12-20 13:04:45.34536,[0],[5],[32767],[234252],[-4643347364763456],[234325893724892375823742523],[124.41],[1903-12-24,1903-12-24],[1291-12-20 13:04:45],[1903-12-24,1903-12-24],[1291-12-20 13:04:45,2051-12-20 13:04:45,2051-12-20 13:04:45.3436,2051-12-20 13:04:45.34536],[Heed,Fwnjwbfw,Wehnckwewebvwefwcwe],[Heed,Fwnjwbfw,Wehnckwewebvwefwcwe,Heed,Fwnjwbfw,Wehnckwewebvwefwcwe],[Fwnjwbfw,Wehnckwewebvwefwcwe,Heed,Fwnjwbfw,Wehnckwewebvwefwcwe,Wehnckwewebvwefwcwe],[-325262.435,23352.23523523]),
(0,6,-32767,-2352342,34634796439863689,532623456245244235,-41251.1,523.43252,-0.436,-3543.35,235423626.4,-0.436,-3543.35,235423626.4,Hdefd,Ewgweufhwfwf,fwevbwcbkweb,Hdefd,Ewgweufhwfwf,fwevbwcbkweb,fwevbwcbkweb,9599-12-21,9599-12-21,1251-12-21 13:04:45,2011-12-23 13:04:00,2011-12-20 13:04:45.7667,2011-12-20 13:04:45.76567,2011-12-20 13:04:45.76567,[0],[6],[-32767],[-2352342],[34634796439863689],[532623456245244235],[-41251.1],[9599-12-21,9599-12-21],[1251-12-20 13:04:45],[9599-12-21,9599-12-21],[1251-12-20 13:04:45,2011-12-20 13:04:00,2011-12-20 13:04:45.7667,2011-12-20 13:04:45.76567],[Hdefd,Ewgweufhwfwf,fwevbwcbkweb],[Hdefd,Ewgweufhwfwf,fwevbwcbkweb,Hdefd,Ewgweufhwfwf,fwevbwcbkweb],[Ewgweufhwfwf,fwevbwcbkweb,Hdefd,Ewgweufhwfwf,fwevbwcbkweb,fwevbwcbkweb],[-3543.35,235423626.434353]),
(0,7,255,325265656,-79834967836,3652354346,0.1331,63.64354,346346.346,432534.3252,-346367634.5,346346.346,432534.3252,-346367634.5,gdcg,Fwegwdwegw,Fwefwecwegwehgdrherehjgfd,gdcg,Fwegwdwegw,Fwefwecwegwehgdrherehjgfd,Fwefwecwegwehgdrherehjgfd,1201-12-30,1201-12-30,1211-12-21 13:04:45,2051-12-23 13:04:45,2051-12-20 13:04:45.3436,2051-12-20 13:04:45.34536,2051-12-20 13:04:45.34536,[0],[7],[255],[325265656],[-79834967836],[3652354346],[0.1331],[1201-12-23,1201-12-23],[1211-12-20 13:04:45],[1201-12-23,1201-12-23],[1211-12-20 13:04:45,2051-12-20 13:04:45,2051-12-20 13:04:45.3436,2051-12-20 13:04:45.34536],[gdcg,Fwegwdwegw,Fwefwecwegwehgdrherehjgfd],[gdcg,Fwegwdwegw,Fwefwecwegwehgdrherehjgfd,gdcg,Fwegwdwegw,Fwefwecwegwehgdrherehjgfd],[Fwegwdwegw,Fwefwecwegwehgdrherehjgfd,gdcg,Fwegwdwegw,Fwefwecwegwehgdrherehjgfd,Fwefwecwegwehgdrherehjgfd],[432534.32523,-346367634.543636]),
(1,8,650,-2374982,36347,-46354743565437547345373,0.00,7346.5436,-634536.534,3256262.435,-454863.5346,-634536.534,3256262.435,-454863.5346,Edged,Fwegwefwed,Hergfdjdgdhdwswh,Edged,Fwegwefwed,Hergfdjdgdhdwswh,Hergfdjdgdhdwswh,2420-01-12,2420-01-12,1171-12-21 13:04:45,2011-12-23 13:04:45,2011-12-20 13:04:45.7657,2011-12-20 13:04:45.76567,2011-12-20 13:04:45.76567,[1],[8],[650],[-2374982],[36347],[-46354743565437547345373],[0.00],[2420-01-12,2420-01-12],[1171-12-20 13:04:45],[2420-01-12,2420-01-12],[1171-12-20 13:04:45,2011-12-20 13:04:45,2011-12-20 13:04:45.7657,2011-12-20 13:04:45.76567],[Edged,Fwegwefwed,Hergfdjdgdhdwswh],[Edged,Fwegwefwed,Hergfdjdgdhdwswh,Edged,Fwegwefwed,Hergfdjdgdhdwswh],[Fwegwefwed,Hergfdjdgdhdwswh,Edged,Fwegwefwed,Hergfdjdgdhdwswh,Hergfdjdgdhdwswh],[3256262.435,-454863.53463773]),
(1,9,32767,248962598,-78345736,4.58753E+12,41.412,77645.643,637.34,35943.35,273352.2352,637.34,35943.35,273352.2352,ewjdf,Hdfger,Xgdxsgxgrshhs,ewjdf,Hdfger,Xgdxsgxgrshhs,Xgdxsgxgrshhs,1903-12-24,1903-12-24,1131-12-21 13:04:45,2051-12-23 13:04:45,2051-12-20 13:04:45.3436,2051-12-20 13:04:45.34536,2051-12-20 13:04:45.34536,[1],[9],[32767],[248962598],[-78345736],[4587534654345],[41.412],[1903-12-24,1903-12-24],[1131-12-20 13:04:45],[1903-12-24,1903-12-24],[1131-12-20 13:04:45,2051-12-20 13:04:45,2051-12-20 13:04:45.3436,2051-12-20 13:04:45.34536],[ewjdf,Hdfger,Xgdxsgxgrshhs],[ewjdf,Hdfger,Xgdxsgxgrshhs,ewjdf,Hdfger,Xgdxsgxgrshhs],[Hdfger,Xgdxsgxgrshhs,ewjdf,Hdfger,Xgdxsgxgrshhs,Xgdxsgxgrshhs],[35943.35,273352.23523523]),
(1,10,-32767,-257822502,35753753,3.45785E+11,0.312,87.4634,3425.23,43274.92523,235423626.4,3425.23,43274.92523,235423626.4,Budg,Wheqwerh,Hserhsegrsehseyeg,Budg,Wheqwerh,Hserhsegrsehseyeg,Hserhsegrsehseyeg,9999-12-21,9999-12-21,1091-12-20 13:04:45,2011-12-23 13:04:00,2011-12-20 13:04:45.7667,2011-12-20 13:04:45.76567,2011-12-20 13:04:45.76567,[1],[10],[-32767],[-257822502],[35753753],[345784567457],[0.312],[9999-12-21,9999-12-21],[1091-12-20 13:04:45],[9999-12-21,9999-12-21],[1091-12-20 13:04:45,2011-12-20 13:04:00,2011-12-20 13:04:45.7667,2011-12-20 13:04:45.76567],[Budg,Wheqwerh,Hserhsegrsehseyeg],[Budg,Wheqwerh,Hserhsegrsehseyeg,Budg,Wheqwerh,Hserhsegrsehseyeg],[Wheqwerh,Hserhsegrsehseyeg,Budg,Wheqwerh,Hserhsegrsehseyeg,Hserhsegrsehseyeg],[43274.92523,235423626.434353]),
(1,11,255,3253262,89754686484,65437346547457457345,3.420,7568.457,-32632.32,-325262.435,-3463634.544,-32632.32,-325262.435,-3463634.544,Geed,Werghfweghwwf,Gerszgsegserhdrstjysergesgsegesrgserhserhgsegser,Geed,Werghfweghwwf,Gerszgsegserhdrstjysergesgsegesrgserhserhgsegser,Gerszgsegserhdrstjysergesgsegesrgserhserhgsegser,0101-12-22,0101-12-22,1051-12-20 13:04:45,2051-12-23 13:04:45,2051-12-20 13:04:45.3436,2051-12-20 13:04:45.34536,2051-12-20 13:04:45.34536,[1],[11],[255],[3253262],[89754686484],[65437346547457457345],[3.420],[0101-12-23,0101-12-23],[1051-12-20 13:04:45],[0101-12-23,0101-12-23],[1051-12-20 13:04:45,2051-12-20 13:04:45,2051-12-20 13:04:45.3436,2051-12-20 13:04:45.34536],[Geed,Werghfweghwwf,Gerszgsegserhdrstjysergesgsegesrgserhserhgsegser],[Geed,Werghfweghwwf,Gerszgsegserhdrstjysergesgsegesrgserhserhgsegser,Geed,Werghfweghwwf,Gerszgsegserhdrstjysergesgsegesrgserhserhgsegser],[Werghfweghwwf,Gerszgsegserhdrstjysergesgsegesrgserhserhgsegser,Geed,Werghfweghwwf,Gerszgsegserhdrstjysergesgsegesrgserhserhgsegser,Gerszgsegserhdrstjysergesgsegesrgserhserhgsegser],[-325262.435,-3463634.543636]),
(1,12,650,623244772,5.34589E+11,574745375474745846457456748467457,354.322,7345.3576,232.52,983543.35,45463.53464,232.52,983543.35,45463.53464,Jhdj4,Fewghwfweg,Gesrhesgesyhsetsergsewrhrsegshr,Jhdj4,Fewghwfweg,Gesrhesgesyhsetsergsewrhrsegshr,Gesrhesgesyhsetsergsewrhrsegshr,2420-01-12,2420-01-12,1011-12-20 13:04:45,2011-12-23 13:04:45,2011-12-20 13:04:45.7657,2011-12-20 13:04:45.76567,2011-12-20 13:04:45.76567,[1],[12],[650],[623244772],[534589467548],[574745375474745846457456748467457],[354.322],[2420-01-12,2420-01-12],[1011-12-20 13:04:45],[2420-01-12,2420-01-12],[1011-12-20 13:04:45,2011-12-20 13:04:45,2011-12-20 13:04:45.7657,2011-12-20 13:04:45.76567],[Jhdj4,Fewghwfweg,Gesrhesgesyhsetsergsewrhrsegshr],[Jhdj4,Fewghwfweg,Gesrhesgesyhsetsergsewrhrsegshr,Jhdj4,Fewghwfweg,Gesrhesgesyhsetsergsewrhrsegshr],[Fewghwfweg,Gesrhesgesyhsetsergsewrhrsegshr,Jhdj4,Fewghwfweg,Gesrhesgesyhsetsergsewrhrsegshr,Gesrhesgesyhsetsergsewrhrsegshr],[983543.35,45463.53463773]),
(1,13,32767,23562,5.84439E+11,-56337,453.523,5436.525636,-325.2,64324.32523,2335752.235,-325.2,64324.32523,2335752.235,Gdgdh,Fwewewgh,Gershjsereysjusehresjseefgserjetesg,Gdgdh,Fwewewgh,Gershjsereysjusehresjseefgserjetesg,Gershjsereysjusehresjseefgserjetesg,1903-12-24,1903-12-24,0971-12-19 13:04:45,2051-12-23 13:04:45,2051-12-20 13:04:45.3436,2051-12-20 13:04:45.34536,2051-12-20 13:04:45.34536,[1],[13],[32767],[23562],[584438696476],[-56337],[453.523],[1903-12-24,1903-12-24],[0971-12-20 13:04:45],[1903-12-24,1903-12-24],[0971-12-20 13:04:45,2051-12-20 13:04:45,2051-12-20 13:04:45.3436,2051-12-20 13:04:45.34536],[Gdgdh,Fwewewgh,Gershjsereysjusehresjseefgserjetesg],[Gdgdh,Fwewewgh,Gershjsereysjusehresjseefgserjetesg,Gdgdh,Fwewewgh,Gershjsereysjusehresjseefgserjetesg],[Fwewewgh,Gershjsereysjusehresjseefgserjetesg,Gdgdh,Fwewewgh,Gershjsereysjusehresjseefgserjetesg,Gershjsereysjusehresjseefgserjetesg],[64324.32523,2335752.23523523]),
(1,20,34245,0,-7458,3457534634575534645,536.4535,534634.4378,-0.02,33625262.44,-2354823626,-0.02,33625262.44,-2354823626,Grr,Ssegwgsdvh,Grsejesgserhjsfgsegsehuer,Grr,Ssegwgsdvh,Grsejesgserhjsfgsegsehuer,Grsejesgserhjsfgsegsehuer,9999-12-21,9999-12-21,0931-12-19 13:04:45,2011-12-23 13:04:00,2011-12-20 13:04:45.7667,2011-12-20 13:04:45.76567,2011-12-20 13:04:45.76567,[],[],[],[],[],[],[],[],[],[],[],[],[],[],[]),
(null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null)"""
    // ddl end
    
    // function table begin
    // usage: write the new function in the list and construct answer.
    def scalar_function = [
        'abs' : [['double', 'double'], ['float', 'float'], ['largeint', 'largeint'], ['largeint', 'bigint'], ['integer', 'smallint'], ['bigint', 'integer'], ['smallint', 'tinyint'], ['decimalv2', 'decimalv2']],
        'acos' : [['double', 'double']],
        'aes_decrypt' : [['varchar', 'varchar', 'varchar'], ['string', 'string', 'string'], ['varchar', 'varchar', 'varchar', 'varchar'], ['string', 'string', 'string', 'string'], ['varchar', 'varchar', 'varchar', 'varchar', 'varchar'], ['string', 'string', 'string', 'string', 'string']],
        'aes_encrypt' : [['varchar', 'varchar', 'varchar'], ['string', 'string', 'string'], ['varchar', 'varchar', 'varchar', 'varchar'], ['string', 'string', 'string', 'string'], ['varchar', 'varchar', 'varchar', 'varchar', 'varchar'], ['string', 'string', 'string', 'string', 'string']],
        'append_trailing_char_if_absent' : [['varchar', 'varchar', 'varchar'], ['string', 'string', 'string']],
        'ascii' : [['integer', 'varchar'], ['integer', 'string']],
        'asin' : [['double', 'double']],
        'atan' : [['double', 'double']],
        'bin' : [['varchar', 'bigint']],
        'bit_length' : [['integer', 'varchar'], ['integer', 'string']],
        'bitmap_and' : [['bitmap', 'bitmap', 'bitmap']],
        'bitmap_and_count' : [['bigint', 'bitmap', 'bitmap']],
        'bitmap_and_not' : [['bitmap', 'bitmap', 'bitmap']],
        'bitmap_and_not_count' : [['bigint', 'bitmap', 'bitmap']],
        'bitmap_contains' : [['boolean', 'bitmap', 'bigint']],
        'bitmap_count' : [['bigint', 'bitmap']],
        'bitmap_empty' : [['bitmap', '']],
        'bitmap_from_string' : [['bitmap', 'varchar'], ['bitmap', 'string']],
        'bitmap_has_all' : [['boolean', 'bitmap', 'bitmap']],
        'bitmap_has_any' : [['boolean', 'bitmap', 'bitmap']],
        'bitmap_hash' : [['bitmap', 'varchar'], ['bitmap', 'string']],
        'bitmap_hash64' : [['bitmap', 'varchar'], ['bitmap', 'string']],
        'bitmap_max' : [['bigint', 'bitmap']],
        'bitmap_min' : [['bigint', 'bitmap']],
        'bitmap_not' : [['bitmap', 'bitmap', 'bitmap']],
        'bitmap_or' : [['bitmap', 'bitmap', 'bitmap']],
        'bitmap_or_count' : [['bigint', 'bitmap', 'bitmap']],
        'bitmap_subset_in_range' : [['bitmap', 'bitmap', 'bigint', 'bigint']],
        'bitmap_subset_limit' : [['bitmap', 'bitmap', 'bigint', 'bigint']],
        'bitmap_to_string' : [['string', 'bitmap']],
        'bitmap_xor' : [['bitmap', 'bitmap', 'bitmap']],
        'bitmap_xor_count' : [['bigint', 'bitmap', 'bitmap']],
        'cbrt' : [['double', 'double']],
        'ceil' : [['double', 'double']],
        'ceiling' : [['bigint', 'double']],
        'character_length' : [['integer', 'varchar'], ['integer', 'string']],
        'coalesce' : [['boolean', 'boolean'], ['tinyint', 'tinyint'], ['smallint', 'smallint'], ['integer', 'integer'], ['bigint', 'bigint'], ['largeint', 'largeint'], ['float', 'float'], ['double', 'double'], ['datetime', 'datetime'], ['date', 'date'], ['datetimev2', 'datetimev2'], ['datev2', 'datev2'], ['decimalv2', 'decimalv2'], ['bitmap', 'bitmap'], ['varchar', 'varchar'], ['string', 'string']],
        'concat' : [['varchar', 'varchar'], ['string', 'string']],
        'connection_id' : [['varchar', '']],
        'conv' : [['varchar', 'bigint', 'tinyint', 'tinyint'], ['varchar', 'varchar', 'tinyint', 'tinyint'], ['varchar', 'string', 'tinyint', 'tinyint']],
        'convert_to' : [['varchar', 'varchar', 'varchar']],
        'convert_tz' : [['datetime', 'datetime', 'varchar', 'varchar'], ['datetimev2', 'datetimev2', 'varchar', 'varchar'], ['datev2', 'datev2', 'varchar', 'varchar']],
        'cos' : [['double', 'double']],
        'current_date' : [['date', '']],
        'current_time' : [['time', '']],
        'current_timestamp' : [['datetime', ''], ['datetimev2', 'integer']],
        'current_user' : [['bigint', '']],
        'curtime' : [['time', '']],
        'database' : [['varchar', '']],
        'date' : [['date', 'datetime'], ['datev2', 'datetimev2']],
        'date_diff' : [['integer', 'datetime', 'datetime'], ['integer', 'datetimev2', 'datetimev2'], ['integer', 'datetimev2', 'datev2'], ['integer', 'datev2', 'datetimev2'], ['integer', 'datev2', 'datev2'], ['integer', 'datetimev2', 'datetime'], ['integer', 'datev2', 'datetime']],
        'date_format' : [['varchar', 'datetime', 'varchar'], ['varchar', 'date', 'varchar'], ['varchar', 'datetimev2', 'varchar'], ['varchar', 'datev2', 'varchar']],
        'date_trunc' : [['datetime', 'datetime', 'varchar'], ['datetimev2', 'datetimev2', 'varchar']],
        'date_v2' : [['datev2', 'datetimev2']],
        'day' : [['integer', 'datetime'], ['integer', 'datetimev2'], ['integer', 'datev2']],
        'day_ceil' : [['datetime', 'datetime'], ['datetimev2', 'datetimev2'], ['datev2', 'datev2'], ['datetime', 'datetime', 'datetime'], ['datetime', 'datetime', 'integer'], ['datetimev2', 'datetimev2', 'datetimev2'], ['datetimev2', 'datetimev2', 'integer'], ['datev2', 'datev2', 'datev2'], ['datev2', 'datev2', 'integer'], ['datetime', 'datetime', 'integer', 'datetime'], ['datetimev2', 'datetimev2', 'integer', 'datetimev2'], ['datev2', 'datev2', 'integer', 'datev2']],
        'day_floor' : [['datetime', 'datetime'], ['datetimev2', 'datetimev2'], ['datev2', 'datev2'], ['datetime', 'datetime', 'datetime'], ['datetime', 'datetime', 'integer'], ['datetimev2', 'datetimev2', 'datetimev2'], ['datetimev2', 'datetimev2', 'integer'], ['datev2', 'datev2', 'datev2'], ['datev2', 'datev2', 'integer'], ['datetime', 'datetime', 'integer', 'datetime'], ['datetimev2', 'datetimev2', 'integer', 'datetimev2'], ['datev2', 'datev2', 'integer', 'datev2']],
        'day_name' : [['varchar', 'datetime'], ['varchar', 'datetimev2'], ['varchar', 'datev2']],
        'day_of_month' : [['integer', 'datetime'], ['integer', 'datetimev2'], ['integer', 'datev2']],
        'day_of_week' : [['integer', 'datetime'], ['integer', 'datetimev2'], ['integer', 'datev2']],
        'day_of_year' : [['integer', 'datetime'], ['integer', 'datetimev2'], ['integer', 'datev2']],
        'days_add' : [['datetime', 'datetime', 'integer'], ['datetimev2', 'datetimev2', 'integer'], ['date', 'date', 'integer'], ['datev2', 'datev2', 'integer']],
        'days_diff' : [['bigint', 'datetime', 'datetime'], ['bigint', 'datetimev2', 'datetimev2'], ['bigint', 'datev2', 'datetimev2'], ['bigint', 'datetimev2', 'datev2'], ['bigint', 'datev2', 'datev2'], ['bigint', 'datev2', 'datetime'], ['bigint', 'datetime', 'datev2'], ['bigint', 'datetimev2', 'datetime'], ['bigint', 'datetime', 'datetimev2']],
        'days_sub' : [['datetime', 'datetime', 'integer'], ['datetimev2', 'datetimev2', 'integer'], ['date', 'date', 'integer'], ['datev2', 'datev2', 'integer']],
        'dceil' : [['bigint', 'double']],
        'degrees' : [['double', 'double']],
        'dexp' : [['double', 'double']],
        'dfloor' : [['bigint', 'double']],
        'digital_masking' : [['varchar', 'bigint']],
        'dlog1' : [['double', 'double']],
        'dlog10' : [['double', 'double']],
        'domain' : [['string', 'string']],
        'domain_without_www' : [['string', 'string']],
        'dpow' : [['double', 'double', 'double']],
        'dround' : [['bigint', 'double'], ['double', 'double', 'integer']],
        'dsqrt' : [['double', 'double']],
        'e' : [['double', '']],
        'elt' : [['varchar', 'integer', 'varchar'], ['string', 'integer', 'string']],
        'ends_with' : [['boolean', 'varchar', 'varchar'], ['boolean', 'string', 'string']],
        'es_query' : [['boolean', 'varchar', 'varchar']],
        'exp' : [['double', 'double']],
        'extract_url_parameter' : [['varchar', 'varchar', 'varchar']],
        'field' : [['integer', 'tinyint'], ['integer', 'smallint'], ['integer', 'integer'], ['integer', 'bigint'], ['integer', 'largeint'], ['integer', 'float'], ['integer', 'double'], ['integer', 'decimalv2'], ['integer', 'datev2'], ['integer', 'datetimev2'], ['integer', 'varchar'], ['integer', 'string']],
        'find_in_set' : [['integer', 'varchar', 'varchar'], ['integer', 'string', 'string']],
        'floor' : [['double', 'double']],
        'fmod' : [['float', 'float', 'float'], ['double', 'double', 'double']],
        'fpow' : [['double', 'double', 'double']],
        'from_base64' : [['varchar', 'varchar'], ['string', 'string']],
        'from_days' : [['date', 'integer']],
        'from_unixtime' : [['varchar', 'integer'], ['varchar', 'integer', 'varchar'], ['varchar', 'integer', 'string']],
        'get_json_double' : [['double', 'varchar', 'varchar'], ['double', 'string', 'string']],
        'get_json_int' : [['integer', 'varchar', 'varchar'], ['integer', 'string', 'string']],
        'get_json_string' : [['varchar', 'varchar', 'varchar'], ['string', 'string', 'string']],
        'greatest' : [['tinyint', 'tinyint'], ['smallint', 'smallint'], ['integer', 'integer'], ['bigint', 'bigint'], ['largeint', 'largeint'], ['float', 'float'], ['double', 'double'], ['decimalv2', 'decimalv2'], ['datetime', 'datetime'], ['datetimev2', 'datetimev2'], ['varchar', 'varchar'], ['string', 'string']],
        'hex' : [['varchar', 'bigint'], ['varchar', 'varchar'], ['string', 'string']],
        'hll_cardinality' : [['bigint', 'hll']],
        'hll_empty' : [['hll', '']],
        'hll_hash' : [['hll', 'varchar'], ['hll', 'string']],
        'hour' : [['integer', 'datetime'], ['integer', 'datetimev2'], ['integer', 'datev2']],
        'hour_ceil' : [['datetime', 'datetime'], ['datetimev2', 'datetimev2'], ['datetimev2', 'datev2'], ['datetime', 'datetime', 'datetime'], ['datetime', 'datetime', 'integer'], ['datetimev2', 'datetimev2', 'datetimev2'], ['datetimev2', 'datetimev2', 'integer'], ['datetimev2', 'datev2', 'datev2'], ['datetimev2', 'datev2', 'integer'], ['datetime', 'datetime', 'integer', 'datetime'], ['datetimev2', 'datetimev2', 'integer', 'datetimev2'], ['datetimev2', 'datev2', 'integer', 'datev2']],
        'hour_floor' : [['datetime', 'datetime'], ['datetimev2', 'datetimev2'], ['datetimev2', 'datev2'], ['datetime', 'datetime', 'datetime'], ['datetime', 'datetime', 'integer'], ['datetimev2', 'datetimev2', 'datetimev2'], ['datetimev2', 'datetimev2', 'integer'], ['datetimev2', 'datev2', 'datev2'], ['datetimev2', 'datev2', 'integer'], ['datetime', 'datetime', 'integer', 'datetime'], ['datetimev2', 'datetimev2', 'integer', 'datetimev2'], ['datetimev2', 'datev2', 'integer', 'datev2']],
        'hours_add' : [['datetime', 'datetime', 'integer'], ['datetimev2', 'datetimev2', 'integer'], ['datetime', 'date', 'integer'], ['datetimev2', 'datev2', 'integer']],
        'hours_diff' : [['bigint', 'datetime', 'datetime'], ['bigint', 'datetimev2', 'datetimev2'], ['bigint', 'datev2', 'datetimev2'], ['bigint', 'datetimev2', 'datev2'], ['bigint', 'datev2', 'datev2'], ['bigint', 'datev2', 'datetime'], ['bigint', 'datetime', 'datev2'], ['bigint', 'datetimev2', 'datetime'], ['bigint', 'datetime', 'datetimev2']],
        'hours_sub' : [['datetime', 'datetime', 'integer'], ['datetimev2', 'datetimev2', 'integer'], ['datetime', 'date', 'integer'], ['datetimev2', 'datev2', 'integer']],
        'initcap' : [['varchar', 'varchar']],
        'instr' : [['integer', 'varchar', 'varchar'], ['integer', 'string', 'string']],
        'json_array' : [['varchar', 'varchar']],
        'json_object' : [['varchar', 'varchar']],
        'json_quote' : [['varchar', 'varchar']],
        'jsonb_exists_path' : [['boolean', 'json', 'varchar'], ['boolean', 'json', 'string']],
        'jsonb_extract' : [['json', 'json', 'varchar'], ['json', 'json', 'string']],
        'jsonb_extract_bigint' : [['bigint', 'json', 'varchar'], ['bigint', 'json', 'string']],
        'jsonb_extract_bool' : [['boolean', 'json', 'varchar'], ['boolean', 'json', 'string']],
        'jsonb_extract_double' : [['double', 'json', 'varchar'], ['double', 'json', 'string']],
        'jsonb_extract_int' : [['integer', 'json', 'varchar'], ['integer', 'json', 'string']],
        'jsonb_extract_isnull' : [['boolean', 'json', 'varchar'], ['boolean', 'json', 'string']],
        'jsonb_extract_string' : [['string', 'json', 'varchar'], ['string', 'json', 'string']],
        'jsonb_parse' : [['json', 'varchar']],
        'jsonb_parse_error_to_invalid' : [['json', 'varchar']],
        'jsonb_parse_error_to_null' : [['json', 'varchar']],
        'jsonb_parse_error_to_value' : [['json', 'varchar', 'varchar']],
        'jsonb_parse_notnull' : [['json', 'varchar']],
        'jsonb_parse_notnull_error_to_invalid' : [['json', 'varchar']],
        'jsonb_parse_notnull_error_to_value' : [['json', 'varchar', 'varchar']],
        'jsonb_parse_nullable' : [['json', 'varchar']],
        'jsonb_parse_nullable_error_to_invalid' : [['json', 'varchar']],
        'jsonb_parse_nullable_error_to_null' : [['json', 'varchar']],
        'jsonb_parse_nullable_error_to_value' : [['json', 'varchar', 'varchar']],
        'jsonb_type' : [['string', 'json', 'string']],
        'last_day' : [['date', 'datetime'], ['date', 'date'], ['datev2', 'datetimev2'], ['datev2', 'datev2']],
        'least' : [['tinyint', 'tinyint'], ['smallint', 'smallint'], ['integer', 'integer'], ['bigint', 'bigint'], ['largeint', 'largeint'], ['float', 'float'], ['double', 'double'], ['datetime', 'datetime'], ['decimalv2', 'decimalv2'], ['varchar', 'varchar'], ['string', 'string']],
        'left' : [['varchar', 'varchar', 'integer'], ['string', 'string', 'integer']],
        'length' : [['integer', 'varchar'], ['integer', 'string']],
        'ln' : [['double', 'double']],
        'local_time' : [['datetime', ''], ['datetimev2', 'integer']],
        'local_timestamp' : [['datetime', ''], ['datetimev2', 'integer']],
        'locate' : [['integer', 'varchar', 'varchar'], ['integer', 'string', 'string'], ['integer', 'varchar', 'varchar', 'integer'], ['integer', 'string', 'string', 'integer']],
        'log' : [['double', 'double', 'double']],
        'log10' : [['double', 'double']],
        'log2' : [['double', 'double']],
        'lower' : [['varchar', 'varchar'], ['string', 'string']],
        'lpad' : [['varchar', 'varchar', 'integer', 'varchar'], ['string', 'string', 'integer', 'string']],
        'ltrim' : [['varchar', 'varchar'], ['string', 'string']],
        'make_date' : [['date', 'integer', 'integer']],
        'mask' : [['varchar', 'varchar'], ['string', 'string']],
        'mask_first_n' : [['varchar', 'varchar'], ['string', 'string'], ['varchar', 'varchar', 'integer'], ['string', 'string', 'integer']],
        'mask_last_n' : [['varchar', 'varchar'], ['string', 'string'], ['varchar', 'varchar', 'integer'], ['string', 'string', 'integer']],
        'md5' : [['varchar', 'varchar'], ['varchar', 'string']],
        'md5_sum' : [['varchar', 'varchar'], ['varchar', 'string']],
        'minute' : [['integer', 'datetime'], ['integer', 'datetimev2'], ['integer', 'datev2']],
        'minute_ceil' : [['datetime', 'datetime'], ['datetimev2', 'datetimev2'], ['datetimev2', 'datev2'], ['datetime', 'datetime', 'datetime'], ['datetime', 'datetime', 'integer'], ['datetimev2', 'datetimev2', 'datetimev2'], ['datetimev2', 'datetimev2', 'integer'], ['datetimev2', 'datev2', 'datev2'], ['datetimev2', 'datev2', 'integer'], ['datetime', 'datetime', 'integer', 'datetime'], ['datetimev2', 'datetimev2', 'integer', 'datetimev2'], ['datetimev2', 'datev2', 'integer', 'datev2']],
        'minute_floor' : [['datetime', 'datetime'], ['datetimev2', 'datetimev2'], ['datetimev2', 'datev2'], ['datetime', 'datetime', 'datetime'], ['datetime', 'datetime', 'integer'], ['datetimev2', 'datetimev2', 'datetimev2'], ['datetimev2', 'datetimev2', 'integer'], ['datetimev2', 'datev2', 'datev2'], ['datetimev2', 'datev2', 'integer'], ['datetime', 'datetime', 'integer', 'datetime'], ['datetimev2', 'datetimev2', 'integer', 'datetimev2'], ['datetimev2', 'datev2', 'integer', 'datev2']],
        'minutes_add' : [['datetime', 'datetime', 'integer'], ['datetimev2', 'datetimev2', 'integer'], ['datetime', 'date', 'integer'], ['datetimev2', 'datev2', 'integer']],
        'minutes_diff' : [['bigint', 'datetime', 'datetime'], ['bigint', 'datetimev2', 'datetimev2'], ['bigint', 'datev2', 'datetimev2'], ['bigint', 'datetimev2', 'datev2'], ['bigint', 'datev2', 'datev2'], ['bigint', 'datev2', 'datetime'], ['bigint', 'datetime', 'datev2'], ['bigint', 'datetimev2', 'datetime'], ['bigint', 'datetime', 'datetimev2']],
        'minutes_sub' : [['datetime', 'datetime', 'integer'], ['datetimev2', 'datetimev2', 'integer'], ['datetime', 'date', 'integer'], ['datetimev2', 'datev2', 'integer']],
        'money_format' : [['varchar', 'bigint'], ['varchar', 'largeint'], ['varchar', 'double'], ['varchar', 'decimalv2']],
        'month' : [['integer', 'datetime'], ['integer', 'datetimev2'], ['integer', 'datev2']],
        'month_ceil' : [['datetime', 'datetime'], ['datetimev2', 'datetimev2'], ['datev2', 'datev2'], ['datetime', 'datetime', 'datetime'], ['datetime', 'datetime', 'integer'], ['datetimev2', 'datetimev2', 'datetimev2'], ['datetimev2', 'datetimev2', 'integer'], ['datev2', 'datev2', 'datev2'], ['datev2', 'datev2', 'integer'], ['datetime', 'datetime', 'integer', 'datetime'], ['datetimev2', 'datetimev2', 'integer', 'datetimev2'], ['datev2', 'datev2', 'integer', 'datev2']],
        'month_floor' : [['datetime', 'datetime'], ['datetimev2', 'datetimev2'], ['datev2', 'datev2'], ['datetime', 'datetime', 'datetime'], ['datetime', 'datetime', 'integer'], ['datetimev2', 'datetimev2', 'datetimev2'], ['datetimev2', 'datetimev2', 'integer'], ['datev2', 'datev2', 'datev2'], ['datev2', 'datev2', 'integer'], ['datetime', 'datetime', 'integer', 'datetime'], ['datetimev2', 'datetimev2', 'integer', 'datetimev2'], ['datev2', 'datev2', 'integer', 'datev2']],
        'month_name' : [['varchar', 'datetime'], ['varchar', 'datetimev2'], ['varchar', 'datev2']],
        'months_add' : [['datetime', 'datetime', 'integer'], ['datetimev2', 'datetimev2', 'integer'], ['date', 'date', 'integer'], ['datev2', 'datev2', 'integer']],
        'months_diff' : [['bigint', 'datetime', 'datetime'], ['bigint', 'datetimev2', 'datetimev2'], ['bigint', 'datev2', 'datetimev2'], ['bigint', 'datetimev2', 'datev2'], ['bigint', 'datev2', 'datev2'], ['bigint', 'datev2', 'datetime'], ['bigint', 'datetime', 'datev2'], ['bigint', 'datetimev2', 'datetime'], ['bigint', 'datetime', 'datetimev2']],
        'months_sub' : [['datetime', 'datetime', 'integer'], ['datetimev2', 'datetimev2', 'integer'], ['date', 'date', 'integer'], ['datev2', 'datev2', 'integer']],
        'murmur_hash332' : [['integer', 'varchar'], ['integer', 'string']],
        'murmur_hash364' : [['bigint', 'varchar'], ['bigint', 'string']],
        'negative' : [['bigint', 'bigint'], ['double', 'double'], ['decimalv2', 'decimalv2']],
        'not_null_or_empty' : [['boolean', 'varchar'], ['boolean', 'string']],
        'now' : [['datetime', ''], ['datetimev2', 'integer']],
        'null_if' : [['boolean', 'boolean', 'boolean'], ['tinyint', 'tinyint', 'tinyint'], ['smallint', 'smallint', 'smallint'], ['integer', 'integer', 'integer'], ['bigint', 'bigint', 'bigint'], ['largeint', 'largeint', 'largeint'], ['float', 'float', 'float'], ['double', 'double', 'double'], ['datetime', 'datetime', 'datetime'], ['date', 'date', 'date'], ['datetimev2', 'datetimev2', 'datetimev2'], ['datev2', 'datev2', 'datev2'], ['decimalv2', 'decimalv2', 'decimalv2'], ['varchar', 'varchar', 'varchar'], ['string', 'string', 'string']],
        'null_or_empty' : [['boolean', 'varchar'], ['boolean', 'string']],
        'nvl' : [['boolean', 'boolean', 'boolean'], ['tinyint', 'tinyint', 'tinyint'], ['smallint', 'smallint', 'smallint'], ['integer', 'integer', 'integer'], ['bigint', 'bigint', 'bigint'], ['largeint', 'largeint', 'largeint'], ['float', 'float', 'float'], ['double', 'double', 'double'], ['date', 'date', 'date'], ['datetime', 'datetime', 'datetime'], ['datetime', 'date', 'datetime'], ['datetime', 'datetime', 'date'], ['datetimev2', 'datetimev2', 'datetimev2'], ['datetimev2', 'datev2', 'datetimev2'], ['datetimev2', 'datetimev2', 'datev2'], ['decimalv2', 'decimalv2', 'decimalv2'], ['bitmap', 'bitmap', 'bitmap'], ['varchar', 'varchar', 'varchar'], ['string', 'string', 'string']],
        'parse_url' : [['varchar', 'varchar', 'varchar'], ['string', 'string', 'string'], ['varchar', 'varchar', 'varchar', 'varchar'], ['string', 'string', 'string', 'string']],
        'pi' : [['double', '']],
        'pmod' : [['bigint', 'bigint', 'bigint'], ['double', 'double', 'double']],
        'positive' : [['bigint', 'bigint'], ['double', 'double'], ['decimalv2', 'decimalv2']],
        'pow' : [['double', 'double', 'double']],
        'power' : [['double', 'double', 'double']],
        'protocol' : [['string', 'string']],
        'quantile_percent' : [['double', 'quantilestate', 'float']],
        'quarter' : [['integer', 'datetime'], ['integer', 'datetimev2'], ['integer', 'datev2']],
        'radians' : [['double', 'double']],
        'random' : [['double', ''], ['double', 'bigint']],
        'regexp_extract' : [['varchar', 'varchar', 'varchar', 'bigint'], ['string', 'string', 'string', 'bigint']],
        'regexp_extract_all' : [['varchar', 'varchar', 'varchar'], ['string', 'string', 'string']],
        'regexp_replace' : [['varchar', 'varchar', 'varchar', 'varchar'], ['string', 'string', 'string', 'string']],
        'regexp_replace_one' : [['varchar', 'varchar', 'varchar', 'varchar'], ['string', 'string', 'string', 'string']],
        'repeat' : [['varchar', 'varchar', 'integer'], ['string', 'string', 'integer']],
        'replace' : [['varchar', 'varchar', 'varchar', 'varchar'], ['string', 'string', 'string', 'string']],
        'right' : [['varchar', 'varchar', 'integer'], ['string', 'string', 'integer']],
        'round' : [['double', 'double'], ['double', 'double', 'integer']],
        'round_bankers' : [['double', 'double'], ['double', 'double', 'integer']],
        'rpad' : [['varchar', 'varchar', 'integer', 'varchar'], ['string', 'string', 'integer', 'string']],
        'rtrim' : [['varchar', 'varchar'], ['string', 'string']],
        'running_difference' : [['smallint', 'tinyint'], ['integer', 'smallint'], ['bigint', 'integer'], ['largeint', 'bigint'], ['largeint', 'largeint'], ['double', 'float'], ['double', 'double'], ['decimalv2', 'decimalv2'], ['decimalv3.defaultdecimal32', 'decimalv3.defaultdecimal32'], ['decimalv3.defaultdecimal64', 'decimalv3.defaultdecimal64'], ['decimalv3.defaultdecimal128', 'decimalv3.defaultdecimal128'], ['integer', 'date'], ['integer', 'datev2'], ['double', 'datetime'], ['double', 'datetimev2']],
        'second' : [['integer', 'datetime'], ['integer', 'datetimev2'], ['integer', 'datev2']],
        'second_ceil' : [['datetime', 'datetime'], ['datetimev2', 'datetimev2'], ['datetimev2', 'datev2'], ['datetime', 'datetime', 'datetime'], ['datetime', 'datetime', 'integer'], ['datetimev2', 'datetimev2', 'datetimev2'], ['datetimev2', 'datetimev2', 'integer'], ['datetimev2', 'datev2', 'datev2'], ['datetimev2', 'datev2', 'integer'], ['datetime', 'datetime', 'integer', 'datetime'], ['datetimev2', 'datetimev2', 'integer', 'datetimev2'], ['datetimev2', 'datev2', 'integer', 'datev2']],
        'second_floor' : [['datetime', 'datetime'], ['datetimev2', 'datetimev2'], ['datetimev2', 'datev2'], ['datetime', 'datetime', 'datetime'], ['datetime', 'datetime', 'integer'], ['datetimev2', 'datetimev2', 'datetimev2'], ['datetimev2', 'datetimev2', 'integer'], ['datetimev2', 'datev2', 'datev2'], ['datetimev2', 'datev2', 'integer'], ['datetime', 'datetime', 'integer', 'datetime'], ['datetimev2', 'datetimev2', 'integer', 'datetimev2'], ['datetimev2', 'datev2', 'integer', 'datev2']],
        'seconds_add' : [['datetime', 'datetime', 'integer'], ['datetimev2', 'datetimev2', 'integer'], ['datetime', 'date', 'integer'], ['datetimev2', 'datev2', 'integer']],
        'seconds_diff' : [['bigint', 'datetime', 'datetime'], ['bigint', 'datetimev2', 'datetimev2'], ['bigint', 'datev2', 'datetimev2'], ['bigint', 'datetimev2', 'datev2'], ['bigint', 'datev2', 'datev2'], ['bigint', 'datev2', 'datetime'], ['bigint', 'datetime', 'datev2'], ['bigint', 'datetimev2', 'datetime'], ['bigint', 'datetime', 'datetimev2']],
        'seconds_sub' : [['datetime', 'datetime', 'integer'], ['datetimev2', 'datetimev2', 'integer'], ['datetime', 'date', 'integer'], ['datetimev2', 'datev2', 'integer']],
        'sign' : [['tinyint', 'double']],
        'sin' : [['double', 'double']],
        'sleep' : [['boolean', 'integer']],
        'sm3' : [['varchar', 'varchar'], ['varchar', 'string']],
        'sm3sum' : [['varchar', 'varchar'], ['varchar', 'string']],
        'sm4_decrypt' : [['varchar', 'varchar', 'varchar'], ['string', 'string', 'string'], ['varchar', 'varchar', 'varchar', 'varchar'], ['string', 'string', 'string', 'string'], ['varchar', 'varchar', 'varchar', 'varchar', 'varchar'], ['string', 'string', 'string', 'string', 'string']],
        'sm4_encrypt' : [['varchar', 'varchar', 'varchar'], ['string', 'string', 'string'], ['varchar', 'varchar', 'varchar', 'varchar'], ['string', 'string', 'string', 'string'], ['varchar', 'varchar', 'varchar', 'varchar', 'varchar'], ['string', 'string', 'string', 'string', 'string']],
        'space' : [['varchar', 'integer']],
        'split_part' : [['varchar', 'varchar', 'varchar', 'integer'], ['string', 'string', 'string', 'integer']],
        'sqrt' : [['double', 'double']],
        'st_astext' : [['varchar', 'varchar'], ['varchar', 'string']],
        'st_aswkt' : [['varchar', 'varchar'], ['varchar', 'string']],
        'st_circle' : [['varchar', 'double', 'double', 'double']],
        'st_contains' : [['boolean', 'varchar', 'varchar']],
        'st_distance_sphere' : [['double', 'double', 'double', 'double', 'double']],
        'st_geometryfromtext' : [['varchar', 'varchar'], ['varchar', 'string']],
        'st_geomfromtext' : [['varchar', 'varchar'], ['varchar', 'string']],
        'st_linefromtext' : [['varchar', 'varchar'], ['varchar', 'string']],
        'st_linestringfromtext' : [['varchar', 'varchar'], ['varchar', 'string']],
        'st_point' : [['varchar', 'double', 'double']],
        'st_polyfromtext' : [['varchar', 'varchar'], ['varchar', 'string']],
        'st_polygon' : [['varchar', 'varchar'], ['varchar', 'string']],
        'st_polygonfromtext' : [['varchar', 'varchar'], ['varchar', 'string']],
        'st_x' : [['double', 'varchar'], ['double', 'string']],
        'st_y' : [['double', 'varchar'], ['double', 'string']],
        'starts_with' : [['boolean', 'varchar', 'varchar'], ['boolean', 'string', 'string']],
        'str_left' : [['varchar', 'varchar', 'integer'], ['string', 'string', 'integer']],
        'str_right' : [['varchar', 'varchar', 'integer'], ['string', 'string', 'integer']],
        'str_to_date' : [['datetime', 'varchar', 'varchar'], ['datetime', 'string', 'string']],
        'sub_bitmap' : [['bitmap', 'bitmap', 'bigint', 'bigint']],
        'sub_replace' : [['varchar', 'varchar', 'varchar', 'integer'], ['string', 'string', 'string', 'integer'], ['varchar', 'varchar', 'varchar', 'integer', 'integer'], ['string', 'string', 'string', 'integer', 'integer']],
        'substring' : [['varchar', 'varchar', 'integer'], ['string', 'string', 'integer'], ['varchar', 'varchar', 'integer', 'integer'], ['string', 'string', 'integer', 'integer']],
        'substring_index' : [['varchar', 'varchar', 'varchar', 'integer'], ['string', 'string', 'string', 'integer']],
        'tan' : [['double', 'double']],
        'time_diff' : [['time', 'datetime', 'datetime'], ['timev2', 'datetimev2', 'datetimev2'], ['timev2', 'datetimev2', 'datev2'], ['timev2', 'datev2', 'datetimev2'], ['timev2', 'datev2', 'datev2'], ['timev2', 'datetimev2', 'datetime'], ['timev2', 'datev2', 'datetime']],
        'timestamp' : [['datetime', 'datetime'], ['datetimev2', 'datetimev2']],
        'to_base64' : [['string', 'string']],
        'to_bitmap' : [['bitmap', 'varchar'], ['bitmap', 'string']],
        'to_bitmap_with_check' : [['bitmap', 'varchar'], ['bitmap', 'string']],
        'to_date' : [['date', 'datetime'], ['datev2', 'datetimev2']],
        'to_date_v2' : [['datev2', 'datetimev2']],
        'to_days' : [['integer', 'date'], ['integer', 'datev2']],
        'to_monday' : [['datev2', 'datetimev2'], ['datev2', 'datev2'], ['date', 'datetime'], ['date', 'date']],
        'to_quantile_state' : [['quantilestate', 'varchar', 'float']],
        'trim' : [['varchar', 'varchar'], ['string', 'string']],
        'truncate' : [['double', 'double', 'integer']],
        'unhex' : [['varchar', 'varchar'], ['string', 'string']],
        'unix_timestamp' : [['integer', ''], ['integer', 'datetime'], ['integer', 'date'], ['integer', 'datetimev2'], ['integer', 'datev2'], ['integer', 'varchar', 'varchar'], ['integer', 'string', 'string']],
        'upper' : [['varchar', 'varchar'], ['string', 'string']],
        'user' : [['varchar', '']],
        'utc_timestamp' : [['datetime', '']],
        'uuid' : [['varchar', '']],
        'version' : [['varchar', '']],
        'week' : [['integer', 'datetime'], ['integer', 'datetimev2'], ['integer', 'datev2'], ['integer', 'datetime', 'integer'], ['integer', 'datetimev2', 'integer'], ['integer', 'datev2', 'integer']],
        'week_ceil' : [['datetime', 'datetime'], ['datetimev2', 'datetimev2'], ['datev2', 'datev2'], ['datetime', 'datetime', 'datetime'], ['datetime', 'datetime', 'integer'], ['datetimev2', 'datetimev2', 'datetimev2'], ['datetimev2', 'datetimev2', 'integer'], ['datev2', 'datev2', 'datev2'], ['datev2', 'datev2', 'integer'], ['datetime', 'datetime', 'integer', 'datetime'], ['datetimev2', 'datetimev2', 'integer', 'datetimev2'], ['datev2', 'datev2', 'integer', 'datev2']],
        'week_floor' : [['datetime', 'datetime'], ['datetimev2', 'datetimev2'], ['datev2', 'datev2'], ['datetime', 'datetime', 'datetime'], ['datetime', 'datetime', 'integer'], ['datetimev2', 'datetimev2', 'datetimev2'], ['datetimev2', 'datetimev2', 'integer'], ['datev2', 'datev2', 'datev2'], ['datev2', 'datev2', 'integer'], ['datetime', 'datetime', 'integer', 'datetime'], ['datetimev2', 'datetimev2', 'integer', 'datetimev2'], ['datev2', 'datev2', 'integer', 'datev2']],
        'week_of_year' : [['integer', 'datetime'], ['integer', 'datetimev2'], ['integer', 'datev2']],
        'weekday' : [['integer', 'datetime'], ['integer', 'datetimev2'], ['integer', 'datev2']],
        'weeks_diff' : [['bigint', 'datetime', 'datetime'], ['bigint', 'datetimev2', 'datetimev2'], ['bigint', 'datev2', 'datetimev2'], ['bigint', 'datetimev2', 'datev2'], ['bigint', 'datev2', 'datev2'], ['bigint', 'datev2', 'datetime'], ['bigint', 'datetime', 'datev2'], ['bigint', 'datetimev2', 'datetime'], ['bigint', 'datetime', 'datetimev2']],
        'year' : [['integer', 'datev2'], ['integer', 'datetime'], ['integer', 'datetimev2']],
        'year_ceil' : [['datetime', 'datetime'], ['datetimev2', 'datetimev2'], ['datev2', 'datev2'], ['datetime', 'datetime', 'datetime'], ['datetime', 'datetime', 'integer'], ['datetimev2', 'datetimev2', 'datetimev2'], ['datetimev2', 'datetimev2', 'integer'], ['datev2', 'datev2', 'datev2'], ['datev2', 'datev2', 'integer'], ['datetime', 'datetime', 'integer', 'datetime'], ['datetimev2', 'datetimev2', 'integer', 'datetimev2'], ['datev2', 'datev2', 'integer', 'datev2']],
        'year_floor' : [['datetime', 'datetime'], ['datetimev2', 'datetimev2'], ['datev2', 'datev2'], ['datetime', 'datetime', 'datetime'], ['datetime', 'datetime', 'integer'], ['datetimev2', 'datetimev2', 'datetimev2'], ['datetimev2', 'datetimev2', 'integer'], ['datev2', 'datev2', 'datev2'], ['datev2', 'datev2', 'integer'], ['datetime', 'datetime', 'integer', 'datetime'], ['datetimev2', 'datetimev2', 'integer', 'datetimev2'], ['datev2', 'datev2', 'integer', 'datev2']],
        'year_week' : [['integer', 'datetime'], ['integer', 'datetimev2'], ['integer', 'datev2'], ['integer', 'datetime', 'integer'], ['integer', 'datetimev2', 'integer'], ['integer', 'datev2', 'integer']],
        'years_add' : [['datetime', 'datetime', 'integer'], ['datetimev2', 'datetimev2', 'integer'], ['date', 'date', 'integer'], ['datev2', 'datev2', 'integer']],
        'years_diff' : [['bigint', 'datetime', 'datetime'], ['bigint', 'datetimev2', 'datetimev2'], ['bigint', 'datev2', 'datetimev2'], ['bigint', 'datetimev2', 'datev2'], ['bigint', 'datev2', 'datev2'], ['bigint', 'datev2', 'datetime'], ['bigint', 'datetime', 'datev2'], ['bigint', 'datetimev2', 'datetime'], ['bigint', 'datetime', 'datetimev2']],
        'years_sub' : [['datetime', 'datetime', 'integer'], ['datetimev2', 'datetimev2', 'integer'], ['date', 'date', 'integer'], ['datev2', 'datev2', 'integer']]
    ]

    def agg_function = [: ]
    def array_scalar_function = [: ]
    def array_agg_function = [: ]
    // function table end

    // test begin
    def typeToColumn = [
        'tinyint' : ['ktint'],
        'smallint' : ['ksint'],
        'integer' : ['kint'],
        'bigint' : ['kbint'],
        'largeint' : ['klint'],
        'float' : ['kfloat'],
        'double' : ['kdbl'],
        'decimalv2' : ['kdcmls1'],
        'decimalv3_32' : ['kdcmlv3s1', 'kdcmlv3s2', 'kdcmlv3s3'],
        'decimalv3_64' : ['kdcmlv3s2'],
        'decimalv3_128' : ['kdcmlv3s3'],
        'char' : ['kchrs1'],
        'varchar' : ['kvchrs1'],
        'string' : ['kstr'],
        'date' : ['kdt'],
        'datetime' : ['kdtm'],
        'datev2' : ['kdtv2'],
        'datetimev2' : ['kdtmv2s1']
    ]

    def isError = false

    // key is string, value is array
    scalar_function.each { fn_name, v ->
        v.each {
            List<String> types
            try {
                types = it.subList(1, it.size()).collect {
                    typeToColumn[it][0]
                }
            } catch (Exception ignored) {
                logger.warn "${fn_name} with argument ${it} is not test, because framework does not support yet"
                return
            }
            def args = String.join(',', types)
            def fn = "${fn_name}(${args})"
            def scalar_sql = "select ${fn} from t order by ${args}"
            try {
                sql scalar_sql
            } catch (Exception e) {
                logger.error e.getMessage()
                isError = true
            }
        }
    }
    if (isError) {
        throw new IllegalStateException("TestError, please check log")
    }
    // test end
}