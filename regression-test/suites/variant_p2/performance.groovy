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

suite("regression_test_variant_performance", "p2"){
    sql """CREATE TABLE IF NOT EXISTS var_perf (
                        k bigint,
                        v variant

                    )
                    DUPLICATE KEY(`k`)
                    DISTRIBUTED BY RANDOM BUCKETS 4
                    properties("replication_num" = "1", "disable_auto_compaction" = "false");
    """
    sql """
        insert into var_perf 
                SELECT *, '{"field1":348,"field2":596,"field3":781,"field4":41,"field5":922,"field6":84,"field7":222,"field8":312,"field9":490,"field10":715,"field11":837,"field12":753,"field13":171,"field14":727,"field15":739,"field16":545,"field17":964,"field18":540,"field19":685,"field20":828,"field21":157,"field22":404,"field23":287,"field24":481,"field25":476,"field26":559,"field27":144,"field28":545,"field29":70,"field30":668,"field31":820,"field32":193,"field33":465,"field34":347,"field35":898,"field36":705,"field37":754,"field38":866,"field39":752,"field40":303,"field41":214,"field42":41,"field43":609,"field44":487,"field45":832,"field46":832,"field47":134,"field48":964,"field49":919,"field50":670,"field51":767,"field52":334,"field53":506,"field54":838,"field55":510,"field56":770,"field57":168,"field58":701,"field59":961,"field60":927,"field61":375,"field62":939,"field63":464,"field64":420,"field65":212,"field66":882,"field67":344,"field68":724,"field69":997,"field70":198,"field71":739,"field72":628,"field73":563,"field74":979,"field75":563,"field76":891,"field77":496,"field78":442,"field79":847,"field80":771,"field81":229,"field82":1023,"field83":184,"field84":563,"field85":980,"field86":191,"field87":426,"field88":527,"field89":945,"field90":552,"field91":454,"field92":728,"field93":631,"field94":191,"field95":148,"field96":679,"field97":955,"field98":934,"field99":258,"field100":442}'
                        from numbers("number" = "10000000")
                union all
                SELECT *, '{"field1":201,"field2":465,"field3":977,"field4":101112,"field5":131415,"field6":216,"field7":192021,"field8":822324,"field9":525627,"field10":928930,"field11":413233,"field12":243536,"field13":373839,"field14":404142,"field15":434445,"field16":1464748,"field17":495051,"field18":525354,"field19":565657,"field20":1585960,"field21":616263,"field22":646566,"field23":676869,"field24":707172,"field25":737475,"field26":767778,"field27":798081,"field28":828384,"field29":858687,"field30":888990,"field31":919293,"field32":949596,"field33":979899,"field34":100101,"field35":103104,"field36":106107,"field37":109110,"field38":112113,"field39":115116,"field40":118119,"field41":121122,"field42":124125,"field43":127128,"field44":130131,"field45":133134,"field46":136137,"field47":139140,"field48":142143,"field49":145146,"field50":148149,"field51":151152,"field52":154155,"field53":157158,"field54":160161,"field55":163164,"field56":166167,"field57":169170,"field58":172173,"field59":175176,"field60":178179,"field61":181182,"field62":184185,"field63":187188,"field64":190191,"field65":193194,"field66":196197,"field67":199200,"field68":202203,"field69":205206,"field70":208209,"field71":211212,"field72":214215,"field73":217218,"field74":220221,"field75":223224,"field76":226227,"field77":229230,"field78":232233,"field79":235236,"field80":238239,"field81":241242,"field82":244245,"field83":247248,"field84":250251,"field85":253254,"field86":256257,"field87":259260,"field88":262263,"field89":265266,"field90":268269,"field91":271272,"field92":274275,"field93":277278,"field94":280281,"field95":283284,"field96":286287,"field97":289290,"field98":292293,"field99":295296,"field100":298299}'
                        from numbers("number" = "10000000")
        """
}