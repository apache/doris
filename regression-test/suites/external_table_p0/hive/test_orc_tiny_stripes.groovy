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

suite("test_orc_tiny_stripes", "p0,external,hive,external_docker,external_docker_hive") {

    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("diable Hive test.")
        return;

    }

    for (String hivePrefix : ["hive2"]) {
        try {
            String hms_port = context.config.otherConfigs.get(hivePrefix + "HmsPort")
            String catalog_name = "${hivePrefix}_test_orc_tiny_stripes"
            String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

            sql """drop catalog if exists ${catalog_name}"""
            sql """create catalog if not exists ${catalog_name} properties (
                "type"="hms",
                'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}'
            );"""
            sql """use `${catalog_name}`.`default`"""



            
            def orc_configs = [
                [0,0,0],
                [0,10230,1024],
                [1,1,1],
                [201,130,0],
                [1024,1024,0],
                [1024,1024,1024],
                [4096,1024,0],    
                [1024,4096,0],
                [1,10240,10000000],
                [1000000,888888888,0],
                [1000000000000,1000000000000,100000000000]
            ]
            def li = [ "set enable_orc_lazy_materialization=true;","set enable_orc_lazy_materialization=false;"] 


            li.each  { it1 -> 
                sql it1 

                orc_configs.each { it2 ->
                    def value1 = it2[0].toString()
                    def value2 = it2[1].toString()
                    def value3 = it2[2].toString()

                    sql "set orc_tiny_stripe_threshold_bytes = " + value1 + ";"
                    sql "set orc_once_max_read_bytes = " + value2 + ";"
                    sql "set orc_max_merge_distance_bytes = " + value3 + ";"


                    qt_test_1 """ select count(*) from orc_tiny_stripes; """  //372

/*
*/

                    qt_test_2 """ select * from orc_tiny_stripes where col1 = 1 order by col1,col2,col3; """  
/*
1       str_1   10000000001
1       str_1   10000000001
*/                    
                    qt_test_3 """ select * from orc_tiny_stripes where col1%100 = 0 order by col1,col2,col3 ; """
/*
0       str_0   10000000000
0       str_0   10000000000
100     9DPJaFc00euBteqiW1f1    10000000027
100     str_100 10000000100
2200    tQ7BRFEuf8h56kahqsLPa1vu        10000000034
4800    TaWGgh4iZ       10000000115
5700    SwOaGJj9fVbk5j0Np       10000000050
*/

                    qt_test_4 """ select * from orc_tiny_stripes where col2 = "str_4" order by col1,col2,col3; """
/*
4       str_4   10000000004
4       str_4   10000000004
*/ 
                    qt_test_5 """ select count(*) from orc_tiny_stripes  where col3 > 10000000005; """ //348
                    qt_test_6 """ select * from orc_tiny_stripes  where col3 in ( 10000000005,10000000053,10000000146) order by col1,col2,col3 ; """ 
/*
5       str_5   10000000005
5       str_5   10000000005
53      str_53  10000000053
146     str_146 10000000146
3961    hMgIY4oui0MHYgaIFg4zz5Ti3p      10000000053
4129    qwPIwtkTZb      10000000005
4942    vAdLpLUN3VkGNmTjvuPv    10000000053
5349    koTeYPr2Qaqqnlk07X      10000000146
5745    1cx1jZ6QGRWAkskiOgURj6dscYxDOl  10000000005
7573    e3lIPwNnbG6DPmog        10000000005
8614    TtyopDvRptLB5   10000000005                    
*/                  
                    
                    qt_test_7 """ select * from orc_tiny_stripes  where col3 in ( 10000000005,10000000053,10000000146) order by col1,col2,col3 ; """ 
/*
5       str_5   10000000005
5       str_5   10000000005
53      str_53  10000000053
146     str_146 10000000146
3961    hMgIY4oui0MHYgaIFg4zz5Ti3p      10000000053
4129    qwPIwtkTZb      10000000005
4942    vAdLpLUN3VkGNmTjvuPv    10000000053
5349    koTeYPr2Qaqqnlk07X      10000000146
5745    1cx1jZ6QGRWAkskiOgURj6dscYxDOl  10000000005
7573    e3lIPwNnbG6DPmog        10000000005
8614    TtyopDvRptLB5   10000000005
*/                  
                    
                    qt_test_8 """ select col3 from orc_tiny_stripes  where col3 in ( 10000000005,10000000053,10000000146) order by col3 ; """ 
/*
10000000005
10000000005
10000000005
10000000005
10000000005
10000000005
10000000053
10000000053
10000000053
10000000146
10000000146
*/

                    qt_test_9 """ select col1 from orc_tiny_stripes  where col1 in (10,1000) order by col1 ; """ // 10 
                    qt_test_10 """ select col2 from orc_tiny_stripes  where length(col2) > 29 order by col2  ; """
/*
1cx1jZ6QGRWAkskiOgURj6dscYxDOl
Asn3tnIg1xYm8Lbgey8baqw3EmooFm
MSBtFURjtMu3LyDTLYx9FBM23UQdZ1
e8e7xgwaSI2JKI65FEThzSQBVmKeAZ
w3xAirHLO1tvjon2jgr7y9tBtrGfMS
zABBLCkowUIqfONQOAjir8YPkFqfDW
*/
                    qt_test_11 """ select * from orc_tiny_stripes  where col1 < 10 order by col1,col2,col3; """ 
/*
0       str_0   10000000000
0       str_0   10000000000
1       str_1   10000000001
1       str_1   10000000001
2       str_2   10000000002
2       str_2   10000000002
3       str_3   10000000003
3       str_3   10000000003
4       str_4   10000000004
4       str_4   10000000004
5       str_5   10000000005
5       str_5   10000000005
6       str_6   10000000006
7       str_7   10000000007
8       str_8   10000000008
9       str_9   10000000009
*/

                    qt_test_12 """ select col1 from orc_tiny_stripes  where col1 in(0,6 ) order by  col1; """ 
/*
0
0
6
*/
 
                    qt_test_13 """ select col1 from orc_tiny_stripes  where col1 in(20,60 ) order by  col1; """ 
 /*
20
60
*/
 
                    qt_test_14 """ select col1 from orc_tiny_stripes  where col1 in(40,0 ) order by col1; """ 
/*
0
0
40
*/


                }
            }

            sql """drop catalog if exists ${catalog_name}"""
        } finally {
        }
    }
}

