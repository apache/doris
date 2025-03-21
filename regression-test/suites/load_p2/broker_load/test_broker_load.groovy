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

suite("test_broker_load_p2", "p2") {
    def s3BucketName = getS3BucketName()
    def s3Endpoint = getS3Endpoint()
    def s3Region = getS3Region()
    def tables = ["part",             // case 0
                  "upper_case",       // case 1
                  "reverse",          // case 2
                  "set1",             // case 3
                  "set2",             // case 4
                  "set3",             // case 5
                  "set4",             // case 6
                  "set5",             // case 7
                  "set6",             // case 8
                  "set7",             // case 9
                  "null_default",     // case 10
                  "filter",           // case 11
                  "path_column",      // case 12
                  "parquet_s3_case1", // case 13, col1 not in file but in table, will load default value for it.
                  "parquet_s3_case2", // case 14, x1 not in file, not in table, will throw "col not found" error.
                  "parquet_s3_case3", // case 15, p_comment not in table but in file, load normally.
                  "parquet_s3_case4", // case 16, all columns are in table but not in file, will fill default values.
                  "parquet_s3_case5", // case 17, x1 not in file, not in table, will throw "col not found" error.
                  "parquet_s3_case6", // case 18, normal
                  "parquet_s3_case7", // case 19, col5 will be ignored, load normally
                  "parquet_s3_case8", // case 20, first column in table is not specified, will load default value for it.
                  "orc_s3_case1", // case 21, table column capitalize * load column lowercase * orc file lowercase
                  "orc_s3_case2", // case 22, table column lowercase * load column lowercase * orc file lowercase
                  "orc_s3_case3", // case 23, table column lowercase * load column uppercase * orc file lowercase
                  "orc_s3_case4", // case 24, table column lowercase * load column lowercase * orc file uppercase
                  "orc_s3_case5", // case 25, table column lowercase * load column uppercase * orc file uppercase
                  "orc_s3_case6", // case 26, table column uppercase * load column uppercase * orc file lowercase
                  "orc_s3_case7", // case 27, table column uppercase * load column lowercase * orc file lowercase
                  "orc_s3_case8", // case 28, table column uppercase * load column uppercase * orc file uppercase
                  "orc_s3_case9", // case 29, table column uppercase * load column lowercase * orc file uppercase
                  "csv_s3_case_line_delimiter" // case 30, csv format table with special line delimiter
                 ]
    def paths = ["s3://${s3BucketName}/regression/load/data/part*",                              // case 0
                 "s3://${s3BucketName}/regression/load/data/part*",                              // case 1
                 "s3://${s3BucketName}/regression/load/data/part*",                              // case 2
                 "s3://${s3BucketName}/regression/load/data/part*",                              // case 3
                 "s3://${s3BucketName}/regression/load/data/part*",                              // case 4
                 "s3://${s3BucketName}/regression/load/data/part*",                              // case 5
                 "s3://${s3BucketName}/regression/load/data/part*",                              // case 6
                 "s3://${s3BucketName}/regression/load/data/part*",                              // case 7
                 "s3://${s3BucketName}/regression/load/data/part*",                              // case 8
                 "s3://${s3BucketName}/regression/load/data/part*",                              // case 9
                 "s3://${s3BucketName}/regression/load/data/part*",                              // case 10
                 "s3://${s3BucketName}/regression/load/data/part*",                              // case 11
                 "s3://${s3BucketName}/regression/load/data/path/*/part*",                       // case 12
                 "s3://${s3BucketName}/regression/load/data/part*",                              // case 13
                 "s3://${s3BucketName}/regression/load/data/part*",                              // case 14
                 "s3://${s3BucketName}/regression/load/data/part*",                              // case 15
                 "s3://${s3BucketName}/regression/load/data/part*",                              // case 16
                 "s3://${s3BucketName}/regression/load/data/part*",                              // case 17
                 "s3://${s3BucketName}/regression/load/data/part*",                              // case 18
                 "s3://${s3BucketName}/regression/load/data/part*",                              // case 19
                 "s3://${s3BucketName}/regression/load/data/part*",                              // case 20
                 "s3://${s3BucketName}/regression/load/data/orc/hits_100k_rows.orc",             // case 21
                 "s3://${s3BucketName}/regression/load/data/orc/hits_10k_rows_lowercase.orc",    // case 22
                 "s3://${s3BucketName}/regression/load/data/orc/hits_10k_rows_lowercase.orc",    // case 23
                 "s3://${s3BucketName}/regression/load/data/orc/hits_10k_rows_uppercase.orc",    // case 24
                 "s3://${s3BucketName}/regression/load/data/orc/hits_10k_rows_uppercase.orc",    // case 25
                 "s3://${s3BucketName}/regression/load/data/orc/hits_10k_rows_lowercase.orc",    // case 26
                 "s3://${s3BucketName}/regression/load/data/orc/hits_10k_rows_lowercase.orc",    // case 27
                 "s3://${s3BucketName}/regression/load/data/orc/hits_10k_rows_uppercase.orc",    // case 28
                 "s3://${s3BucketName}/regression/load/data/orc/hits_10k_rows_uppercase.orc",    // case 29
                 "s3://${s3BucketName}/regression/line_delimiter/lineitem_0x7.csv.gz"            // case 30
    ]
    def columns_list = ["""p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment""",  // case 0
                   """p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment""",       // case 1
                   """p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment""",       // case 2
                   """p_partkey, p_name, p_size""",                                                                       // case 3
                   """p_partkey""",                                                                                       // case 4
                   """p_partkey""",                                                                                       // case 5
                   """p_partkey,  p_size""",                                                                              // case 6
                   """p_partkey""",                                                                                       // case 7
                   """p_partkey,  p_size""",                                                                              // case 8
                   """p_partkey,  p_size""",                                                                              // case 9
                   """p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment""",       // case 10
                   """p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment""",       // case 11
                   """p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment""",       // case 12
                   """p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment, col1""", // case 13
                   """p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment, x1""",   // case 14
                   """p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment""",       // case 15
                   """col1, col2, col3, col4""",                                                                          // case 16
                   """p_partkey, p_name, p_mfgr, x1""",                                                                   // case 17
                   """p_partkey, p_name, p_mfgr, p_brand""",                                                              // case 18
                   """p_partkey, p_name, p_mfgr, p_brand""",                                                              // case 19
                   """p_name, p_mfgr""",                                                                                  // case 20
                   """watchid,javaenable,title,goodevent,eventtime,eventdate,counterid,clientip,regionid,userid,counterclass,os,useragent,url,referer,isrefresh,referercategoryid,refererregionid,urlcategoryid,urlregionid,resolutionwidth,resolutionheight,resolutiondepth,flashmajor,flashminor,flashminor2,netmajor,netminor,useragentmajor,useragentminor,cookieenable,javascriptenable,ismobile,mobilephone,mobilephonemodel,params,ipnetworkid,traficsourceid,searchengineid,searchphrase,advengineid,isartifical,windowclientwidth,windowclientheight,clienttimezone,clienteventtime,silverlightversion1,silverlightversion2,silverlightversion3,silverlightversion4,pagecharset,codeversion,islink,isdownload,isnotbounce,funiqid,originalurl,hid,isoldcounter,isevent,isparameter,dontcounthits,withhash,hitcolor,localeventtime,age,sex,income,interests,robotness,remoteip,windowname,openername,historylength,browserlanguage,browsercountry,socialnetwork,socialaction,httperror,sendtiming,dnstiming,connecttiming,responsestarttiming,responseendtiming,fetchtiming,socialsourcenetworkid,socialsourcepage,paramprice,paramorderid,paramcurrency,paramcurrencyid,openstatservicename,openstatcampaignid,openstatadid,openstatsourceid,utmsource,utmmedium,utmcampaign,utmcontent,utmterm,fromtag,hasgclid,refererhash,urlhash,clid""",  // case 21
                   """watchid,javaenable,title,goodevent,eventtime,eventdate,counterid,clientip,regionid,userid,counterclass,os,useragent,url,referer,isrefresh,referercategoryid,refererregionid,urlcategoryid,urlregionid,resolutionwidth,resolutionheight,resolutiondepth,flashmajor,flashminor,flashminor2,netmajor,netminor,useragentmajor,useragentminor,cookieenable,javascriptenable,ismobile,mobilephone,mobilephonemodel,params,ipnetworkid,traficsourceid,searchengineid,searchphrase,advengineid,isartifical,windowclientwidth,windowclientheight,clienttimezone,clienteventtime,silverlightversion1,silverlightversion2,silverlightversion3,silverlightversion4,pagecharset,codeversion,islink,isdownload,isnotbounce,funiqid,originalurl,hid,isoldcounter,isevent,isparameter,dontcounthits,withhash,hitcolor,localeventtime,age,sex,income,interests,robotness,remoteip,windowname,openername,historylength,browserlanguage,browsercountry,socialnetwork,socialaction,httperror,sendtiming,dnstiming,connecttiming,responsestarttiming,responseendtiming,fetchtiming,socialsourcenetworkid,socialsourcepage,paramprice,paramorderid,paramcurrency,paramcurrencyid,openstatservicename,openstatcampaignid,openstatadid,openstatsourceid,utmsource,utmmedium,utmcampaign,utmcontent,utmterm,fromtag,hasgclid,refererhash,urlhash,clid""",  // case 22
                   """watchid,javaenable,title,goodevent,eventtime,eventdate,counterid,clientip,regionid,userid,counterclass,os,useragent,url,referer,isrefresh,referercategoryid,refererregionid,urlcategoryid,urlregionid,resolutionwidth,resolutionheight,resolutiondepth,flashmajor,flashminor,flashminor2,netmajor,netminor,useragentmajor,useragentminor,cookieenable,javascriptenable,ismobile,mobilephone,mobilephonemodel,params,ipnetworkid,traficsourceid,searchengineid,searchphrase,advengineid,isartifical,windowclientwidth,windowclientheight,clienttimezone,clienteventtime,silverlightversion1,silverlightversion2,silverlightversion3,silverlightversion4,pagecharset,codeversion,islink,isdownload,isnotbounce,funiqid,originalurl,hid,isoldcounter,isevent,isparameter,dontcounthits,withhash,hitcolor,localeventtime,age,sex,income,interests,robotness,remoteip,windowname,openername,historylength,browserlanguage,browsercountry,socialnetwork,socialaction,httperror,sendtiming,dnstiming,connecttiming,responsestarttiming,responseendtiming,fetchtiming,socialsourcenetworkid,socialsourcepage,paramprice,paramorderid,paramcurrency,paramcurrencyid,openstatservicename,openstatcampaignid,openstatadid,openstatsourceid,utmsource,utmmedium,utmcampaign,utmcontent,utmterm,fromtag,hasgclid,refererhash,urlhash,clid""",  // case 23
                   """watchid,javaenable,title,goodevent,eventtime,eventdate,counterid,clientip,regionid,userid,counterclass,os,useragent,url,referer,isrefresh,referercategoryid,refererregionid,urlcategoryid,urlregionid,resolutionwidth,resolutionheight,resolutiondepth,flashmajor,flashminor,flashminor2,netmajor,netminor,useragentmajor,useragentminor,cookieenable,javascriptenable,ismobile,mobilephone,mobilephonemodel,params,ipnetworkid,traficsourceid,searchengineid,searchphrase,advengineid,isartifical,windowclientwidth,windowclientheight,clienttimezone,clienteventtime,silverlightversion1,silverlightversion2,silverlightversion3,silverlightversion4,pagecharset,codeversion,islink,isdownload,isnotbounce,funiqid,originalurl,hid,isoldcounter,isevent,isparameter,dontcounthits,withhash,hitcolor,localeventtime,age,sex,income,interests,robotness,remoteip,windowname,openername,historylength,browserlanguage,browsercountry,socialnetwork,socialaction,httperror,sendtiming,dnstiming,connecttiming,responsestarttiming,responseendtiming,fetchtiming,socialsourcenetworkid,socialsourcepage,paramprice,paramorderid,paramcurrency,paramcurrencyid,openstatservicename,openstatcampaignid,openstatadid,openstatsourceid,utmsource,utmmedium,utmcampaign,utmcontent,utmterm,fromtag,hasgclid,refererhash,urlhash,clid""",  // case 24
                   """watchid,javaenable,title,goodevent,eventtime,eventdate,counterid,clientip,regionid,userid,counterclass,os,useragent,url,referer,isrefresh,referercategoryid,refererregionid,urlcategoryid,urlregionid,resolutionwidth,resolutionheight,resolutiondepth,flashmajor,flashminor,flashminor2,netmajor,netminor,useragentmajor,useragentminor,cookieenable,javascriptenable,ismobile,mobilephone,mobilephonemodel,params,ipnetworkid,traficsourceid,searchengineid,searchphrase,advengineid,isartifical,windowclientwidth,windowclientheight,clienttimezone,clienteventtime,silverlightversion1,silverlightversion2,silverlightversion3,silverlightversion4,pagecharset,codeversion,islink,isdownload,isnotbounce,funiqid,originalurl,hid,isoldcounter,isevent,isparameter,dontcounthits,withhash,hitcolor,localeventtime,age,sex,income,interests,robotness,remoteip,windowname,openername,historylength,browserlanguage,browsercountry,socialnetwork,socialaction,httperror,sendtiming,dnstiming,connecttiming,responsestarttiming,responseendtiming,fetchtiming,socialsourcenetworkid,socialsourcepage,paramprice,paramorderid,paramcurrency,paramcurrencyid,openstatservicename,openstatcampaignid,openstatadid,openstatsourceid,utmsource,utmmedium,utmcampaign,utmcontent,utmterm,fromtag,hasgclid,refererhash,urlhash,clid""",  // case 25
                   """watchid,javaenable,title,goodevent,eventtime,eventdate,counterid,clientip,regionid,userid,counterclass,os,useragent,url,referer,isrefresh,referercategoryid,refererregionid,urlcategoryid,urlregionid,resolutionwidth,resolutionheight,resolutiondepth,flashmajor,flashminor,flashminor2,netmajor,netminor,useragentmajor,useragentminor,cookieenable,javascriptenable,ismobile,mobilephone,mobilephonemodel,params,ipnetworkid,traficsourceid,searchengineid,searchphrase,advengineid,isartifical,windowclientwidth,windowclientheight,clienttimezone,clienteventtime,silverlightversion1,silverlightversion2,silverlightversion3,silverlightversion4,pagecharset,codeversion,islink,isdownload,isnotbounce,funiqid,originalurl,hid,isoldcounter,isevent,isparameter,dontcounthits,withhash,hitcolor,localeventtime,age,sex,income,interests,robotness,remoteip,windowname,openername,historylength,browserlanguage,browsercountry,socialnetwork,socialaction,httperror,sendtiming,dnstiming,connecttiming,responsestarttiming,responseendtiming,fetchtiming,socialsourcenetworkid,socialsourcepage,paramprice,paramorderid,paramcurrency,paramcurrencyid,openstatservicename,openstatcampaignid,openstatadid,openstatsourceid,utmsource,utmmedium,utmcampaign,utmcontent,utmterm,fromtag,hasgclid,refererhash,urlhash,clid""",  // case 26
                   """watchid,javaenable,title,goodevent,eventtime,eventdate,counterid,clientip,regionid,userid,counterclass,os,useragent,url,referer,isrefresh,referercategoryid,refererregionid,urlcategoryid,urlregionid,resolutionwidth,resolutionheight,resolutiondepth,flashmajor,flashminor,flashminor2,netmajor,netminor,useragentmajor,useragentminor,cookieenable,javascriptenable,ismobile,mobilephone,mobilephonemodel,params,ipnetworkid,traficsourceid,searchengineid,searchphrase,advengineid,isartifical,windowclientwidth,windowclientheight,clienttimezone,clienteventtime,silverlightversion1,silverlightversion2,silverlightversion3,silverlightversion4,pagecharset,codeversion,islink,isdownload,isnotbounce,funiqid,originalurl,hid,isoldcounter,isevent,isparameter,dontcounthits,withhash,hitcolor,localeventtime,age,sex,income,interests,robotness,remoteip,windowname,openername,historylength,browserlanguage,browsercountry,socialnetwork,socialaction,httperror,sendtiming,dnstiming,connecttiming,responsestarttiming,responseendtiming,fetchtiming,socialsourcenetworkid,socialsourcepage,paramprice,paramorderid,paramcurrency,paramcurrencyid,openstatservicename,openstatcampaignid,openstatadid,openstatsourceid,utmsource,utmmedium,utmcampaign,utmcontent,utmterm,fromtag,hasgclid,refererhash,urlhash,clid""",  // case 27
                   """watchid,javaenable,title,goodevent,eventtime,eventdate,counterid,clientip,regionid,userid,counterclass,os,useragent,url,referer,isrefresh,referercategoryid,refererregionid,urlcategoryid,urlregionid,resolutionwidth,resolutionheight,resolutiondepth,flashmajor,flashminor,flashminor2,netmajor,netminor,useragentmajor,useragentminor,cookieenable,javascriptenable,ismobile,mobilephone,mobilephonemodel,params,ipnetworkid,traficsourceid,searchengineid,searchphrase,advengineid,isartifical,windowclientwidth,windowclientheight,clienttimezone,clienteventtime,silverlightversion1,silverlightversion2,silverlightversion3,silverlightversion4,pagecharset,codeversion,islink,isdownload,isnotbounce,funiqid,originalurl,hid,isoldcounter,isevent,isparameter,dontcounthits,withhash,hitcolor,localeventtime,age,sex,income,interests,robotness,remoteip,windowname,openername,historylength,browserlanguage,browsercountry,socialnetwork,socialaction,httperror,sendtiming,dnstiming,connecttiming,responsestarttiming,responseendtiming,fetchtiming,socialsourcenetworkid,socialsourcepage,paramprice,paramorderid,paramcurrency,paramcurrencyid,openstatservicename,openstatcampaignid,openstatadid,openstatsourceid,utmsource,utmmedium,utmcampaign,utmcontent,utmterm,fromtag,hasgclid,refererhash,urlhash,clid""",  // case 28
                   """watchid,javaenable,title,goodevent,eventtime,eventdate,counterid,clientip,regionid,userid,counterclass,os,useragent,url,referer,isrefresh,referercategoryid,refererregionid,urlcategoryid,urlregionid,resolutionwidth,resolutionheight,resolutiondepth,flashmajor,flashminor,flashminor2,netmajor,netminor,useragentmajor,useragentminor,cookieenable,javascriptenable,ismobile,mobilephone,mobilephonemodel,params,ipnetworkid,traficsourceid,searchengineid,searchphrase,advengineid,isartifical,windowclientwidth,windowclientheight,clienttimezone,clienteventtime,silverlightversion1,silverlightversion2,silverlightversion3,silverlightversion4,pagecharset,codeversion,islink,isdownload,isnotbounce,funiqid,originalurl,hid,isoldcounter,isevent,isparameter,dontcounthits,withhash,hitcolor,localeventtime,age,sex,income,interests,robotness,remoteip,windowname,openername,historylength,browserlanguage,browsercountry,socialnetwork,socialaction,httperror,sendtiming,dnstiming,connecttiming,responsestarttiming,responseendtiming,fetchtiming,socialsourcenetworkid,socialsourcepage,paramprice,paramorderid,paramcurrency,paramcurrencyid,openstatservicename,openstatcampaignid,openstatadid,openstatsourceid,utmsource,utmmedium,utmcampaign,utmcontent,utmterm,fromtag,hasgclid,refererhash,urlhash,clid""",  // case 29
                   """""" // case 30
                   ]
    def column_in_paths = ["",   // case 0
                           "",   // case 1
                           "",   // case 2
                           "",   // case 3  
                           "",   // case 4
                           "",   // case 5
                           "",   // case 6
                           "",   // case 7
                           "",   // case 8
                           "",   // case 9
                           "",   // case 10
                           "",   // case 11
                           "COLUMNS FROM PATH AS (city)",   // case 12
                           "",   // case 13
                           "",   // case 14
                           "",   // case 15
                           "",   // case 16
                           "",   // case 17
                           "",   // case 18
                           "",   // case 19
                           "",   // case 20
                           "",   // case 21
                           "",   // case 22
                           "",   // case 23
                           "",   // case 24
                           "",   // case 25
                           "",   // case 26
                           "",   // case 27
                           "",   // case 28
                           "",   // case 29
                           ""   // case 30
                          ]
    def preceding_filters = ["",   // case 0
                            "",   // case 1
                            "",   // case 2
                            "",   // case 3
                            "",   // case 4
                            "",   // case 5
                            "",   // case 6
                            "",   // case 7
                            "",   // case 8
                            "",   // case 9
                            "",   // case 10
                            "preceding filter p_size < 10",   // case 11
                            "",   // case 12
                            "",   // case 13
                            "",   // case 14
                            "",   // case 15
                            "",   // case 16
                            "",   // case 17
                            "",   // case 18
                            "",   // case 19
                            "",   // case 20
                            "",   // case 21
                            "",   // case 22
                            "",   // case 23
                            "",   // case 24
                            "",   // case 25
                            "",   // case 26
                            "",   // case 27
                            "",   // case 28
                            "",   // case 29
                            ""    // case 30
                           ]
    def set_values = ["",     // case 0
                      "",     // case 1
                      "SET(comment=p_comment, retailprice=p_retailprice, container=p_container, size=p_size, type=p_type, brand=p_brand, mfgr=p_mfgr, name=p_name, partkey=p_partkey)", // case 2
                      "set(p_name=upper(p_name),p_greatest=greatest(cast(p_partkey as int), cast(p_size as int)))", // case 3
                      "set(p_partkey = p_partkey + 100)", // case 4
                      "set(partkey = p_partkey + 100)", // case 5
                      "set(partkey = p_partkey + p_size)", // case 6
                      "set(tmpk = p_partkey + 1, partkey = tmpk*2)", // case 7
                      "set(partkey = p_partkey + 1, partsize = p_size*2)", // case 8
                      "set(partsize = p_partkey + p_size)", // case 9
                      "", // case 10
                      "", // case 11
                      "", // case 12
                      "", // case 13
                      "", // case 14
                      "", // case 15
                      "", // case 16
                      "set(col4 = x1)", // case 17
                      "set(col4 = p_brand)", // case 18
                      "set(col5 = p_brand)", // case 19
                      "", // case 20
                      "", // case 21
                      "", // case 22
                      "", // case 23
                      "", // case 24
                      "", // case 25
                      "", // case 26
                      "", // case 27
                      "", // case 28
                      "", // case 29
                      ""  // case 30
    ]
    def where_exprs = ["",   // case 0
                       "",   // case 1
                       "",   // case 2
                       "",   // case 3
                       "",   // case 4
                       "",   // case 5
                       "",   // case 6
                       "",   // case 7
                       "",   // case 8
                       "",   // case 9
                       "",   // case 10
                       "where p_partkey>10",   // case 11
                       "",   // case 12
                       "",   // case 13
                       "",   // case 14
                       "",   // case 15
                       "",   // case 16
                       "",   // case 17
                       "",   // case 18
                       "",   // case 19
                       "",   // case 20
                       "",   // case 21
                       "",   // case 22
                       "",   // case 23
                       "",   // case 24
                       "",   // case 25
                       "",   // case 26
                       "",   // case 27
                       "",   // case 28
                       "",   // case 29
                       ""    // case 30
                      ]

    def line_delimiters = ["",   // case 0
                           "",   // case 1
                           "",   // case 2
                           "",   // case 3
                           "",   // case 4
                           "",   // case 5
                           "",   // case 6
                           "",   // case 7
                           "",   // case 8
                           "",   // case 9
                           "",   // case 10
                           "",   // case 11
                           "",   // case 12
                           "",   // case 13
                           "",   // case 14
                           "",   // case 15
                           "",   // case 16
                           "",   // case 17
                           "",   // case 18
                           "",   // case 19
                           "",   // case 20
                           "",   // case 21
                           "",   // case 22
                           "",   // case 23
                           "",   // case 24
                           "",   // case 25
                           "",   // case 26
                           "",   // case 27
                           "",   // case 28
                           "",   // case 29
                           "\u0007"   // case 30
                          ]

    def etl_info = ["unselected.rows=0; dpp.abnorm.ALL=0; dpp.norm.ALL=200000",  // case 0
                    "unselected.rows=0; dpp.abnorm.ALL=0; dpp.norm.ALL=200000",  // case 1
                    "unselected.rows=0; dpp.abnorm.ALL=0; dpp.norm.ALL=200000",  // case 2
                    "unselected.rows=0; dpp.abnorm.ALL=0; dpp.norm.ALL=200000",  // case 3
                    "unselected.rows=0; dpp.abnorm.ALL=0; dpp.norm.ALL=200000",  // case 4
                    "unselected.rows=0; dpp.abnorm.ALL=0; dpp.norm.ALL=200000",  // case 5
                    "unselected.rows=0; dpp.abnorm.ALL=0; dpp.norm.ALL=200000",  // case 6
                    "unselected.rows=0; dpp.abnorm.ALL=0; dpp.norm.ALL=200000",  // case 7
                    "unselected.rows=0; dpp.abnorm.ALL=0; dpp.norm.ALL=200000",  // case 8
                    "unselected.rows=0; dpp.abnorm.ALL=0; dpp.norm.ALL=200000",  // case 9
                    "unselected.rows=0; dpp.abnorm.ALL=0; dpp.norm.ALL=200000",  // case 10
                    "unselected.rows=163706; dpp.abnorm.ALL=0; dpp.norm.ALL=36294",  // case 11
                    "unselected.rows=0; dpp.abnorm.ALL=0; dpp.norm.ALL=200000",  // case 12
                    "unselected.rows=0; dpp.abnorm.ALL=0; dpp.norm.ALL=200000",  // case 13
                    "\\N",  // case 14
                    "unselected.rows=0; dpp.abnorm.ALL=0; dpp.norm.ALL=200000",  // case 15
                    "unselected.rows=0; dpp.abnorm.ALL=0; dpp.norm.ALL=200000",  // case 16
                    "\\N",  // case 17
                    "unselected.rows=0; dpp.abnorm.ALL=0; dpp.norm.ALL=200000",  // case 18
                    "unselected.rows=0; dpp.abnorm.ALL=0; dpp.norm.ALL=200000",  // case 19
                    "unselected.rows=0; dpp.abnorm.ALL=0; dpp.norm.ALL=200000",  // case 20
                    "unselected.rows=0; dpp.abnorm.ALL=0; dpp.norm.ALL=100000",  // case 21
                    "unselected.rows=0; dpp.abnorm.ALL=0; dpp.norm.ALL=10000",   // case 22
                    "unselected.rows=0; dpp.abnorm.ALL=0; dpp.norm.ALL=10000",   // case 23
                    "unselected.rows=0; dpp.abnorm.ALL=0; dpp.norm.ALL=10000",   // case 24
                    "unselected.rows=0; dpp.abnorm.ALL=0; dpp.norm.ALL=10000",   // case 25
                    "unselected.rows=0; dpp.abnorm.ALL=0; dpp.norm.ALL=10000",   // case 26
                    "unselected.rows=0; dpp.abnorm.ALL=0; dpp.norm.ALL=10000",   // case 27
                    "unselected.rows=0; dpp.abnorm.ALL=0; dpp.norm.ALL=10000",   // case 28
                    "unselected.rows=0; dpp.abnorm.ALL=0; dpp.norm.ALL=10000",   // case 29
                    "unselected.rows=0; dpp.abnorm.ALL=0; dpp.norm.ALL=10000"    // case 30
                    ]

    def task_info = ["cluster:${s3Endpoint}; timeout(s):14400; max_filter_ratio:0.0",  // case 0
                     "cluster:${s3Endpoint}; timeout(s):14400; max_filter_ratio:0.0",  // case 1
                     "cluster:${s3Endpoint}; timeout(s):14400; max_filter_ratio:0.0",  // case 2
                     "cluster:${s3Endpoint}; timeout(s):14400; max_filter_ratio:0.0",  // case 3
                     "cluster:${s3Endpoint}; timeout(s):14400; max_filter_ratio:0.0",  // case 4
                     "cluster:${s3Endpoint}; timeout(s):14400; max_filter_ratio:0.0",  // case 5
                     "cluster:${s3Endpoint}; timeout(s):14400; max_filter_ratio:0.0",  // case 6
                     "cluster:${s3Endpoint}; timeout(s):14400; max_filter_ratio:0.0",  // case 7
                     "cluster:${s3Endpoint}; timeout(s):14400; max_filter_ratio:0.0",  // case 8
                     "cluster:${s3Endpoint}; timeout(s):14400; max_filter_ratio:0.0",  // case 9
                     "cluster:${s3Endpoint}; timeout(s):14400; max_filter_ratio:0.0",  // case 10
                     "cluster:${s3Endpoint}; timeout(s):14400; max_filter_ratio:0.0",  // case 11
                     "cluster:${s3Endpoint}; timeout(s):14400; max_filter_ratio:0.0",  // case 12
                     "cluster:${s3Endpoint}; timeout(s):14400; max_filter_ratio:0.0",  // case 13
                     "cluster:${s3Endpoint}; timeout(s):14400; max_filter_ratio:0.0",  // case 14
                     "cluster:${s3Endpoint}; timeout(s):14400; max_filter_ratio:0.0",  // case 15
                     "cluster:${s3Endpoint}; timeout(s):14400; max_filter_ratio:0.0",  // case 16
                     "cluster:${s3Endpoint}; timeout(s):14400; max_filter_ratio:0.0",  // case 17
                     "cluster:${s3Endpoint}; timeout(s):14400; max_filter_ratio:0.0",  // case 18
                     "cluster:${s3Endpoint}; timeout(s):14400; max_filter_ratio:0.0",  // case 19
                     "cluster:${s3Endpoint}; timeout(s):14400; max_filter_ratio:0.0",  // case 20
                     "cluster:${s3Endpoint}; timeout(s):14400; max_filter_ratio:0.0",  // case 21
                     "cluster:${s3Endpoint}; timeout(s):14400; max_filter_ratio:0.0",  // case 22
                     "cluster:${s3Endpoint}; timeout(s):14400; max_filter_ratio:0.0",  // case 23
                     "cluster:${s3Endpoint}; timeout(s):14400; max_filter_ratio:0.0",  // case 24
                     "cluster:${s3Endpoint}; timeout(s):14400; max_filter_ratio:0.0",  // case 25
                     "cluster:${s3Endpoint}; timeout(s):14400; max_filter_ratio:0.0",  // case 26
                     "cluster:${s3Endpoint}; timeout(s):14400; max_filter_ratio:0.0",  // case 27
                     "cluster:${s3Endpoint}; timeout(s):14400; max_filter_ratio:0.0",  // case 28
                     "cluster:${s3Endpoint}; timeout(s):14400; max_filter_ratio:0.0",  // case 29
                     "cluster:${s3Endpoint}; timeout(s):14400; max_filter_ratio:0.0"   // case 30
    ]

    def error_msg = ["",                                                         // case 0
                    "",                                                          // case 1
                    "",                                                          // case 2
                    "",                                                          // case 3
                    "",                                                          // case 4
                    "",                                                          // case 5
                    "",                                                          // case 6
                    "",                                                          // case 7
                    "",                                                          // case 8
                    "",                                                          // case 9
                    "",                                                          // case 10
                    "",                                                          // case 11
                    "",                                                          // case 12
                    "",                                                          // case 13
                    "failed to find default value expr for slot: x1",            // case 14
                    "",                                                          // case 15
                    "failed to find default value expr for slot: x1",            // case 16
                    "",                                                          // case 17
                    "",                                                          // case 18
                    "",                                                          // case 19
                    "",                                                          // case 20
                    "",                                                          // case 21
                    "",                                                          // case 22
                    "",                                                          // case 23
                    "",                                                          // case 24
                    "",                                                          // case 25
                    "",                                                          // case 26
                    "",                                                          // case 27
                    "",                                                          // case 28
                    "",                                                          // case 29
                    "",                                                          // case 30
                    ]

    String ak = getS3AK()
    String sk = getS3SK()
    String enabled = context.config.otherConfigs.get("enableBrokerLoad")

    def do_load_job = {label, path, table, columns, column_in_path, preceding_filter,
                          set_value, where_expr, line_delimiter ->
        String columns_str = ("$columns" != "") ? "($columns)" : "";
        String line_term = ("$line_delimiter" != "") ? "lines terminated by '$line_delimiter'" : "";
        String format_str
        if (table.startsWith("orc_s3_case")) {
            format_str = "ORC"
        } else if (table.startsWith("csv")) {
            format_str = "CSV"
        } else {
            format_str = "PARQUET"
        }
        def sql_str = """
            LOAD LABEL $label (
                DATA INFILE("$path")
                INTO TABLE $table
                $line_term
                FORMAT AS $format_str
                $columns_str
                $column_in_path
                $preceding_filter
                $set_value
                $where_expr
            )
            WITH S3 (
                "AWS_ACCESS_KEY" = "$ak",
                "AWS_SECRET_KEY" = "$sk",
                "AWS_ENDPOINT" = "${s3Endpoint}",
                "AWS_REGION" = "${s3Region}",
                "provider" = "${getS3Provider()}"
            )
            """
        logger.info("submit sql: ${sql_str}");
        sql """${sql_str}"""
        logger.info("Submit load with lable: $label, table: $table, path: $path")
    }

    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        def uuids = []
        try {
            def i = 0
            for (String table in tables) {
                sql new File("""${context.file.parent}/ddl/${table}_drop.sql""").text
                sql new File("""${context.file.parent}/ddl/${table}_create.sql""").text

                def uuid = UUID.randomUUID().toString().replace("-", "0")
                def the_label = "L${i}_${uuid}"
                uuids.add(the_label)
                do_load_job.call(the_label, paths[i], table, columns_list[i], column_in_paths[i], preceding_filters[i],
                        set_values[i], where_exprs[i], line_delimiters[i])
                i++
            }

            i = 0
            for (String label in uuids) {
                def max_try_milli_secs = 600000
                while (max_try_milli_secs > 0) {
                    def String[][] result = sql """ show load where label="$label" order by createtime desc limit 1; """
                    def logStr = "";
                    result.each { row ->
                        row.each {
                            element -> logStr += element + ", "
                        }
                        logStr += "\n"
                    }
                    logger.info("Load status: " + logStr + ", label: $label")
                    if (result[0][2].equals("FINISHED")) {
                        logger.info("Load FINISHED " + label)
                        assertTrue(result[0][6].contains(task_info[i]))
                        load_counters = etl_info[i].split('; ');
                        for (String counter : load_counters) {
                            assertTrue(result[0][5].contains(counter), "expected: " + counter + ", actual: " + result[0][5] + ", label: $label")
                        }
                        break;
                    }
                    if (result[0][2].equals("CANCELLED")) {
                        logger.info("Load result: " + result[0])
                        assertTrue(result[0][6].contains(task_info[i]))
                        assertTrue(result[0][7].contains(error_msg[i]), "expected: " + error_msg[i] + ", actual: " + result[0][7] + ", label: $label")
                        break;
                    }
                    Thread.sleep(1000)
                    max_try_milli_secs -= 1000
                    if(max_try_milli_secs <= 0) {
                        assertTrue(1 == 2, "load Timeout: $label")
                    }
                }
                i++
            }

            def orc_expect_result = """[[20, 15901, 6025915247311731176, 1373910657, 8863282788606566657], [38, 15901, -9154375582268094750, 1373853561, 4923892366467329038], [38, 15901, -9154375582268094750, 1373853561, 8447995939656287502], [38, 15901, -9154375582268094750, 1373853565, 7451966001310881759], [38, 15901, -9154375582268094750, 1373853565, 7746521994248163870], [38, 15901, -9154375582268094750, 1373853577, 6795654975682437824], [38, 15901, -9154375582268094750, 1373853577, 9009208035649338594], [38, 15901, -9154375582268094750, 1373853608, 6374361939566017108], [38, 15901, -9154375582268094750, 1373853608, 7387298457456465364], [38, 15901, -9154375582268094750, 1373853616, 7463736180224933002]]"""
            for (String table in tables) {
                if (table.matches("orc_s3_case[23456789]")) {
                    def String[][] orc_actual_result = sql """select CounterID, EventDate, UserID, EventTime, WatchID from $table order by CounterID, EventDate, UserID, EventTime, WatchID limit 10;"""
                    assertTrue("$orc_actual_result" == "$orc_expect_result")
                }
            }

            order_qt_parquet_s3_case1 """select count(*) from parquet_s3_case1 where col1=10"""
            order_qt_parquet_s3_case3 """select count(*) from parquet_s3_case3 where p_partkey < 100000"""
            order_qt_parquet_s3_case6 """select count(*) from parquet_s3_case6 where p_partkey < 100000"""
            order_qt_parquet_s3_case7 """select count(*) from parquet_s3_case7 where col4=4"""
            order_qt_parquet_s3_case8 """ select count(*) from parquet_s3_case8 where p_partkey=1"""

            // pr 22666
            def tbl_22666 = "part_22666"
            sql """drop table if exists ${tbl_22666} force"""
            sql """
                CREATE TABLE ${tbl_22666} (
                    p_partkey          int NULL,
                    p_name        VARCHAR(55) NULL,
                    p_mfgr        VARCHAR(25) NULL,
                    p_brand       VARCHAR(10) NULL,
                    p_type        VARCHAR(25) NULL,
                    p_size        int NULL,
                    p_container   VARCHAR(10) NULL,
                    p_retailprice decimal(15, 2) NULL,
                    p_comment     VARCHAR(23) NULL
                )ENGINE=OLAP
                DUPLICATE KEY(`p_partkey`)
                DISTRIBUTED BY HASH(`p_partkey`) BUCKETS 3
                PROPERTIES (
                    "replication_num" = "1"
                );
            """

            def label_22666 = "part_" + UUID.randomUUID().toString().replace("-", "0")
            sql """
                LOAD LABEL ${label_22666} (
                    DATA INFILE("s3://${s3BucketName}/regression/load/data/part0.parquet")
                    INTO TABLE ${tbl_22666}
                    FORMAT AS "PARQUET"
                    (p_partkey, p_name, p_mfgr),
                    DATA INFILE("s3://${s3BucketName}/regression/load/data/part1.parquet")
                    INTO TABLE ${tbl_22666}
                    FORMAT AS "PARQUET"
                    (p_partkey, p_brand, p_type)
                )
                WITH S3 (
                    "AWS_ACCESS_KEY" = "$ak",
                    "AWS_SECRET_KEY" = "$sk",
                    "AWS_ENDPOINT" = "${s3Endpoint}",
                    "AWS_REGION" = "${s3Region}",
                    "provider" = "${getS3Provider()}"
                );
            """

            def max_try_milli_secs = 600000
            while (max_try_milli_secs > 0) {
                def String[][] result = sql """ show load where label="$label_22666" order by createtime desc limit 1; """
                logger.info("Load status: " + result[0][2] + ", label: $label_22666")
                if (result[0][2].equals("FINISHED")) {
                    logger.info("Load FINISHED " + label_22666)
                    break;
                }
                if (result[0][2].equals("CANCELLED")) {
                    assertTrue(false, "load failed: $result")
                    break;
                }
                Thread.sleep(1000)
                max_try_milli_secs -= 1000
                if(max_try_milli_secs <= 0) {
                    assertTrue(1 == 2, "load Timeout: $label_22666")
                }
            }

            order_qt_pr22666_1 """ select count(*) from ${tbl_22666} where p_brand is not null limit 10;"""
            order_qt_pr22666_2 """ select count(*) from ${tbl_22666} where p_name is not null limit 10;"""

        } finally {
            for (String table in tables) {
                sql new File("""${context.file.parent}/ddl/${table}_drop.sql""").text
            }
        }
    }
}

