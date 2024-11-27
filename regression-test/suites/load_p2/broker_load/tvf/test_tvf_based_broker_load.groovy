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

suite("test_tvf_based_broker_load", "p2") {
    def s3BucketName = getS3BucketName()
    def s3Endpoint = getS3Endpoint()
    def s3Region = getS3Region()
    def tables = ["part",
                  "upper_case",
                  "reverse",
                  "set1",
                  "set2",
                  "set3",
                  "set4",
                  "set5",
                  "set6",
                  "set7",
                  "null_default",
                  "filter",
                  "path_column",
                  "parquet_s3_case1", // col1 not in file but in table, will load default value for it.
                  "parquet_s3_case2", // x1 not in file, not in table, will throw "col not found" error.
                  "parquet_s3_case3", // p_comment not in table but in file, load normally.
                  "parquet_s3_case4", // all columns are in table but not in file, will fill default values.
                  "parquet_s3_case5", // x1 not in file, not in table, will throw "col not found" error.
                  "parquet_s3_case6", // normal
                  "parquet_s3_case7", // col5 will be ignored, load normally
                  "parquet_s3_case8", // first column in table is not specified, will load default value for it.
                  // "parquet_s3_case9", // can't support complex type, we just ignore it. first column in table is not specified, will load default value for it.
                  "orc_s3_case1", // table column capitalize firsrt
                  "orc_s3_case2", // table column lowercase * load column lowercase * orc file lowercase
                  "orc_s3_case3", // table column lowercase * load column uppercase * orc file lowercase
                  "orc_s3_case4", // table column lowercase * load column lowercase * orc file uppercase
                  "orc_s3_case5", // table column lowercase * load column uppercase * orc file uppercase
                  "orc_s3_case6", // table column uppercase * load column uppercase * orc file lowercase
                  "orc_s3_case7", // table column uppercase * load column lowercase * orc file lowercase
                  "orc_s3_case8", // table column uppercase * load column uppercase * orc file uppercase
                  "orc_s3_case9", // table column uppercase * load column lowercase * orc file uppercase
                  "csv_s3_case_line_delimiter" // csv format table with special line delimiter
    ]
    def paths = ["s3://${s3BucketName}/regression/load/data/part*",
                 "s3://${s3BucketName}/regression/load/data/part*",
                 "s3://${s3BucketName}/regression/load/data/part*",
                 "s3://${s3BucketName}/regression/load/data/part*",
                 "s3://${s3BucketName}/regression/load/data/part*",
                 "s3://${s3BucketName}/regression/load/data/part*",
                 "s3://${s3BucketName}/regression/load/data/part*",
                 "s3://${s3BucketName}/regression/load/data/part*",
                 "s3://${s3BucketName}/regression/load/data/part*",
                 "s3://${s3BucketName}/regression/load/data/part*",
                 "s3://${s3BucketName}/regression/load/data/part*",
                 "s3://${s3BucketName}/regression/load/data/part*",
                 "s3://${s3BucketName}/regression/load/data/path/*/part*",
                 "s3://${s3BucketName}/regression/load/data/part*",
                 "s3://${s3BucketName}/regression/load/data/part*",
                 "s3://${s3BucketName}/regression/load/data/part*",
                 "s3://${s3BucketName}/regression/load/data/part*",
                 "s3://${s3BucketName}/regression/load/data/part*",
                 "s3://${s3BucketName}/regression/load/data/part*",
                 "s3://${s3BucketName}/regression/load/data/part*",
                 "s3://${s3BucketName}/regression/load/data/part*",
                 // "s3://${s3BucketName}/regression/load/data/random_all_types/part*", // just ignore it, parquet_case9 can't support complex type
                 "s3://${s3BucketName}/regression/load/data/orc/hits_100k_rows.orc",
                 "s3://${s3BucketName}/regression/load/data/orc/hits_10k_rows_lowercase.orc",
                 "s3://${s3BucketName}/regression/load/data/orc/hits_10k_rows_lowercase.orc",
                 "s3://${s3BucketName}/regression/load/data/orc/hits_10k_rows_uppercase.orc",
                 "s3://${s3BucketName}/regression/load/data/orc/hits_10k_rows_uppercase.orc",
                 "s3://${s3BucketName}/regression/load/data/orc/hits_10k_rows_lowercase.orc",
                 "s3://${s3BucketName}/regression/load/data/orc/hits_10k_rows_lowercase.orc",
                 "s3://${s3BucketName}/regression/load/data/orc/hits_10k_rows_uppercase.orc",
                 "s3://${s3BucketName}/regression/load/data/orc/hits_10k_rows_uppercase.orc",
                 "s3://${s3BucketName}/regression/line_delimiter/lineitem_0x7.csv.gz"
    ]
    def columns_list = ["""p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment""",
                        """p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment""",
                        """p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment""",
                        """p_partkey, p_name, p_size""",
                        """p_partkey""",
                        """p_partkey""",
                        """p_partkey,  p_size""",
                        """p_partkey""",
                        """p_partkey,  p_size""",
                        """p_partkey,  p_size""",
                        """p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment""",
                        """p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment""",
                        """p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment""",
                        """p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment, col1""",
                        """p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment, x1""",
                        """p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment""",
                        """col1, col2, col3, col4""",
                        """p_partkey, p_name, p_mfgr, x1""",
                        """p_partkey, p_name, p_mfgr, p_brand""",
                        """p_partkey, p_name, p_mfgr, p_brand""",
                        """p_name, p_mfgr""",
                        // """""",
                        """watchid,javaenable,title,goodevent,eventtime,eventdate,counterid,clientip,regionid,userid,counterclass,os,useragent,url,referer,isrefresh,referercategoryid,refererregionid,urlcategoryid,urlregionid,resolutionwidth,resolutionheight,resolutiondepth,flashmajor,flashminor,flashminor2,netmajor,netminor,useragentmajor,useragentminor,cookieenable,javascriptenable,ismobile,mobilephone,mobilephonemodel,params,ipnetworkid,traficsourceid,searchengineid,searchphrase,advengineid,isartifical,windowclientwidth,windowclientheight,clienttimezone,clienteventtime,silverlightversion1,silverlightversion2,silverlightversion3,silverlightversion4,pagecharset,codeversion,islink,isdownload,isnotbounce,funiqid,originalurl,hid,isoldcounter,isevent,isparameter,dontcounthits,withhash,hitcolor,localeventtime,age,sex,income,interests,robotness,remoteip,windowname,openername,historylength,browserlanguage,browsercountry,socialnetwork,socialaction,httperror,sendtiming,dnstiming,connecttiming,responsestarttiming,responseendtiming,fetchtiming,socialsourcenetworkid,socialsourcepage,paramprice,paramorderid,paramcurrency,paramcurrencyid,openstatservicename,openstatcampaignid,openstatadid,openstatsourceid,utmsource,utmmedium,utmcampaign,utmcontent,utmterm,fromtag,hasgclid,refererhash,urlhash,clid""",
                        //TODO: comment blow 8 rows after jibing fix
                        """watchid,javaenable,title,goodevent,eventtime,eventdate,counterid,clientip,regionid,userid,counterclass,os,useragent,url,referer,isrefresh,referercategoryid,refererregionid,urlcategoryid,urlregionid,resolutionwidth,resolutionheight,resolutiondepth,flashmajor,flashminor,flashminor2,netmajor,netminor,useragentmajor,useragentminor,cookieenable,javascriptenable,ismobile,mobilephone,mobilephonemodel,params,ipnetworkid,traficsourceid,searchengineid,searchphrase,advengineid,isartifical,windowclientwidth,windowclientheight,clienttimezone,clienteventtime,silverlightversion1,silverlightversion2,silverlightversion3,silverlightversion4,pagecharset,codeversion,islink,isdownload,isnotbounce,funiqid,originalurl,hid,isoldcounter,isevent,isparameter,dontcounthits,withhash,hitcolor,localeventtime,age,sex,income,interests,robotness,remoteip,windowname,openername,historylength,browserlanguage,browsercountry,socialnetwork,socialaction,httperror,sendtiming,dnstiming,connecttiming,responsestarttiming,responseendtiming,fetchtiming,socialsourcenetworkid,socialsourcepage,paramprice,paramorderid,paramcurrency,paramcurrencyid,openstatservicename,openstatcampaignid,openstatadid,openstatsourceid,utmsource,utmmedium,utmcampaign,utmcontent,utmterm,fromtag,hasgclid,refererhash,urlhash,clid""",
                        """watchid,javaenable,title,goodevent,eventtime,eventdate,counterid,clientip,regionid,userid,counterclass,os,useragent,url,referer,isrefresh,referercategoryid,refererregionid,urlcategoryid,urlregionid,resolutionwidth,resolutionheight,resolutiondepth,flashmajor,flashminor,flashminor2,netmajor,netminor,useragentmajor,useragentminor,cookieenable,javascriptenable,ismobile,mobilephone,mobilephonemodel,params,ipnetworkid,traficsourceid,searchengineid,searchphrase,advengineid,isartifical,windowclientwidth,windowclientheight,clienttimezone,clienteventtime,silverlightversion1,silverlightversion2,silverlightversion3,silverlightversion4,pagecharset,codeversion,islink,isdownload,isnotbounce,funiqid,originalurl,hid,isoldcounter,isevent,isparameter,dontcounthits,withhash,hitcolor,localeventtime,age,sex,income,interests,robotness,remoteip,windowname,openername,historylength,browserlanguage,browsercountry,socialnetwork,socialaction,httperror,sendtiming,dnstiming,connecttiming,responsestarttiming,responseendtiming,fetchtiming,socialsourcenetworkid,socialsourcepage,paramprice,paramorderid,paramcurrency,paramcurrencyid,openstatservicename,openstatcampaignid,openstatadid,openstatsourceid,utmsource,utmmedium,utmcampaign,utmcontent,utmterm,fromtag,hasgclid,refererhash,urlhash,clid""",
                        """watchid,javaenable,title,goodevent,eventtime,eventdate,counterid,clientip,regionid,userid,counterclass,os,useragent,url,referer,isrefresh,referercategoryid,refererregionid,urlcategoryid,urlregionid,resolutionwidth,resolutionheight,resolutiondepth,flashmajor,flashminor,flashminor2,netmajor,netminor,useragentmajor,useragentminor,cookieenable,javascriptenable,ismobile,mobilephone,mobilephonemodel,params,ipnetworkid,traficsourceid,searchengineid,searchphrase,advengineid,isartifical,windowclientwidth,windowclientheight,clienttimezone,clienteventtime,silverlightversion1,silverlightversion2,silverlightversion3,silverlightversion4,pagecharset,codeversion,islink,isdownload,isnotbounce,funiqid,originalurl,hid,isoldcounter,isevent,isparameter,dontcounthits,withhash,hitcolor,localeventtime,age,sex,income,interests,robotness,remoteip,windowname,openername,historylength,browserlanguage,browsercountry,socialnetwork,socialaction,httperror,sendtiming,dnstiming,connecttiming,responsestarttiming,responseendtiming,fetchtiming,socialsourcenetworkid,socialsourcepage,paramprice,paramorderid,paramcurrency,paramcurrencyid,openstatservicename,openstatcampaignid,openstatadid,openstatsourceid,utmsource,utmmedium,utmcampaign,utmcontent,utmterm,fromtag,hasgclid,refererhash,urlhash,clid""",
                        """watchid,javaenable,title,goodevent,eventtime,eventdate,counterid,clientip,regionid,userid,counterclass,os,useragent,url,referer,isrefresh,referercategoryid,refererregionid,urlcategoryid,urlregionid,resolutionwidth,resolutionheight,resolutiondepth,flashmajor,flashminor,flashminor2,netmajor,netminor,useragentmajor,useragentminor,cookieenable,javascriptenable,ismobile,mobilephone,mobilephonemodel,params,ipnetworkid,traficsourceid,searchengineid,searchphrase,advengineid,isartifical,windowclientwidth,windowclientheight,clienttimezone,clienteventtime,silverlightversion1,silverlightversion2,silverlightversion3,silverlightversion4,pagecharset,codeversion,islink,isdownload,isnotbounce,funiqid,originalurl,hid,isoldcounter,isevent,isparameter,dontcounthits,withhash,hitcolor,localeventtime,age,sex,income,interests,robotness,remoteip,windowname,openername,historylength,browserlanguage,browsercountry,socialnetwork,socialaction,httperror,sendtiming,dnstiming,connecttiming,responsestarttiming,responseendtiming,fetchtiming,socialsourcenetworkid,socialsourcepage,paramprice,paramorderid,paramcurrency,paramcurrencyid,openstatservicename,openstatcampaignid,openstatadid,openstatsourceid,utmsource,utmmedium,utmcampaign,utmcontent,utmterm,fromtag,hasgclid,refererhash,urlhash,clid""",
                        """watchid,javaenable,title,goodevent,eventtime,eventdate,counterid,clientip,regionid,userid,counterclass,os,useragent,url,referer,isrefresh,referercategoryid,refererregionid,urlcategoryid,urlregionid,resolutionwidth,resolutionheight,resolutiondepth,flashmajor,flashminor,flashminor2,netmajor,netminor,useragentmajor,useragentminor,cookieenable,javascriptenable,ismobile,mobilephone,mobilephonemodel,params,ipnetworkid,traficsourceid,searchengineid,searchphrase,advengineid,isartifical,windowclientwidth,windowclientheight,clienttimezone,clienteventtime,silverlightversion1,silverlightversion2,silverlightversion3,silverlightversion4,pagecharset,codeversion,islink,isdownload,isnotbounce,funiqid,originalurl,hid,isoldcounter,isevent,isparameter,dontcounthits,withhash,hitcolor,localeventtime,age,sex,income,interests,robotness,remoteip,windowname,openername,historylength,browserlanguage,browsercountry,socialnetwork,socialaction,httperror,sendtiming,dnstiming,connecttiming,responsestarttiming,responseendtiming,fetchtiming,socialsourcenetworkid,socialsourcepage,paramprice,paramorderid,paramcurrency,paramcurrencyid,openstatservicename,openstatcampaignid,openstatadid,openstatsourceid,utmsource,utmmedium,utmcampaign,utmcontent,utmterm,fromtag,hasgclid,refererhash,urlhash,clid""",
                        """watchid,javaenable,title,goodevent,eventtime,eventdate,counterid,clientip,regionid,userid,counterclass,os,useragent,url,referer,isrefresh,referercategoryid,refererregionid,urlcategoryid,urlregionid,resolutionwidth,resolutionheight,resolutiondepth,flashmajor,flashminor,flashminor2,netmajor,netminor,useragentmajor,useragentminor,cookieenable,javascriptenable,ismobile,mobilephone,mobilephonemodel,params,ipnetworkid,traficsourceid,searchengineid,searchphrase,advengineid,isartifical,windowclientwidth,windowclientheight,clienttimezone,clienteventtime,silverlightversion1,silverlightversion2,silverlightversion3,silverlightversion4,pagecharset,codeversion,islink,isdownload,isnotbounce,funiqid,originalurl,hid,isoldcounter,isevent,isparameter,dontcounthits,withhash,hitcolor,localeventtime,age,sex,income,interests,robotness,remoteip,windowname,openername,historylength,browserlanguage,browsercountry,socialnetwork,socialaction,httperror,sendtiming,dnstiming,connecttiming,responsestarttiming,responseendtiming,fetchtiming,socialsourcenetworkid,socialsourcepage,paramprice,paramorderid,paramcurrency,paramcurrencyid,openstatservicename,openstatcampaignid,openstatadid,openstatsourceid,utmsource,utmmedium,utmcampaign,utmcontent,utmterm,fromtag,hasgclid,refererhash,urlhash,clid""",
                        """watchid,javaenable,title,goodevent,eventtime,eventdate,counterid,clientip,regionid,userid,counterclass,os,useragent,url,referer,isrefresh,referercategoryid,refererregionid,urlcategoryid,urlregionid,resolutionwidth,resolutionheight,resolutiondepth,flashmajor,flashminor,flashminor2,netmajor,netminor,useragentmajor,useragentminor,cookieenable,javascriptenable,ismobile,mobilephone,mobilephonemodel,params,ipnetworkid,traficsourceid,searchengineid,searchphrase,advengineid,isartifical,windowclientwidth,windowclientheight,clienttimezone,clienteventtime,silverlightversion1,silverlightversion2,silverlightversion3,silverlightversion4,pagecharset,codeversion,islink,isdownload,isnotbounce,funiqid,originalurl,hid,isoldcounter,isevent,isparameter,dontcounthits,withhash,hitcolor,localeventtime,age,sex,income,interests,robotness,remoteip,windowname,openername,historylength,browserlanguage,browsercountry,socialnetwork,socialaction,httperror,sendtiming,dnstiming,connecttiming,responsestarttiming,responseendtiming,fetchtiming,socialsourcenetworkid,socialsourcepage,paramprice,paramorderid,paramcurrency,paramcurrencyid,openstatservicename,openstatcampaignid,openstatadid,openstatsourceid,utmsource,utmmedium,utmcampaign,utmcontent,utmterm,fromtag,hasgclid,refererhash,urlhash,clid""",
                        """watchid,javaenable,title,goodevent,eventtime,eventdate,counterid,clientip,regionid,userid,counterclass,os,useragent,url,referer,isrefresh,referercategoryid,refererregionid,urlcategoryid,urlregionid,resolutionwidth,resolutionheight,resolutiondepth,flashmajor,flashminor,flashminor2,netmajor,netminor,useragentmajor,useragentminor,cookieenable,javascriptenable,ismobile,mobilephone,mobilephonemodel,params,ipnetworkid,traficsourceid,searchengineid,searchphrase,advengineid,isartifical,windowclientwidth,windowclientheight,clienttimezone,clienteventtime,silverlightversion1,silverlightversion2,silverlightversion3,silverlightversion4,pagecharset,codeversion,islink,isdownload,isnotbounce,funiqid,originalurl,hid,isoldcounter,isevent,isparameter,dontcounthits,withhash,hitcolor,localeventtime,age,sex,income,interests,robotness,remoteip,windowname,openername,historylength,browserlanguage,browsercountry,socialnetwork,socialaction,httperror,sendtiming,dnstiming,connecttiming,responsestarttiming,responseendtiming,fetchtiming,socialsourcenetworkid,socialsourcepage,paramprice,paramorderid,paramcurrency,paramcurrencyid,openstatservicename,openstatcampaignid,openstatadid,openstatsourceid,utmsource,utmmedium,utmcampaign,utmcontent,utmterm,fromtag,hasgclid,refererhash,urlhash,clid""",
                        """"""
                        //TODO: uncomment blow 8 rows after jibing fix
                        //    """watchid,javaenable,title,goodevent,eventtime,eventdate,counterid,clientip,regionid,userid,counterclass,os,useragent,url,referer,isrefresh,referercategoryid,refererregionid,urlcategoryid,urlregionid,resolutionwidth,resolutionheight,resolutiondepth,flashmajor,flashminor,flashminor2,netmajor,netminor,useragentmajor,useragentminor,cookieenable,javascriptenable,ismobile,mobilephone,mobilephonemodel,params,ipnetworkid,traficsourceid,searchengineid,searchphrase,advengineid,isartifical,windowclientwidth,windowclientheight,clienttimezone,clienteventtime,silverlightversion1,silverlightversion2,silverlightversion3,silverlightversion4,pagecharset,codeversion,islink,isdownload,isnotbounce,funiqid,originalurl,hid,isoldcounter,isevent,isparameter,dontcounthits,withhash,hitcolor,localeventtime,age,sex,income,interests,robotness,remoteip,windowname,openername,historylength,browserlanguage,browsercountry,socialnetwork,socialaction,httperror,sendtiming,dnstiming,connecttiming,responsestarttiming,responseendtiming,fetchtiming,socialsourcenetworkid,socialsourcepage,paramprice,paramorderid,paramcurrency,paramcurrencyid,openstatservicename,openstatcampaignid,openstatadid,openstatsourceid,utmsource,utmmedium,utmcampaign,utmcontent,utmterm,fromtag,hasgclid,refererhash,urlhash,clid""",
                        //    """WATCHID,JAVAENABLE,TITLE,GOODEVENT,EVENTTIME,EVENTDATE,COUNTERID,CLIENTIP,REGIONID,USERID,COUNTERCLASS,OS,USERAGENT,URL,REFERER,ISREFRESH,REFERERCATEGORYID,REFERERREGIONID,URLCATEGORYID,URLREGIONID,RESOLUTIONWIDTH,RESOLUTIONHEIGHT,RESOLUTIONDEPTH,FLASHMAJOR,FLASHMINOR,FLASHMINOR2,NETMAJOR,NETMINOR,USERAGENTMAJOR,USERAGENTMINOR,COOKIEENABLE,JAVASCRIPTENABLE,ISMOBILE,MOBILEPHONE,MOBILEPHONEMODEL,PARAMS,IPNETWORKID,TRAFICSOURCEID,SEARCHENGINEID,SEARCHPHRASE,ADVENGINEID,ISARTIFICAL,WINDOWCLIENTWIDTH,WINDOWCLIENTHEIGHT,CLIENTTIMEZONE,CLIENTEVENTTIME,SILVERLIGHTVERSION1,SILVERLIGHTVERSION2,SILVERLIGHTVERSION3,SILVERLIGHTVERSION4,PAGECHARSET,CODEVERSION,ISLINK,ISDOWNLOAD,ISNOTBOUNCE,FUNIQID,ORIGINALURL,HID,ISOLDCOUNTER,ISEVENT,ISPARAMETER,DONTCOUNTHITS,WITHHASH,HITCOLOR,LOCALEVENTTIME,AGE,SEX,INCOME,INTERESTS,ROBOTNESS,REMOTEIP,WINDOWNAME,OPENERNAME,HISTORYLENGTH,BROWSERLANGUAGE,BROWSERCOUNTRY,SOCIALNETWORK,SOCIALACTION,HTTPERROR,SENDTIMING,DNSTIMING,CONNECTTIMING,RESPONSESTARTTIMING,RESPONSEENDTIMING,FETCHTIMING,SOCIALSOURCENETWORKID,SOCIALSOURCEPAGE,PARAMPRICE,PARAMORDERID,PARAMCURRENCY,PARAMCURRENCYID,OPENSTATSERVICENAME,OPENSTATCAMPAIGNID,OPENSTATADID,OPENSTATSOURCEID,UTMSOURCE,UTMMEDIUM,UTMCAMPAIGN,UTMCONTENT,UTMTERM,FROMTAG,HASGCLID,REFERERHASH,URLHASH,CLID""",
                        //    """watchid,javaenable,title,goodevent,eventtime,eventdate,counterid,clientip,regionid,userid,counterclass,os,useragent,url,referer,isrefresh,referercategoryid,refererregionid,urlcategoryid,urlregionid,resolutionwidth,resolutionheight,resolutiondepth,flashmajor,flashminor,flashminor2,netmajor,netminor,useragentmajor,useragentminor,cookieenable,javascriptenable,ismobile,mobilephone,mobilephonemodel,params,ipnetworkid,traficsourceid,searchengineid,searchphrase,advengineid,isartifical,windowclientwidth,windowclientheight,clienttimezone,clienteventtime,silverlightversion1,silverlightversion2,silverlightversion3,silverlightversion4,pagecharset,codeversion,islink,isdownload,isnotbounce,funiqid,originalurl,hid,isoldcounter,isevent,isparameter,dontcounthits,withhash,hitcolor,localeventtime,age,sex,income,interests,robotness,remoteip,windowname,openername,historylength,browserlanguage,browsercountry,socialnetwork,socialaction,httperror,sendtiming,dnstiming,connecttiming,responsestarttiming,responseendtiming,fetchtiming,socialsourcenetworkid,socialsourcepage,paramprice,paramorderid,paramcurrency,paramcurrencyid,openstatservicename,openstatcampaignid,openstatadid,openstatsourceid,utmsource,utmmedium,utmcampaign,utmcontent,utmterm,fromtag,hasgclid,refererhash,urlhash,clid""",
                        //    """WATCHID,JAVAENABLE,TITLE,GOODEVENT,EVENTTIME,EVENTDATE,COUNTERID,CLIENTIP,REGIONID,USERID,COUNTERCLASS,OS,USERAGENT,URL,REFERER,ISREFRESH,REFERERCATEGORYID,REFERERREGIONID,URLCATEGORYID,URLREGIONID,RESOLUTIONWIDTH,RESOLUTIONHEIGHT,RESOLUTIONDEPTH,FLASHMAJOR,FLASHMINOR,FLASHMINOR2,NETMAJOR,NETMINOR,USERAGENTMAJOR,USERAGENTMINOR,COOKIEENABLE,JAVASCRIPTENABLE,ISMOBILE,MOBILEPHONE,MOBILEPHONEMODEL,PARAMS,IPNETWORKID,TRAFICSOURCEID,SEARCHENGINEID,SEARCHPHRASE,ADVENGINEID,ISARTIFICAL,WINDOWCLIENTWIDTH,WINDOWCLIENTHEIGHT,CLIENTTIMEZONE,CLIENTEVENTTIME,SILVERLIGHTVERSION1,SILVERLIGHTVERSION2,SILVERLIGHTVERSION3,SILVERLIGHTVERSION4,PAGECHARSET,CODEVERSION,ISLINK,ISDOWNLOAD,ISNOTBOUNCE,FUNIQID,ORIGINALURL,HID,ISOLDCOUNTER,ISEVENT,ISPARAMETER,DONTCOUNTHITS,WITHHASH,HITCOLOR,LOCALEVENTTIME,AGE,SEX,INCOME,INTERESTS,ROBOTNESS,REMOTEIP,WINDOWNAME,OPENERNAME,HISTORYLENGTH,BROWSERLANGUAGE,BROWSERCOUNTRY,SOCIALNETWORK,SOCIALACTION,HTTPERROR,SENDTIMING,DNSTIMING,CONNECTTIMING,RESPONSESTARTTIMING,RESPONSEENDTIMING,FETCHTIMING,SOCIALSOURCENETWORKID,SOCIALSOURCEPAGE,PARAMPRICE,PARAMORDERID,PARAMCURRENCY,PARAMCURRENCYID,OPENSTATSERVICENAME,OPENSTATCAMPAIGNID,OPENSTATADID,OPENSTATSOURCEID,UTMSOURCE,UTMMEDIUM,UTMCAMPAIGN,UTMCONTENT,UTMTERM,FROMTAG,HASGCLID,REFERERHASH,URLHASH,CLID""",
                        //    """WATCHID,JAVAENABLE,TITLE,GOODEVENT,EVENTTIME,EVENTDATE,COUNTERID,CLIENTIP,REGIONID,USERID,COUNTERCLASS,OS,USERAGENT,URL,REFERER,ISREFRESH,REFERERCATEGORYID,REFERERREGIONID,URLCATEGORYID,URLREGIONID,RESOLUTIONWIDTH,RESOLUTIONHEIGHT,RESOLUTIONDEPTH,FLASHMAJOR,FLASHMINOR,FLASHMINOR2,NETMAJOR,NETMINOR,USERAGENTMAJOR,USERAGENTMINOR,COOKIEENABLE,JAVASCRIPTENABLE,ISMOBILE,MOBILEPHONE,MOBILEPHONEMODEL,PARAMS,IPNETWORKID,TRAFICSOURCEID,SEARCHENGINEID,SEARCHPHRASE,ADVENGINEID,ISARTIFICAL,WINDOWCLIENTWIDTH,WINDOWCLIENTHEIGHT,CLIENTTIMEZONE,CLIENTEVENTTIME,SILVERLIGHTVERSION1,SILVERLIGHTVERSION2,SILVERLIGHTVERSION3,SILVERLIGHTVERSION4,PAGECHARSET,CODEVERSION,ISLINK,ISDOWNLOAD,ISNOTBOUNCE,FUNIQID,ORIGINALURL,HID,ISOLDCOUNTER,ISEVENT,ISPARAMETER,DONTCOUNTHITS,WITHHASH,HITCOLOR,LOCALEVENTTIME,AGE,SEX,INCOME,INTERESTS,ROBOTNESS,REMOTEIP,WINDOWNAME,OPENERNAME,HISTORYLENGTH,BROWSERLANGUAGE,BROWSERCOUNTRY,SOCIALNETWORK,SOCIALACTION,HTTPERROR,SENDTIMING,DNSTIMING,CONNECTTIMING,RESPONSESTARTTIMING,RESPONSEENDTIMING,FETCHTIMING,SOCIALSOURCENETWORKID,SOCIALSOURCEPAGE,PARAMPRICE,PARAMORDERID,PARAMCURRENCY,PARAMCURRENCYID,OPENSTATSERVICENAME,OPENSTATCAMPAIGNID,OPENSTATADID,OPENSTATSOURCEID,UTMSOURCE,UTMMEDIUM,UTMCAMPAIGN,UTMCONTENT,UTMTERM,FROMTAG,HASGCLID,REFERERHASH,URLHASH,CLID""",
                        //    """watchid,javaenable,title,goodevent,eventtime,eventdate,counterid,clientip,regionid,userid,counterclass,os,useragent,url,referer,isrefresh,referercategoryid,refererregionid,urlcategoryid,urlregionid,resolutionwidth,resolutionheight,resolutiondepth,flashmajor,flashminor,flashminor2,netmajor,netminor,useragentmajor,useragentminor,cookieenable,javascriptenable,ismobile,mobilephone,mobilephonemodel,params,ipnetworkid,traficsourceid,searchengineid,searchphrase,advengineid,isartifical,windowclientwidth,windowclientheight,clienttimezone,clienteventtime,silverlightversion1,silverlightversion2,silverlightversion3,silverlightversion4,pagecharset,codeversion,islink,isdownload,isnotbounce,funiqid,originalurl,hid,isoldcounter,isevent,isparameter,dontcounthits,withhash,hitcolor,localeventtime,age,sex,income,interests,robotness,remoteip,windowname,openername,historylength,browserlanguage,browsercountry,socialnetwork,socialaction,httperror,sendtiming,dnstiming,connecttiming,responsestarttiming,responseendtiming,fetchtiming,socialsourcenetworkid,socialsourcepage,paramprice,paramorderid,paramcurrency,paramcurrencyid,openstatservicename,openstatcampaignid,openstatadid,openstatsourceid,utmsource,utmmedium,utmcampaign,utmcontent,utmterm,fromtag,hasgclid,refererhash,urlhash,clid""",
                        //    """WATCHID,JAVAENABLE,TITLE,GOODEVENT,EVENTTIME,EVENTDATE,COUNTERID,CLIENTIP,REGIONID,USERID,COUNTERCLASS,OS,USERAGENT,URL,REFERER,ISREFRESH,REFERERCATEGORYID,REFERERREGIONID,URLCATEGORYID,URLREGIONID,RESOLUTIONWIDTH,RESOLUTIONHEIGHT,RESOLUTIONDEPTH,FLASHMAJOR,FLASHMINOR,FLASHMINOR2,NETMAJOR,NETMINOR,USERAGENTMAJOR,USERAGENTMINOR,COOKIEENABLE,JAVASCRIPTENABLE,ISMOBILE,MOBILEPHONE,MOBILEPHONEMODEL,PARAMS,IPNETWORKID,TRAFICSOURCEID,SEARCHENGINEID,SEARCHPHRASE,ADVENGINEID,ISARTIFICAL,WINDOWCLIENTWIDTH,WINDOWCLIENTHEIGHT,CLIENTTIMEZONE,CLIENTEVENTTIME,SILVERLIGHTVERSION1,SILVERLIGHTVERSION2,SILVERLIGHTVERSION3,SILVERLIGHTVERSION4,PAGECHARSET,CODEVERSION,ISLINK,ISDOWNLOAD,ISNOTBOUNCE,FUNIQID,ORIGINALURL,HID,ISOLDCOUNTER,ISEVENT,ISPARAMETER,DONTCOUNTHITS,WITHHASH,HITCOLOR,LOCALEVENTTIME,AGE,SEX,INCOME,INTERESTS,ROBOTNESS,REMOTEIP,WINDOWNAME,OPENERNAME,HISTORYLENGTH,BROWSERLANGUAGE,BROWSERCOUNTRY,SOCIALNETWORK,SOCIALACTION,HTTPERROR,SENDTIMING,DNSTIMING,CONNECTTIMING,RESPONSESTARTTIMING,RESPONSEENDTIMING,FETCHTIMING,SOCIALSOURCENETWORKID,SOCIALSOURCEPAGE,PARAMPRICE,PARAMORDERID,PARAMCURRENCY,PARAMCURRENCYID,OPENSTATSERVICENAME,OPENSTATCAMPAIGNID,OPENSTATADID,OPENSTATSOURCEID,UTMSOURCE,UTMMEDIUM,UTMCAMPAIGN,UTMCONTENT,UTMTERM,FROMTAG,HASGCLID,REFERERHASH,URLHASH,CLID""",
                        //    """watchid,javaenable,title,goodevent,eventtime,eventdate,counterid,clientip,regionid,userid,counterclass,os,useragent,url,referer,isrefresh,referercategoryid,refererregionid,urlcategoryid,urlregionid,resolutionwidth,resolutionheight,resolutiondepth,flashmajor,flashminor,flashminor2,netmajor,netminor,useragentmajor,useragentminor,cookieenable,javascriptenable,ismobile,mobilephone,mobilephonemodel,params,ipnetworkid,traficsourceid,searchengineid,searchphrase,advengineid,isartifical,windowclientwidth,windowclientheight,clienttimezone,clienteventtime,silverlightversion1,silverlightversion2,silverlightversion3,silverlightversion4,pagecharset,codeversion,islink,isdownload,isnotbounce,funiqid,originalurl,hid,isoldcounter,isevent,isparameter,dontcounthits,withhash,hitcolor,localeventtime,age,sex,income,interests,robotness,remoteip,windowname,openername,historylength,browserlanguage,browsercountry,socialnetwork,socialaction,httperror,sendtiming,dnstiming,connecttiming,responsestarttiming,responseendtiming,fetchtiming,socialsourcenetworkid,socialsourcepage,paramprice,paramorderid,paramcurrency,paramcurrencyid,openstatservicename,openstatcampaignid,openstatadid,openstatsourceid,utmsource,utmmedium,utmcampaign,utmcontent,utmterm,fromtag,hasgclid,refererhash,urlhash,clid""",
    ]
    def column_in_paths = ["", "", "", "", "", "", "", "", "", "", "", "", "COLUMNS FROM PATH AS (city)", "", "", "", "", "", "", "", "", 
        // "",
         "", "", "", "", "", "", "", "", "", ""]
    def preceding_filters = ["", "", "", "", "", "", "", "", "", "", "", "preceding filter p_size < 10", "", "", "", "", "", "", "", "", "", 
    // "", 
    "", "", "", "", "", "", "", "", "", ""]
    def set_values = ["",
                      "",
                      "SET(comment=p_comment, retailprice=p_retailprice, container=p_container, size=p_size, type=p_type, brand=p_brand, mfgr=p_mfgr, name=p_name, partkey=p_partkey)",
                      "set(p_name=upper(p_name),p_greatest=greatest(cast(p_partkey as int), cast(p_size as int)))",
                      "set(p_partkey = p_partkey + 100)",
                      "set(partkey = p_partkey + 100)",
                      "set(partkey = p_partkey + p_size)",
                      "set(tmpk = p_partkey + 1, partkey = tmpk*2)",
                      "set(partkey = p_partkey + 1, partsize = p_size*2)",
                      "set(partsize = p_partkey + p_size)",
                      "",
                      "",
                      "",
                      "",
                      "",
                      "",
                      "",
                      "set(col4 = x1)",
                      "set(col4 = p_brand)",
                      "set(col5 = p_brand)",
                      "",
                      // "",
                      "",
                      "",
                      "",
                      "",
                      "",
                      "",
                      "",
                      "",
                      "",
                      ""
    ]
    def where_exprs = ["", "", "", "", "", "", "", "", "", "", "", "where p_partkey>10", "", "", "", "", "", "", "", "", 
        //"", 
        "", "", "", "", "", "", "", "", "", "", ""]
    def line_delimiters = ["", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", 
    // "", 
    "", "", "", "", "", "", "", "", "", "", "\u0007"]

    String ak = getS3AK()
    String sk = getS3SK()
    String enabled = context.config.otherConfigs.get("enableBrokerLoad")

    def parse_compress_type = { path ->
       def pos = path.lastIndexOf(".") 
       String type = path.substring(pos + 1)
       switch(type) {
        case "gz":
            return "GZ" 
        case "lzo":
            return "LZO"
        case "deflate":
            return "DEFLATE"
        case "plain":
            return "PLAIN"
        case "bz2":
            return "BZ2"
        case "lz4":
            return "LZ4FRAME"
        default:
            return "UNKNOWN"
       }
       return ""
    }

    def do_load_job = { uuid, path, table, columns, column_in_path, preceding_filter,
                        set_value, where_expr, line_delimiter ->
        String columns_str = ("$columns" != "") ? "($columns)" : "";
        String compress_type = "compress_type as '${parse_compress_type(path)}'"
        String line_term = ("$line_delimiter" != "") ? "lines terminated by '$line_delimiter'" : "";

        String column_separator = ("$line_term" != "UNKNOWN") ? "columns terminated by '|'" : "columns terminated by ','";

        String format_str
        if (table.startsWith("orc_s3_case")) {
            format_str = "ORC"
        } else if (table.startsWith("csv")) {
            format_str = "CSV"
        } else {
            format_str = "PARQUET"
        }
        sql """
            LOAD LABEL $uuid (
                DATA INFILE("$path")
                INTO TABLE $table
                $column_separator
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
        logger.info("Submit load with lable: $uuid, table: $table, path: $path")
        
        
    }

    def etl_info = ["unselected.rows=0; dpp.abnorm.ALL=0; dpp.norm.ALL=200000"]
    def task_info = ["cluster:${s3Endpoint}; timeout(s):14400; max_filter_ratio:0.0"]
    def error_msg = [""]

    // test load
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        def uuids = []
        try {
            def i = 0
            for (String table in tables) {
                sql new File("""${context.file.parent}/../ddl/${table}_drop.sql""").text
                sql new File("""${context.file.parent}/../ddl/${table}_create.sql""").text
                def uuid = UUID.randomUUID().toString().replace("-", "0")
                uuids.add(uuid)
                do_load_job.call(uuid, paths[i], table, columns_list[i], column_in_paths[i], preceding_filters[i],
                        set_values[i], where_exprs[i], line_delimiters[i])
                i++
            }
            
            i = 0
            for (String label in uuids) {
                def max_try_milli_secs = 60000
                while (max_try_milli_secs > 0) {
                    String[][] result = sql """ show load where label="$label" order by createtime desc limit 1; """
                    if (result[0][2].equals("FINISHED")) {
                        
                        assertTrue(result[0][6].contains(task_info[0]))
                        // assertTrue(etl_info[0] == result[0][5], "expected: " + etl_info[0] + ", actual: " + result[0][5] + ", label: $label")
                        break;
                    }
                    if (result[0][2].equals("CANCELLED")) {
                        assertTrue(result[0][6].contains(task_info[0]))
                        assertTrue(result[0][7].contains(error_msg[0]))
                        break;
                    }
                    Thread.sleep(1000)
                    max_try_milli_secs -= 1000
                    if(max_try_milli_secs <= 0) {
                        break
                        // assertTrue(1 == 2, "load Timeout: $label")
                    }
                }
                i++
            }



            def orc_expect_result = """[[20, 15901, 6025915247311731176, 1373910657, 8863282788606566657], [38, 15901, -9154375582268094750, 1373853561, 4923892366467329038], [38, 15901, -9154375582268094750, 1373853561, 8447995939656287502], [38, 15901, -9154375582268094750, 1373853565, 7451966001310881759], [38, 15901, -9154375582268094750, 1373853565, 7746521994248163870], [38, 15901, -9154375582268094750, 1373853577, 6795654975682437824], [38, 15901, -9154375582268094750, 1373853577, 9009208035649338594], [38, 15901, -9154375582268094750, 1373853608, 6374361939566017108], [38, 15901, -9154375582268094750, 1373853608, 7387298457456465364], [38, 15901, -9154375582268094750, 1373853616, 7463736180224933002]]"""
            for (String table in tables) {
                if (table.matches("orc_s3_case[23456789]")) {
                    String[][] orc_actual_result = sql """select CounterID, EventDate, UserID, EventTime, WatchID from $table order by CounterID, EventDate, UserID, EventTime, WatchID limit 10;"""
                    assertTrue("$orc_actual_result" == "$orc_expect_result")
                }
            }


            order_qt_parquet_s3_case1 """select count(*) from parquet_s3_case1 where col1=10"""
            order_qt_parquet_s3_case3 """select count(*) from parquet_s3_case3 where p_partkey < 100000"""
            order_qt_parquet_s3_case6 """select count(*) from parquet_s3_case6 where p_partkey < 100000"""
            order_qt_parquet_s3_case7 """select count(*) from parquet_s3_case7 where col4=4"""
            order_qt_parquet_s3_case8 """ select count(*) from parquet_s3_case8 where p_partkey=1"""
            // order_qt_parquet_s3_case9 """ select * from parquet_s3_case9""" // we ignore this table temporarily due to complex field type

        } finally {
            for (String table in tables) {
                sql new File("""${context.file.parent}/../ddl/${table}_drop.sql""").text
            }
        }
    }
}
