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

suite("test_parquet_orc_case", "p0") {
    def tableName = "test_parquet_orc_case"

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
		CREATE TABLE IF NOT EXISTS ${tableName} (
            `WatchId` char(128), 
            `JavaEnable` smallint, 
            `Title` string, 
            `GoodEvent` smallint, 
            `EventTime` datetime, 
            `EventDate` date, 
            `CounterId` bigint, 
            `ClientIp` bigint, 
            `ClientIp6` char(50), 
            `RegionId` bigint, 
            `UserId` string, 
            `CounterClass` tinyint, 
            `Os` smallint, 
            `UserAgent` smallint, 
            `Url` string, 
            `Referer` string, 
            `Urldomain` string, 
            `RefererDomain` string, 
            `Refresh` smallint, 
            `IsRobot` smallint, 
            `RefererCategories` string, 
            `UrlCategories` string, 
            `UrlRegions` string, 
            `RefererRegions` string, 
            `ResolutionWidth` int, 
            `ResolutionHeight` int, 
            `ResolutionDepth` smallint, 
            `FlashMajor` smallint, 
            `FlashMinor` smallint, 
            `FlashMinor2` string, 
            `NetMajor` smallint, 
            `NetMinor` smallint, 
            `UserAgentMajor` int, 
            `UserAgentMinor` char(4), 
            `CookieEnable` smallint, 
            `JavascriptEnable` smallint, 
            `IsMobile` smallint, 
            `MobilePhone` smallint, 
            `MobilePhoneModel` string, 
            `Params` string, 
            `IpNetworkId` bigint, 
            `TraficSourceId` tinyint, 
            `SearchEngineId` int, 
            `SearchPhrase` string, 
            `AdvEngineId` smallint, 
            `IsArtifical` smallint, 
            `WindowClientWidth` int, 
            `WindowClientHeight` int, 
            `ClientTimeZone` smallint, 
            `ClientEventTime` datetime, 
            `SilverLightVersion1` smallint, 
            `SilverlightVersion2` smallint, 
            `SilverlightVersion3` bigint, 
            `SilverlightVersion4` int, 
            `PageCharset` string, 
            `CodeVersion` bigint, 
            `IsLink` smallint, 
            `IsDownload` smallint, 
            `IsNotBounce` smallint, 
            `FUniqId` string, 
            `Hid` bigint, 
            `IsOldCounter` smallint, 
            `IsEvent` smallint, 
            `IsParameter` smallint, 
            `DontCountHits` smallint, 
            `WithHash` smallint, 
            `HitColor` char(2), 
            `UtcEventTime` datetime, 
            `Age` smallint, 
            `Sex` smallint, 
            `Income` smallint, 
            `Interests` int, 
            `Robotness` smallint, 
            `GeneralInterests` string, 
            `RemoteIp` bigint, 
            `RemoteIp6` char(50), 
            `WindowName` int, 
            `OpenerName` int, 
            `historylength` smallint, 
            `BrowserLanguage` char(4), 
            `BrowserCountry` char(4), 
            `SocialNetwork` string, 
            `SocialAction` string, 
            `HttpError` int, 
            `SendTiming` int, 
            `DnsTiming` int, 
            `ConnectTiming` int, 
            `ResponseStartTiming` int, 
            `ResponseEndTiming` int, 
            `FetchTiming` int, 
            `RedirectTiming` int, 
            `DomInteractiveTiming` int, 
            `DomContentLoadedTiming` int, 
            `DomCompleteTiming` int, 
            `LoadEventStartTiming` int, 
            `LoadEventEndTiming` int, 
            `NsToDomContentLoadedTiming` int, 
            `FirstPaintTiming` int, 
            `RedirectCount` tinyint, 
            `SocialSourceNetworkId` smallint, 
            `SocialSourcePage` string, 
            `ParamPrice` bigint, 
            `ParamOrderId` string, 
            `ParamCurrency` char(6), 
            `ParamCurrencyId` int, 
            `GoalsReached` string, 
            `OpenStatServiceName` string, 
            `OpenStatCampaignId` string, 
            `OpenStatAdId` string, 
            `OpenStatSourceId` string, 
            `UtmSource` string, 
            `UtmMedium` string, 
            `UtmCampaign` string, 
            `UtmContent` string, 
            `UtmTerm` string, 
            `FromTag` string, 
            `HasGclId` smallint, 
            `RefererHash` string, 
            `UrlHash` string, 
            `ClId` bigint, 
            `YclId` string, 
            `ShareService` string, 
            `ShareUrl` string, 
            `ShareTitle` string, 
            `ParsedParamsKey1` string, 
            `ParsedParamsKey2` string, 
            `ParsedParamsKey3` string, 
            `ParsedParamsKey4` string, 
            `ParsedParamsKey5` string, 
            `ParsedParamsValueDouble` double, 
            `IsLandId` char(40), 
            `RequestNum` bigint, 
            `RequestTry` smallint
        ) ENGINE=OLAP
        DUPLICATE KEY(`WatchId`, `JavaEnable`)
        DISTRIBUTED BY HASH(`WatchId`, `JavaEnable`) BUCKETS 3
        PROPERTIES ("replication_num" = "1");
    """

    streamLoad {
        table "${tableName}"
        set 'format', 'parquet'
        set 'columns', 'watchid, javaenable, title, goodevent, eventtime, eventdate, counterid, clientip, clientip6, regionid, userid, counterclass, os, useragent, url, referer, urldomain, refererdomain, refresh, isrobot, referercategories, urlcategories, urlregions, refererregions, resolutionwidth, resolutionheight, resolutiondepth, flashmajor, flashminor, flashminor2, netmajor, netminor, useragentmajor, useragentminor, cookieenable, javascriptenable, ismobile, mobilephone, mobilephonemodel, params, ipnetworkid, traficsourceid, searchengineid, searchphrase, advengineid, isartifical, windowclientwidth, windowclientheight, clienttimezone, clienteventtime, silverlightversion1, silverlightversion2, silverlightversion3, silverlightversion4, pagecharset, codeversion, islink, isdownload, isnotbounce, funiqid, hid, isoldcounter, isevent, isparameter, dontcounthits, withhash, hitcolor, utceventtime, age, sex, income, interests, robotness, generalinterests, remoteip, remoteip6, windowname, openername, historylength, browserlanguage, browsercountry, socialnetwork, socialaction, httperror, sendtiming, dnstiming, connecttiming, responsestarttiming, responseendtiming, fetchtiming, redirecttiming, dominteractivetiming, domcontentloadedtiming, domcompletetiming, loadeventstarttiming, loadeventendtiming, nstodomcontentloadedtiming, firstpainttiming, redirectcount, socialsourcenetworkid, socialsourcepage, paramprice, paramorderid, paramcurrency, paramcurrencyid, goalsreached, openstatservicename, openstatcampaignid, openstatadid, openstatsourceid, utmsource, utmmedium, utmcampaign, utmcontent, utmterm, fromtag, hasgclid, refererhash, urlhash, clid, yclid, shareservice, shareurl, sharetitle, parsedparamskey1, parsedparamskey2, parsedparamskey3, parsedparamskey4, parsedparamskey5, parsedparamsvaluedouble, islandid, requestnum, requesttry'
        file 'test_parquet_case.parquet'
        // time 20000 // limit inflight 10s
    }
    sql "sync"
    qt_sql "select * from ${tableName} order by WatchId"
    sql """truncate table ${tableName}"""

    streamLoad {
        table "${tableName}"
        set 'format', 'parquet'
        set 'columns', 'WATCHID, JAVAENABLE, TITLE, GOODEVENT, EVENTTIME, EVENTDATE, COUNTERID, CLIENTIP, CLIENTIP6, REGIONID, USERID, COUNTERCLASS, OS, USERAGENT, URL, REFERER, URLDOMAIN, REFERERDOMAIN, REFRESH, ISROBOT, REFERERCATEGORIES, URLCATEGORIES, URLREGIONS, REFERERREGIONS, RESOLUTIONWIDTH, RESOLUTIONHEIGHT, RESOLUTIONDEPTH, FLASHMAJOR, FLASHMINOR, FLASHMINOR2, NETMAJOR, NETMINOR, USERAGENTMAJOR, USERAGENTMINOR, COOKIEENABLE, JAVASCRIPTENABLE, ISMOBILE, MOBILEPHONE, MOBILEPHONEMODEL, PARAMS, IPNETWORKID, TRAFICSOURCEID, SEARCHENGINEID, SEARCHPHRASE, ADVENGINEID, ISARTIFICAL, WINDOWCLIENTWIDTH, WINDOWCLIENTHEIGHT, CLIENTTIMEZONE, CLIENTEVENTTIME, SILVERLIGHTVERSION1, SILVERLIGHTVERSION2, SILVERLIGHTVERSION3, SILVERLIGHTVERSION4, PAGECHARSET, CODEVERSION, ISLINK, ISDOWNLOAD, ISNOTBOUNCE, FUNIQID, HID, ISOLDCOUNTER, ISEVENT, ISPARAMETER, DONTCOUNTHITS, WITHHASH, HITCOLOR, UTCEVENTTIME, AGE, SEX, INCOME, INTERESTS, ROBOTNESS, GENERALINTERESTS, REMOTEIP, REMOTEIP6, WINDOWNAME, OPENERNAME, HISTORYLENGTH, BROWSERLANGUAGE, BROWSERCOUNTRY, SOCIALNETWORK, SOCIALACTION, HTTPERROR, SENDTIMING, DNSTIMING, CONNECTTIMING, RESPONSESTARTTIMING, RESPONSEENDTIMING, FETCHTIMING, REDIRECTTIMING, DOMINTERACTIVETIMING, DOMCONTENTLOADEDTIMING, DOMCOMPLETETIMING, LOADEVENTSTARTTIMING, LOADEVENTENDTIMING, NSTODOMCONTENTLOADEDTIMING, FIRSTPAINTTIMING, REDIRECTCOUNT, SOCIALSOURCENETWORKID, SOCIALSOURCEPAGE, PARAMPRICE, PARAMORDERID, PARAMCURRENCY, PARAMCURRENCYID, GOALSREACHED, OPENSTATSERVICENAME, OPENSTATCAMPAIGNID, OPENSTATADID, OPENSTATSOURCEID, UTMSOURCE, UTMMEDIUM, UTMCAMPAIGN, UTMCONTENT, UTMTERM, FROMTAG, HASGCLID, REFERERHASH, URLHASH, CLID, YCLID, SHARESERVICE, SHAREURL, SHARETITLE, PARSEDPARAMSKEY1, PARSEDPARAMSKEY2, PARSEDPARAMSKEY3, PARSEDPARAMSKEY4, PARSEDPARAMSKEY5, PARSEDPARAMSVALUEDOUBLE, ISLANDID, REQUESTNUM, REQUESTTRY'
        file 'test_parquet_case.parquet'
        // time 20000 // limit inflight 10s
    }
    sql "sync"
    qt_sql "select * from ${tableName} order by WatchId"
    sql """truncate table ${tableName}"""

    streamLoad {
        table "${tableName}"
        set 'format', 'orc'
        set 'columns', 'watchid, javaenable, title, goodevent, eventtime, eventdate, counterid, clientip, clientip6, regionid, userid, counterclass, os, useragent, url, referer, urldomain, refererdomain, refresh, isrobot, referercategories, urlcategories, urlregions, refererregions, resolutionwidth, resolutionheight, resolutiondepth, flashmajor, flashminor, flashminor2, netmajor, netminor, useragentmajor, useragentminor, cookieenable, javascriptenable, ismobile, mobilephone, mobilephonemodel, params, ipnetworkid, traficsourceid, searchengineid, searchphrase, advengineid, isartifical, windowclientwidth, windowclientheight, clienttimezone, clienteventtime, silverlightversion1, silverlightversion2, silverlightversion3, silverlightversion4, pagecharset, codeversion, islink, isdownload, isnotbounce, funiqid, hid, isoldcounter, isevent, isparameter, dontcounthits, withhash, hitcolor, utceventtime, age, sex, income, interests, robotness, generalinterests, remoteip, remoteip6, windowname, openername, historylength, browserlanguage, browsercountry, socialnetwork, socialaction, httperror, sendtiming, dnstiming, connecttiming, responsestarttiming, responseendtiming, fetchtiming, redirecttiming, dominteractivetiming, domcontentloadedtiming, domcompletetiming, loadeventstarttiming, loadeventendtiming, nstodomcontentloadedtiming, firstpainttiming, redirectcount, socialsourcenetworkid, socialsourcepage, paramprice, paramorderid, paramcurrency, paramcurrencyid, goalsreached, openstatservicename, openstatcampaignid, openstatadid, openstatsourceid, utmsource, utmmedium, utmcampaign, utmcontent, utmterm, fromtag, hasgclid, refererhash, urlhash, clid, yclid, shareservice, shareurl, sharetitle, parsedparamskey1, parsedparamskey2, parsedparamskey3, parsedparamskey4, parsedparamskey5, parsedparamsvaluedouble, islandid, requestnum, requesttry'
        file 'test_orc_case.orc'
        // time 20000 // limit inflight 10s
    }
    sql "sync"
    qt_sql "select * from ${tableName} order by WatchId"
    sql """truncate table ${tableName}"""

    streamLoad {
        table "${tableName}"
        set 'format', 'orc'
        set 'columns', 'WATCHID, JAVAENABLE, TITLE, GOODEVENT, EVENTTIME, EVENTDATE, COUNTERID, CLIENTIP, CLIENTIP6, REGIONID, USERID, COUNTERCLASS, OS, USERAGENT, URL, REFERER, URLDOMAIN, REFERERDOMAIN, REFRESH, ISROBOT, REFERERCATEGORIES, URLCATEGORIES, URLREGIONS, REFERERREGIONS, RESOLUTIONWIDTH, RESOLUTIONHEIGHT, RESOLUTIONDEPTH, FLASHMAJOR, FLASHMINOR, FLASHMINOR2, NETMAJOR, NETMINOR, USERAGENTMAJOR, USERAGENTMINOR, COOKIEENABLE, JAVASCRIPTENABLE, ISMOBILE, MOBILEPHONE, MOBILEPHONEMODEL, PARAMS, IPNETWORKID, TRAFICSOURCEID, SEARCHENGINEID, SEARCHPHRASE, ADVENGINEID, ISARTIFICAL, WINDOWCLIENTWIDTH, WINDOWCLIENTHEIGHT, CLIENTTIMEZONE, CLIENTEVENTTIME, SILVERLIGHTVERSION1, SILVERLIGHTVERSION2, SILVERLIGHTVERSION3, SILVERLIGHTVERSION4, PAGECHARSET, CODEVERSION, ISLINK, ISDOWNLOAD, ISNOTBOUNCE, FUNIQID, HID, ISOLDCOUNTER, ISEVENT, ISPARAMETER, DONTCOUNTHITS, WITHHASH, HITCOLOR, UTCEVENTTIME, AGE, SEX, INCOME, INTERESTS, ROBOTNESS, GENERALINTERESTS, REMOTEIP, REMOTEIP6, WINDOWNAME, OPENERNAME, HISTORYLENGTH, BROWSERLANGUAGE, BROWSERCOUNTRY, SOCIALNETWORK, SOCIALACTION, HTTPERROR, SENDTIMING, DNSTIMING, CONNECTTIMING, RESPONSESTARTTIMING, RESPONSEENDTIMING, FETCHTIMING, REDIRECTTIMING, DOMINTERACTIVETIMING, DOMCONTENTLOADEDTIMING, DOMCOMPLETETIMING, LOADEVENTSTARTTIMING, LOADEVENTENDTIMING, NSTODOMCONTENTLOADEDTIMING, FIRSTPAINTTIMING, REDIRECTCOUNT, SOCIALSOURCENETWORKID, SOCIALSOURCEPAGE, PARAMPRICE, PARAMORDERID, PARAMCURRENCY, PARAMCURRENCYID, GOALSREACHED, OPENSTATSERVICENAME, OPENSTATCAMPAIGNID, OPENSTATADID, OPENSTATSOURCEID, UTMSOURCE, UTMMEDIUM, UTMCAMPAIGN, UTMCONTENT, UTMTERM, FROMTAG, HASGCLID, REFERERHASH, URLHASH, CLID, YCLID, SHARESERVICE, SHAREURL, SHARETITLE, PARSEDPARAMSKEY1, PARSEDPARAMSKEY2, PARSEDPARAMSKEY3, PARSEDPARAMSKEY4, PARSEDPARAMSKEY5, PARSEDPARAMSVALUEDOUBLE, ISLANDID, REQUESTNUM, REQUESTTRY'
        file 'test_orc_case.orc'
        // time 20000 // limit inflight 10s
    }
    sql "sync"
    qt_sql "select * from ${tableName} order by WatchId"
    sql """truncate table ${tableName}"""


    sql """ DROP TABLE IF EXISTS ${tableName} """

    def arrayParquetTbl = "test_array_parquet_tb"
    sql """ DROP TABLE IF EXISTS ${arrayParquetTbl} """

    sql """
    CREATE TABLE ${arrayParquetTbl} (
        k1 int NULL, 
        a1 array<boolean> NULL, 
        a2 array<tinyint> NULL, 
        a3 array<smallint> NULL, 
        a4 array<int> NULL, 
        a5 array<bigint> NULL, 
        a6 array<largeint> NULL,
        a7 array<decimal(25, 7)> NULL,
        a8 array<float> NULL, 
        a9 array<double> NULL, 
        a10 array<date> NULL, 
        a11 array<datetime> NULL, 
        a12 array<char(20)> NULL, 
        a13 array<varchar(50)> NULL, 
        a14 array<string> NULL 
    ) 
    DUPLICATE KEY(k1) 
    DISTRIBUTED BY HASH(k1) BUCKETS 5
    PROPERTIES(
        "replication_num"="1"
    );
    """
}
