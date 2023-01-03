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

// The cases is copied from https://github.com/trinodb/trino/tree/master
// /testing/trino-product-tests/src/main/resources/sql-tests/testcases/tpcds
// and modified by Doris.
suite("load") {
    def tables = ["hits", "visits"]
    def columnsMap = [
        "hits":"WatchID,JavaEnable,Title,GoodEvent,EventTime,EventDate,CounterID,ClientIP,ClientIP6,RegionID,UserID,CounterClass,OS,UserAgent,URL,Referer,URLDomain,RefererDomain,Refresh,IsRobot,RefererCategories,URLCategories,URLRegions,RefererRegions,ResolutionWidth,ResolutionHeight,ResolutionDepth,FlashMajor,FlashMinor,FlashMinor2,NetMajor,NetMinor,UserAgentMajor,UserAgentMinor,CookieEnable,JavascriptEnable,IsMobile,MobilePhone,MobilePhoneModel,Params,IPNetworkID,TraficSourceID,SearchEngineID,SearchPhrase,AdvEngineID,IsArtifical,WindowClientWidth,WindowClientHeight,ClientTimeZone,ClientEventTime,SilverlightVersion1,SilverlightVersion2,SilverlightVersion3,SilverlightVersion4,PageCharset,CodeVersion,IsLink,IsDownload,IsNotBounce,FUniqID,HID,IsOldCounter,IsEvent,IsParameter,DontCountHits,WithHash,HitColor,UTCEventTime,Age,Sex,Income,Interests,Robotness,GeneralInterests,RemoteIP,RemoteIP6,WindowName,OpenerName,HistoryLength,BrowserLanguage,BrowserCountry,SocialNetwork,SocialAction,HTTPError,SendTiming,DNSTiming,ConnectTiming,ResponseStartTiming,ResponseEndTiming,FetchTiming,RedirectTiming,DOMInteractiveTiming,DOMContentLoadedTiming,DOMCompleteTiming,LoadEventStartTiming,LoadEventEndTiming,NSToDOMContentLoadedTiming,FirstPaintTiming,RedirectCount,SocialSourceNetworkID,SocialSourcePage,ParamPrice,ParamOrderID,ParamCurrency,ParamCurrencyID,GoalsReached,OpenstatServiceName,OpenstatCampaignID,OpenstatAdID,OpenstatSourceID,UTMSource,UTMMedium,UTMCampaign,UTMContent,UTMTerm,FromTag,HasGCLID,RefererHash,URLHash,CLID,YCLID,ShareService,ShareURL,ShareTitle,ParsedParamsKey1,ParsedParamsKey2,ParsedParamsKey3,ParsedParamsKey4,ParsedParamsKey5,ParsedParamsValueDouble,IslandID,RequestNum,RequestTry",
        "visits":"CounterID,StartDate,Sign,IsNew,VisitID,UserID,StartTime,Duration,UTCStartTime,PageViews,Hits,IsBounce,Referer,StartURL,RefererDomain,StartURLDomain,EndURL,LinkURL,IsDownload,TraficSourceID,SearchEngineID,SearchPhrase,AdvEngineID,PlaceID,RefererCategories,URLCategories,URLRegions,RefererRegions,IsYandex,GoalReachesDepth,GoalReachesURL,GoalReachesAny,SocialSourceNetworkID,SocialSourcePage,MobilePhoneModel,ClientEventTime,RegionID,ClientIP,ClientIP6,RemoteIP,RemoteIP6,IPNetworkID,SilverlightVersion3,CodeVersion,ResolutionWidth,ResolutionHeight,UserAgentMajor,UserAgentMinor,WindowClientWidth,WindowClientHeight,SilverlightVersion2,SilverlightVersion4,FlashVersion3,FlashVersion4,ClientTimeZone,OS,UserAgent,ResolutionDepth,FlashMajor,FlashMinor,NetMajor,NetMinor,MobilePhone,SilverlightVersion1,Age,Sex,Income,JavaEnable,CookieEnable,JavascriptEnable,IsMobile,BrowserLanguage,BrowserCountry,Interests,Robotness,GeneralInterests,Params,GoalsID,GoalsSerial,GoalsEventTime,GoalsPrice,GoalsOrderID,GoalsCurrencyID,WatchIDs,ParamSumPrice,ParamCurrency,ParamCurrencyID,ClickLogID,ClickEventID,ClickGoodEvent,ClickEventTime,ClickPriorityID,ClickPhraseID,ClickPageID,ClickPlaceID,ClickTypeID,ClickResourceID,ClickCost,ClickClientIP,ClickDomainID,ClickURL,ClickAttempt,ClickOrderID,ClickBannerID,ClickMarketCategoryID,ClickMarketPP,ClickMarketCategoryName,ClickMarketPPName,ClickAWAPSCampaignName,ClickPageName,ClickTargetType,ClickTargetPhraseID,ClickContextType,ClickSelectType,ClickOptions,ClickGroupBannerID,OpenstatServiceName,OpenstatCampaignID,OpenstatAdID,OpenstatSourceID,UTMSource,UTMMedium,UTMCampaign,UTMContent,UTMTerm,FromTag,HasGCLID,FirstVisit,PredLastVisit,LastVisit,TotalVisits,TraficSourceID2,TraficSourceSearchEngineI,TraficSourceAdvEngineID,TraficSourcePlaceID,TraficSourceSocialSourceN,TraficSourceDomain,TraficSourceSearchPhrase,TraficSourceSocialSourceP,Attendance,CLID,YCLID,NormalizedRefererHash,SearchPhraseHash,RefererDomainHash,NormalizedStartURLHash,StartURLDomainHash,NormalizedEndURLHash,TopLevelDomain,URLScheme,OpenstatServiceNameHash,OpenstatCampaignIDHash,OpenstatAdIDHash,OpenstatSourceIDHash,UTMSourceHash,UTMMediumHash,UTMCampaignHash,UTMContentHash,UTMTermHash,FromHash,WebVisorEnabled,WebVisorActivity,ParsedParamsKey1,ParsedParamsKey2,ParsedParamsKey3,ParsedParamsKey4,ParsedParamsKey5,ParsedParamsValueDouble,MarketType,MarketGoalID,MarketOrderID,MarketOrderPrice,MarketPP,MarketDirectPlaceID,MarketDirectOrderID,MarketDirectBannerID,MarketGoodID,MarketGoodName,MarketGoodQuantity,MarketGoodPrice,IslandID"
    ]

    for (String table in tables) {
        sql """ DROP TABLE IF EXISTS $table """
    }

    for (String table in tables) {
        sql new File("""${context.file.parent}/ddl/${table}.sql""").text
    }

    for (String tableName in tables) {
        streamLoad {
            // you can skip declare db, because a default db already specify in ${DORIS_HOME}/conf/regression-conf.groovy
            // db 'regression_test'
            table tableName

            // default label is UUID:
            // set 'label' UUID.randomUUID().toString()

            // default column_separator is specify in doris fe config, usually is '\t'.
            // this line change to ','
            set 'column_separator', '\t'
            set 'compress_type', 'GZ'
            set "columns", columnsMap[tableName]
            set 'timeout', '72000'

            // relate to ${DORIS_HOME}/regression-test/data/demo/streamload_input.csv.
            // also, you can stream load a http stream, e.g. http://xxx/some.csv
            file """${getS3Url() + '/regression/clickhouse/yandex_metrica/' + tableName}.tsv.gz"""

            time 0

            // stream load action will check result, include Success status, and NumberTotalRows == NumberLoadedRows

            // if declared a check callback, the default check condition will ignore.
            // So you must check all condition
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(json.NumberTotalRows, json.NumberLoadedRows)
                assertTrue(json.NumberLoadedRows > 0 && json.LoadBytes > 0)
            }
        }
    }
}
