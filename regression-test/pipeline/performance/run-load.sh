#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Build Step: Command Line
: <<EOF
#!/bin/bash
export DEBUG=true

if [[ -f "${teamcity_build_checkoutDir:-}"/regression-test/pipeline/performance/run-load.sh ]]; then
    cd "${teamcity_build_checkoutDir}"/regression-test/pipeline/performance/
    bash -x run-load.sh
else
    echo "Build Step file missing: regression-test/pipeline/performance/run-load.sh" && exit 1
fi
EOF

#####################################################################################
## run-load.sh content ##

# shellcheck source=/dev/null
# restart_doris, set_session_variable
source "${teamcity_build_checkoutDir}"/regression-test/pipeline/common/doris-utils.sh
# shellcheck source=/dev/null
# create_an_issue_comment
source "${teamcity_build_checkoutDir}"/regression-test/pipeline/common/github-utils.sh
# shellcheck source=/dev/null
# upload_doris_log_to_oss
source "${teamcity_build_checkoutDir}"/regression-test/pipeline/common/oss-utils.sh

if ${DEBUG:-false}; then
    pull_request_num="28431"
    commit_id="5f5c4c80564c76ff4267fc4ce6a5408498ed1ab5"
fi
echo "#### Check env"
if [[ -z "${teamcity_build_checkoutDir}" ||
    -z "${pull_request_num}" ||
    -z "${commit_id}" ]]; then
    echo "ERROR: env teamcity_build_checkoutDir or pull_request_num or commit_id not set"
    exit 1
fi

# shellcheck source=/dev/null
source "$(bash "${teamcity_build_checkoutDir}"/regression-test/pipeline/common/get-or-set-tmp-env.sh 'get')"
if ${skip_pipeline:=false}; then echo "INFO: skip build pipline" && exit 0; else echo "INFO: no skip"; fi

echo "#### Run tpch test on Doris ####"
DORIS_HOME="${teamcity_build_checkoutDir}/output"
export DORIS_HOME
data_home="/data/clickbench/"
query_port=$(get_doris_conf_value "${DORIS_HOME}"/fe/conf/fe.conf query_port)
http_port=$(get_doris_conf_value "${DORIS_HOME}"/fe/conf/fe.conf http_port)
clt="mysql -h127.0.0.1 -P${query_port} -uroot "
DB="load_test_db"
stream_load_json_speed_threshold=${stream_load_json_speed_threshold:-100}      # 单位 MB/s
stream_load_orc_speed_threshold=${stream_load_orc_speed_threshold:-10}         # 单位 MB/s
stream_load_parquet_speed_threshold=${stream_load_parquet_speed_threshold:-10} # 单位 MB/s
insert_into_select_speed_threshold=${insert_into_select_speed_threshold:-310}  # 单位 Krows/s
exit_flag=0

(
    set -e
    shopt -s inherit_errexit

    stream_load_json() {
        echo "#### create table"
        ddl="
            CREATE TABLE IF NOT EXISTS  hits_json (
                CounterID INT NOT NULL, 
                EventDate DateV2 NOT NULL, 
                UserID BIGINT NOT NULL, 
                EventTime DateTimeV2 NOT NULL, 
                WatchID BIGINT NOT NULL, 
                JavaEnable SMALLINT NOT NULL,
                Title STRING NOT NULL,
                GoodEvent SMALLINT NOT NULL,
                ClientIP INT NOT NULL,
                RegionID INT NOT NULL,
                CounterClass SMALLINT NOT NULL,
                OS SMALLINT NOT NULL,
                UserAgent SMALLINT NOT NULL,
                URL STRING NOT NULL,
                Referer STRING NOT NULL,
                IsRefresh SMALLINT NOT NULL,
                RefererCategoryID SMALLINT NOT NULL,
                RefererRegionID INT NOT NULL,
                URLCategoryID SMALLINT NOT NULL,
                URLRegionID INT NOT NULL,
                ResolutionWidth SMALLINT NOT NULL,
                ResolutionHeight SMALLINT NOT NULL,
                ResolutionDepth SMALLINT NOT NULL,
                FlashMajor SMALLINT NOT NULL,
                FlashMinor SMALLINT NOT NULL,
                FlashMinor2 STRING NOT NULL,
                NetMajor SMALLINT NOT NULL,
                NetMinor SMALLINT NOT NULL,
                UserAgentMajor SMALLINT NOT NULL,
                UserAgentMinor VARCHAR(255) NOT NULL,
                CookieEnable SMALLINT NOT NULL,
                JavascriptEnable SMALLINT NOT NULL,
                IsMobile SMALLINT NOT NULL,
                MobilePhone SMALLINT NOT NULL,
                MobilePhoneModel STRING NOT NULL,
                Params STRING NOT NULL,
                IPNetworkID INT NOT NULL,
                TraficSourceID SMALLINT NOT NULL,
                SearchEngineID SMALLINT NOT NULL,
                SearchPhrase STRING NOT NULL,
                AdvEngineID SMALLINT NOT NULL,
                IsArtifical SMALLINT NOT NULL,
                WindowClientWidth SMALLINT NOT NULL,
                WindowClientHeight SMALLINT NOT NULL,
                ClientTimeZone SMALLINT NOT NULL,
                ClientEventTime DateTimeV2 NOT NULL,
                SilverlightVersion1 SMALLINT NOT NULL,
                SilverlightVersion2 SMALLINT NOT NULL,
                SilverlightVersion3 INT NOT NULL,
                SilverlightVersion4 SMALLINT NOT NULL,
                PageCharset STRING NOT NULL,
                CodeVersion INT NOT NULL,
                IsLink SMALLINT NOT NULL,
                IsDownload SMALLINT NOT NULL,
                IsNotBounce SMALLINT NOT NULL,
                FUniqID BIGINT NOT NULL,
                OriginalURL STRING NOT NULL,
                HID INT NOT NULL,
                IsOldCounter SMALLINT NOT NULL,
                IsEvent SMALLINT NOT NULL,
                IsParameter SMALLINT NOT NULL,
                DontCountHits SMALLINT NOT NULL,
                WithHash SMALLINT NOT NULL,
                HitColor CHAR NOT NULL,
                LocalEventTime DateTimeV2 NOT NULL,
                Age SMALLINT NOT NULL,
                Sex SMALLINT NOT NULL,
                Income SMALLINT NOT NULL,
                Interests SMALLINT NOT NULL,
                Robotness SMALLINT NOT NULL,
                RemoteIP INT NOT NULL,
                WindowName INT NOT NULL,
                OpenerName INT NOT NULL,
                HistoryLength SMALLINT NOT NULL,
                BrowserLanguage STRING NOT NULL,
                BrowserCountry STRING NOT NULL,
                SocialNetwork STRING NOT NULL,
                SocialAction STRING NOT NULL,
                HTTPError SMALLINT NOT NULL,
                SendTiming INT NOT NULL,
                DNSTiming INT NOT NULL,
                ConnectTiming INT NOT NULL,
                ResponseStartTiming INT NOT NULL,
                ResponseEndTiming INT NOT NULL,
                FetchTiming INT NOT NULL,
                SocialSourceNetworkID SMALLINT NOT NULL,
                SocialSourcePage STRING NOT NULL,
                ParamPrice BIGINT NOT NULL,
                ParamOrderID STRING NOT NULL,
                ParamCurrency STRING NOT NULL,
                ParamCurrencyID SMALLINT NOT NULL,
                OpenstatServiceName STRING NOT NULL,
                OpenstatCampaignID STRING NOT NULL,
                OpenstatAdID STRING NOT NULL,
                OpenstatSourceID STRING NOT NULL,
                UTMSource STRING NOT NULL,
                UTMMedium STRING NOT NULL,
                UTMCampaign STRING NOT NULL,
                UTMContent STRING NOT NULL,
                UTMTerm STRING NOT NULL,
                FromTag STRING NOT NULL,
                HasGCLID SMALLINT NOT NULL,
                RefererHash BIGINT NOT NULL,
                URLHash BIGINT NOT NULL,
                CLID INT NOT NULL
            )  
            DUPLICATE KEY (CounterID, EventDate, UserID, EventTime, WatchID) 
            DISTRIBUTED BY HASH(UserID) BUCKETS 16
            PROPERTIES (\"replication_num\"=\"1\");
        "
        ${clt} -D"${DB}" -e"${ddl}"
        echo "#### load data"
        if [[ ! -d "${data_home}" ]]; then mkdir -p "${data_home}"; fi
        if [[ ! -f "${data_home}"/hits.json.1000000 ]] || [[ $(wc -c "${data_home}"/hits.json.1000000 | awk '{print $1}') != '2358488459' ]]; then
            cd "${data_home}"
            wget --continue 'https://doris-build-1308700295.cos.ap-beijing.myqcloud.com/ClickBench/hits.json.1000000'
            cd -
        fi
        ret=$(
            curl --location-trusted \
                -u root: \
                -T "${data_home}/hits.json.1000000" \
                -H "format:json" \
                -H "label:hits_json" \
                -H "read_json_by_line:true" \
                -H 'jsonpaths:["$.WatchID","$.JavaEnable","$.Title","$.GoodEvent","$.EventTime","$.EventDate","$.CounterID","$.ClientIP","$.RegionID","$.UserID","$.CounterClass","$.OS","$.UserAgent","$.URL","$.Referer","$.IsRefresh","$.RefererCategoryID","$.RefererRegionID","$.URLCategoryID","$.URLRegionID","$.ResolutionWidth","$.ResolutionHeight","$.ResolutionDepth","$.FlashMajor","$.FlashMinor","$.FlashMinor2","$.NetMajor","$.NetMinor","$.UserAgentMajor","$.UserAgentMinor","$.CookieEnable","$.JavascriptEnable","$.IsMobile","$.MobilePhone","$.MobilePhoneModel","$.Params","$.IPNetworkID","$.TraficSourceID","$.SearchEngineID","$.SearchPhrase","$.AdvEngineID","$.IsArtifical","$.WindowClientWidth","$.WindowClientHeight","$.ClientTimeZone","$.ClientEventTime","$.SilverlightVersion1","$.SilverlightVersion2","$.SilverlightVersion3","$.SilverlightVersion4","$.PageCharset","$.CodeVersion","$.IsLink","$.IsDownload","$.IsNotBounce","$.FUniqID","$.OriginalURL","$.HID","$.IsOldCounter","$.IsEvent","$.IsParameter","$.DontCountHits","$.WithHash","$.HitColor","$.LocalEventTime","$.Age","$.Sex","$.Income","$.Interests","$.Robotness","$.RemoteIP","$.WindowName","$.OpenerName","$.HistoryLength","$.BrowserLanguage","$.BrowserCountry","$.SocialNetwork","$.SocialAction","$.HTTPError","$.SendTiming","$.DNSTiming","$.ConnectTiming","$.ResponseStartTiming","$.ResponseEndTiming","$.FetchTiming","$.SocialSourceNetworkID","$.SocialSourcePage","$.ParamPrice","$.ParamOrderID","$.ParamCurrency","$.ParamCurrencyID","$.OpenstatServiceName","$.OpenstatCampaignID","$.OpenstatAdID","$.OpenstatSourceID","$.UTMSource","$.UTMMedium","$.UTMCampaign","$.UTMContent","$.UTMTerm","$.FromTag","$.HasGCLID","$.RefererHash","$.URLHash","$.CLID"]' \
                -H "columns: WatchID,JavaEnable,Title,GoodEvent,EventTime,EventDate,CounterID,ClientIP,RegionID,UserID,CounterClass,OS,UserAgent,URL,Referer,IsRefresh,RefererCategoryID,RefererRegionID,URLCategoryID,URLRegionID,ResolutionWidth,ResolutionHeight,ResolutionDepth,FlashMajor,FlashMinor,FlashMinor2,NetMajor,NetMinor,UserAgentMajor,UserAgentMinor,CookieEnable,JavascriptEnable,IsMobile,MobilePhone,MobilePhoneModel,Params,IPNetworkID,TraficSourceID,SearchEngineID,SearchPhrase,AdvEngineID,IsArtifical,WindowClientWidth,WindowClientHeight,ClientTimeZone,ClientEventTime,SilverlightVersion1,SilverlightVersion2,SilverlightVersion3,SilverlightVersion4,PageCharset,CodeVersion,IsLink,IsDownload,IsNotBounce,FUniqID,OriginalURL,HID,IsOldCounter,IsEvent,IsParameter,DontCountHits,WithHash,HitColor,LocalEventTime,Age,Sex,Income,Interests,Robotness,RemoteIP,WindowName,OpenerName,HistoryLength,BrowserLanguage,BrowserCountry,SocialNetwork,SocialAction,HTTPError,SendTiming,DNSTiming,ConnectTiming,ResponseStartTiming,ResponseEndTiming,FetchTiming,SocialSourceNetworkID,SocialSourcePage,ParamPrice,ParamOrderID,ParamCurrency,ParamCurrencyID,OpenstatServiceName,OpenstatCampaignID,OpenstatAdID,OpenstatSourceID,UTMSource,UTMMedium,UTMCampaign,UTMContent,UTMTerm,FromTag,HasGCLID,RefererHash,URLHash,CLID" \
                "http://${FE_HOST:-127.0.0.1}:${http_port}/api/${DB}/hits_json/_stream_load"
        )
        sleep 5
        if [[ $(${clt} -D"${DB}" -e"select count(*) from hits_json" | sed -n '2p') != 1000000 ]]; then echo "check load fail..." && return 1; fi

        echo "#### record load test result"
        stream_load_json_size=$(echo "${ret}" | jq '.LoadBytes')
        stream_load_json_time=$(printf "%.0f" "$(echo "scale=1;$(echo "${ret}" | jq '.LoadTimeMs')/1000" | bc)")
        stream_load_json_speed=$(echo "${stream_load_json_size} / 1024 / 1024/ ${stream_load_json_time}" | bc)
        export stream_load_json_size
        export stream_load_json_time
        export stream_load_json_speed
    }

    stream_load_orc() {
        echo "#### create table"
        ddl="
        CREATE TABLE IF NOT EXISTS  hits_orc (
            CounterID INT NOT NULL, 
            EventDate INT NOT NULL, 
            UserID BIGINT NOT NULL, 
            EventTime INT NOT NULL, 
            WatchID BIGINT NOT NULL, 
            JavaEnable SMALLINT NOT NULL,
            Title STRING NOT NULL,
            GoodEvent SMALLINT NOT NULL,
            ClientIP INT NOT NULL,
            RegionID INT NOT NULL,
            CounterClass SMALLINT NOT NULL,
            OS SMALLINT NOT NULL,
            UserAgent SMALLINT NOT NULL,
            URL STRING NOT NULL,
            Referer STRING NOT NULL,
            IsRefresh SMALLINT NOT NULL,
            RefererCategoryID SMALLINT NOT NULL,
            RefererRegionID INT NOT NULL,
            URLCategoryID SMALLINT NOT NULL,
            URLRegionID INT NOT NULL,
            ResolutionWidth SMALLINT NOT NULL,
            ResolutionHeight SMALLINT NOT NULL,
            ResolutionDepth SMALLINT NOT NULL,
            FlashMajor SMALLINT NOT NULL,
            FlashMinor SMALLINT NOT NULL,
            FlashMinor2 STRING NOT NULL,
            NetMajor SMALLINT NOT NULL,
            NetMinor SMALLINT NOT NULL,
            UserAgentMajor SMALLINT NOT NULL,
            UserAgentMinor VARCHAR(255) NOT NULL,
            CookieEnable SMALLINT NOT NULL,
            JavascriptEnable SMALLINT NOT NULL,
            IsMobile SMALLINT NOT NULL,
            MobilePhone SMALLINT NOT NULL,
            MobilePhoneModel STRING NOT NULL,
            Params STRING NOT NULL,
            IPNetworkID INT NOT NULL,
            TraficSourceID SMALLINT NOT NULL,
            SearchEngineID SMALLINT NOT NULL,
            SearchPhrase STRING NOT NULL,
            AdvEngineID SMALLINT NOT NULL,
            IsArtifical SMALLINT NOT NULL,
            WindowClientWidth SMALLINT NOT NULL,
            WindowClientHeight SMALLINT NOT NULL,
            ClientTimeZone SMALLINT NOT NULL,
            ClientEventTime INT NOT NULL,
            SilverlightVersion1 SMALLINT NOT NULL,
            SilverlightVersion2 SMALLINT NOT NULL,
            SilverlightVersion3 INT NOT NULL,
            SilverlightVersion4 SMALLINT NOT NULL,
            PageCharset STRING NOT NULL,
            CodeVersion INT NOT NULL,
            IsLink SMALLINT NOT NULL,
            IsDownload SMALLINT NOT NULL,
            IsNotBounce SMALLINT NOT NULL,
            FUniqID BIGINT NOT NULL,
            OriginalURL STRING NOT NULL,
            HID INT NOT NULL,
            IsOldCounter SMALLINT NOT NULL,
            IsEvent SMALLINT NOT NULL,
            IsParameter SMALLINT NOT NULL,
            DontCountHits SMALLINT NOT NULL,
            WithHash SMALLINT NOT NULL,
            HitColor CHAR NOT NULL,
            LocalEventTime INT NOT NULL,
            Age SMALLINT NOT NULL,
            Sex SMALLINT NOT NULL,
            Income SMALLINT NOT NULL,
            Interests SMALLINT NOT NULL,
            Robotness SMALLINT NOT NULL,
            RemoteIP INT NOT NULL,
            WindowName INT NOT NULL,
            OpenerName INT NOT NULL,
            HistoryLength SMALLINT NOT NULL,
            BrowserLanguage STRING NOT NULL,
            BrowserCountry STRING NOT NULL,
            SocialNetwork STRING NOT NULL,
            SocialAction STRING NOT NULL,
            HTTPError SMALLINT NOT NULL,
            SendTiming INT NOT NULL,
            DNSTiming INT NOT NULL,
            ConnectTiming INT NOT NULL,
            ResponseStartTiming INT NOT NULL,
            ResponseEndTiming INT NOT NULL,
            FetchTiming INT NOT NULL,
            SocialSourceNetworkID SMALLINT NOT NULL,
            SocialSourcePage STRING NOT NULL,
            ParamPrice BIGINT NOT NULL,
            ParamOrderID STRING NOT NULL,
            ParamCurrency STRING NOT NULL,
            ParamCurrencyID SMALLINT NOT NULL,
            OpenstatServiceName STRING NOT NULL,
            OpenstatCampaignID STRING NOT NULL,
            OpenstatAdID STRING NOT NULL,
            OpenstatSourceID STRING NOT NULL,
            UTMSource STRING NOT NULL,
            UTMMedium STRING NOT NULL,
            UTMCampaign STRING NOT NULL,
            UTMContent STRING NOT NULL,
            UTMTerm STRING NOT NULL,
            FromTag STRING NOT NULL,
            HasGCLID SMALLINT NOT NULL,
            RefererHash BIGINT NOT NULL,
            URLHash BIGINT NOT NULL,
            CLID INT NOT NULL
        )  
        DUPLICATE KEY (CounterID, EventDate, UserID, EventTime, WatchID) 
        DISTRIBUTED BY HASH(UserID) BUCKETS 16
        PROPERTIES (\"replication_num\"=\"1\");
        "
        ${clt} -D"${DB}" -e"${ddl}"
        echo "#### load data"
        if [[ ! -d "${data_home}" ]]; then mkdir -p "${data_home}"; fi
        if [[ ! -f "${data_home}"/hits_0.orc ]] || [[ $(wc -c "${data_home}"/hits_0.orc | awk '{print $1}') != '1101869774' ]]; then
            cd "${data_home}"
            wget --continue 'https://doris-build-1308700295.cos.ap-beijing.myqcloud.com/ClickBench/hits_0.orc'
            cd -
        fi
        ret=$(
            curl --location-trusted \
                -u root: \
                -T "${data_home}/hits_0.orc" \
                -H "format:orc" \
                -H "label:hits_0_orc" \
                -H "columns: watchid,javaenable,title,goodevent,eventtime,eventdate,counterid,clientip,regionid,userid,counterclass,os,useragent,url,referer,isrefresh,referercategoryid,refererregionid,urlcategoryid,urlregionid,resolutionwidth,resolutionheight,resolutiondepth,flashmajor,flashminor,flashminor2,netmajor,netminor,useragentmajor,useragentminor,cookieenable,javascriptenable,ismobile,mobilephone,mobilephonemodel,params,ipnetworkid,traficsourceid,searchengineid,searchphrase,advengineid,isartifical,windowclientwidth,windowclientheight,clienttimezone,clienteventtime,silverlightversion1,silverlightversion2,silverlightversion3,silverlightversion4,pagecharset,codeversion,islink,isdownload,isnotbounce,funiqid,originalurl,hid,isoldcounter,isevent,isparameter,dontcounthits,withhash,hitcolor,localeventtime,age,sex,income,interests,robotness,remoteip,windowname,openername,historylength,browserlanguage,browsercountry,socialnetwork,socialaction,httperror,sendtiming,dnstiming,connecttiming,responsestarttiming,responseendtiming,fetchtiming,socialsourcenetworkid,socialsourcepage,paramprice,paramorderid,paramcurrency,paramcurrencyid,openstatservicename,openstatcampaignid,openstatadid,openstatsourceid,utmsource,utmmedium,utmcampaign,utmcontent,utmterm,fromtag,hasgclid,refererhash,urlhash,clid" \
                "http://${FE_HOST:-127.0.0.1}:${http_port}/api/${DB}/hits_orc/_stream_load"
        )
        sleep 5
        if [[ $(${clt} -D"${DB}" -e"select count(*) from hits_orc" | sed -n '2p') != 8800160 ]]; then echo "check load fail..." && return 1; fi

        echo "#### record load test result"
        stream_load_orc_size=$(echo "${ret}" | jq '.LoadBytes')
        stream_load_orc_time=$(printf "%.0f" "$(echo "scale=1;$(echo "${ret}" | jq '.LoadTimeMs')/1000" | bc)")
        stream_load_orc_speed=$(echo "${stream_load_orc_size} / 1024 / 1024/ ${stream_load_orc_time}" | bc)
        export stream_load_orc_size
        export stream_load_orc_time
        export stream_load_orc_speed
    }

    stream_load_parquet() {
        echo "#### create table"
        ddl="
        CREATE TABLE IF NOT EXISTS  hits_parquet (
            CounterID INT NOT NULL, 
            EventDate INT NOT NULL, 
            UserID BIGINT NOT NULL, 
            EventTime INT NOT NULL, 
            WatchID BIGINT NOT NULL, 
            JavaEnable SMALLINT NOT NULL,
            Title STRING NOT NULL,
            GoodEvent SMALLINT NOT NULL,
            ClientIP INT NOT NULL,
            RegionID INT NOT NULL,
            CounterClass SMALLINT NOT NULL,
            OS SMALLINT NOT NULL,
            UserAgent SMALLINT NOT NULL,
            URL STRING NOT NULL,
            Referer STRING NOT NULL,
            IsRefresh SMALLINT NOT NULL,
            RefererCategoryID SMALLINT NOT NULL,
            RefererRegionID INT NOT NULL,
            URLCategoryID SMALLINT NOT NULL,
            URLRegionID INT NOT NULL,
            ResolutionWidth SMALLINT NOT NULL,
            ResolutionHeight SMALLINT NOT NULL,
            ResolutionDepth SMALLINT NOT NULL,
            FlashMajor SMALLINT NOT NULL,
            FlashMinor SMALLINT NOT NULL,
            FlashMinor2 STRING NOT NULL,
            NetMajor SMALLINT NOT NULL,
            NetMinor SMALLINT NOT NULL,
            UserAgentMajor SMALLINT NOT NULL,
            UserAgentMinor VARCHAR(255) NOT NULL,
            CookieEnable SMALLINT NOT NULL,
            JavascriptEnable SMALLINT NOT NULL,
            IsMobile SMALLINT NOT NULL,
            MobilePhone SMALLINT NOT NULL,
            MobilePhoneModel STRING NOT NULL,
            Params STRING NOT NULL,
            IPNetworkID INT NOT NULL,
            TraficSourceID SMALLINT NOT NULL,
            SearchEngineID SMALLINT NOT NULL,
            SearchPhrase STRING NOT NULL,
            AdvEngineID SMALLINT NOT NULL,
            IsArtifical SMALLINT NOT NULL,
            WindowClientWidth SMALLINT NOT NULL,
            WindowClientHeight SMALLINT NOT NULL,
            ClientTimeZone SMALLINT NOT NULL,
            ClientEventTime INT NOT NULL,
            SilverlightVersion1 SMALLINT NOT NULL,
            SilverlightVersion2 SMALLINT NOT NULL,
            SilverlightVersion3 INT NOT NULL,
            SilverlightVersion4 SMALLINT NOT NULL,
            PageCharset STRING NOT NULL,
            CodeVersion INT NOT NULL,
            IsLink SMALLINT NOT NULL,
            IsDownload SMALLINT NOT NULL,
            IsNotBounce SMALLINT NOT NULL,
            FUniqID BIGINT NOT NULL,
            OriginalURL STRING NOT NULL,
            HID INT NOT NULL,
            IsOldCounter SMALLINT NOT NULL,
            IsEvent SMALLINT NOT NULL,
            IsParameter SMALLINT NOT NULL,
            DontCountHits SMALLINT NOT NULL,
            WithHash SMALLINT NOT NULL,
            HitColor CHAR NOT NULL,
            LocalEventTime INT NOT NULL,
            Age SMALLINT NOT NULL,
            Sex SMALLINT NOT NULL,
            Income SMALLINT NOT NULL,
            Interests SMALLINT NOT NULL,
            Robotness SMALLINT NOT NULL,
            RemoteIP INT NOT NULL,
            WindowName INT NOT NULL,
            OpenerName INT NOT NULL,
            HistoryLength SMALLINT NOT NULL,
            BrowserLanguage STRING NOT NULL,
            BrowserCountry STRING NOT NULL,
            SocialNetwork STRING NOT NULL,
            SocialAction STRING NOT NULL,
            HTTPError SMALLINT NOT NULL,
            SendTiming INT NOT NULL,
            DNSTiming INT NOT NULL,
            ConnectTiming INT NOT NULL,
            ResponseStartTiming INT NOT NULL,
            ResponseEndTiming INT NOT NULL,
            FetchTiming INT NOT NULL,
            SocialSourceNetworkID SMALLINT NOT NULL,
            SocialSourcePage STRING NOT NULL,
            ParamPrice BIGINT NOT NULL,
            ParamOrderID STRING NOT NULL,
            ParamCurrency STRING NOT NULL,
            ParamCurrencyID SMALLINT NOT NULL,
            OpenstatServiceName STRING NOT NULL,
            OpenstatCampaignID STRING NOT NULL,
            OpenstatAdID STRING NOT NULL,
            OpenstatSourceID STRING NOT NULL,
            UTMSource STRING NOT NULL,
            UTMMedium STRING NOT NULL,
            UTMCampaign STRING NOT NULL,
            UTMContent STRING NOT NULL,
            UTMTerm STRING NOT NULL,
            FromTag STRING NOT NULL,
            HasGCLID SMALLINT NOT NULL,
            RefererHash BIGINT NOT NULL,
            URLHash BIGINT NOT NULL,
            CLID INT NOT NULL
        )
        DUPLICATE KEY (CounterID, EventDate, UserID, EventTime, WatchID) 
        DISTRIBUTED BY HASH(UserID) BUCKETS 16
        PROPERTIES (\"replication_num\"=\"1\");
        "
        ${clt} -D"${DB}" -e"${ddl}"
        echo "#### load data"
        stream_load_parquet_size=0
        stream_load_parquet_time=0
        if [[ ! -d "${data_home}" ]]; then mkdir -p "${data_home}"; fi
        declare -A file_sizes=(['hits_0.parquet']=122446530 ['hits_1.parquet']=174965044 ['hits_2.parquet']=230595491 ['hits_3.parquet']=192507052 ['hits_4.parquet']=140929275)
        for file_name in ${!file_sizes[*]}; do
            size_expect=${file_sizes[${file_name}]}
            if [[ ! -f "${data_home}/${file_name}" ]] || [[ $(wc -c "${data_home}/${file_name}" | awk '{print $1}') != "${size_expect}" ]]; then
                cd "${data_home}" && rm -f "${file_name}" && wget --continue "https://doris-build-1308700295.cos.ap-beijing.myqcloud.com/ClickBench/${file_name}" && cd - || exit
            fi
            if ret=$(
                curl --location-trusted \
                    -u root: \
                    -T "${data_home}/${file_name}" \
                    -H "format:parquet" \
                    -H "label:${file_name//./_}" \
                    -H "columns: watchid,javaenable,title,goodevent,eventtime,eventdate,counterid,clientip,regionid,userid,counterclass,os,useragent,url,referer,isrefresh,referercategoryid,refererregionid,urlcategoryid,urlregionid,resolutionwidth,resolutionheight,resolutiondepth,flashmajor,flashminor,flashminor2,netmajor,netminor,useragentmajor,useragentminor,cookieenable,javascriptenable,ismobile,mobilephone,mobilephonemodel,params,ipnetworkid,traficsourceid,searchengineid,searchphrase,advengineid,isartifical,windowclientwidth,windowclientheight,clienttimezone,clienteventtime,silverlightversion1,silverlightversion2,silverlightversion3,silverlightversion4,pagecharset,codeversion,islink,isdownload,isnotbounce,funiqid,originalurl,hid,isoldcounter,isevent,isparameter,dontcounthits,withhash,hitcolor,localeventtime,age,sex,income,interests,robotness,remoteip,windowname,openername,historylength,browserlanguage,browsercountry,socialnetwork,socialaction,httperror,sendtiming,dnstiming,connecttiming,responsestarttiming,responseendtiming,fetchtiming,socialsourcenetworkid,socialsourcepage,paramprice,paramorderid,paramcurrency,paramcurrencyid,openstatservicename,openstatcampaignid,openstatadid,openstatsourceid,utmsource,utmmedium,utmcampaign,utmcontent,utmterm,fromtag,hasgclid,refererhash,urlhash,clid" \
                    "http://${FE_HOST:-127.0.0.1}:${http_port}/api/${DB}/hits_parquet/_stream_load"
            ); then
                _stream_load_parquet_size=$(echo "${ret}" | jq '.LoadBytes')
                _stream_load_parquet_time=$(printf "%.0f" "$(echo "scale=1;$(echo "${ret}" | jq '.LoadTimeMs')/1000" | bc)")
                stream_load_parquet_size=$((stream_load_parquet_size + _stream_load_parquet_size))
                stream_load_parquet_time=$((stream_load_parquet_time + _stream_load_parquet_time))
            fi
        done
        sleep 5
        if [[ $(${clt} -D"${DB}" -e"select count(*) from hits_parquet" | sed -n '2p') != 5000000 ]]; then echo "check load fail..." && return 1; fi

        echo "#### record load test result"
        stream_load_parquet_speed=$(echo "${stream_load_parquet_size} / 1024 / 1024/ ${stream_load_parquet_time}" | bc)
        export stream_load_parquet_size
        export stream_load_parquet_time
        export stream_load_parquet_speed
    }

    insert_into_select() {
        echo "#### create table"
        ddl="
        CREATE TABLE IF NOT EXISTS  hits_insert_into_select (
            CounterID INT NOT NULL, 
            EventDate INT NOT NULL, 
            UserID BIGINT NOT NULL, 
            EventTime INT NOT NULL, 
            WatchID BIGINT NOT NULL, 
            JavaEnable SMALLINT NOT NULL,
            Title STRING NOT NULL,
            GoodEvent SMALLINT NOT NULL,
            ClientIP INT NOT NULL,
            RegionID INT NOT NULL,
            CounterClass SMALLINT NOT NULL,
            OS SMALLINT NOT NULL,
            UserAgent SMALLINT NOT NULL,
            URL STRING NOT NULL,
            Referer STRING NOT NULL,
            IsRefresh SMALLINT NOT NULL,
            RefererCategoryID SMALLINT NOT NULL,
            RefererRegionID INT NOT NULL,
            URLCategoryID SMALLINT NOT NULL,
            URLRegionID INT NOT NULL,
            ResolutionWidth SMALLINT NOT NULL,
            ResolutionHeight SMALLINT NOT NULL,
            ResolutionDepth SMALLINT NOT NULL,
            FlashMajor SMALLINT NOT NULL,
            FlashMinor SMALLINT NOT NULL,
            FlashMinor2 STRING NOT NULL,
            NetMajor SMALLINT NOT NULL,
            NetMinor SMALLINT NOT NULL,
            UserAgentMajor SMALLINT NOT NULL,
            UserAgentMinor VARCHAR(255) NOT NULL,
            CookieEnable SMALLINT NOT NULL,
            JavascriptEnable SMALLINT NOT NULL,
            IsMobile SMALLINT NOT NULL,
            MobilePhone SMALLINT NOT NULL,
            MobilePhoneModel STRING NOT NULL,
            Params STRING NOT NULL,
            IPNetworkID INT NOT NULL,
            TraficSourceID SMALLINT NOT NULL,
            SearchEngineID SMALLINT NOT NULL,
            SearchPhrase STRING NOT NULL,
            AdvEngineID SMALLINT NOT NULL,
            IsArtifical SMALLINT NOT NULL,
            WindowClientWidth SMALLINT NOT NULL,
            WindowClientHeight SMALLINT NOT NULL,
            ClientTimeZone SMALLINT NOT NULL,
            ClientEventTime INT NOT NULL,
            SilverlightVersion1 SMALLINT NOT NULL,
            SilverlightVersion2 SMALLINT NOT NULL,
            SilverlightVersion3 INT NOT NULL,
            SilverlightVersion4 SMALLINT NOT NULL,
            PageCharset STRING NOT NULL,
            CodeVersion INT NOT NULL,
            IsLink SMALLINT NOT NULL,
            IsDownload SMALLINT NOT NULL,
            IsNotBounce SMALLINT NOT NULL,
            FUniqID BIGINT NOT NULL,
            OriginalURL STRING NOT NULL,
            HID INT NOT NULL,
            IsOldCounter SMALLINT NOT NULL,
            IsEvent SMALLINT NOT NULL,
            IsParameter SMALLINT NOT NULL,
            DontCountHits SMALLINT NOT NULL,
            WithHash SMALLINT NOT NULL,
            HitColor CHAR NOT NULL,
            LocalEventTime INT NOT NULL,
            Age SMALLINT NOT NULL,
            Sex SMALLINT NOT NULL,
            Income SMALLINT NOT NULL,
            Interests SMALLINT NOT NULL,
            Robotness SMALLINT NOT NULL,
            RemoteIP INT NOT NULL,
            WindowName INT NOT NULL,
            OpenerName INT NOT NULL,
            HistoryLength SMALLINT NOT NULL,
            BrowserLanguage STRING NOT NULL,
            BrowserCountry STRING NOT NULL,
            SocialNetwork STRING NOT NULL,
            SocialAction STRING NOT NULL,
            HTTPError SMALLINT NOT NULL,
            SendTiming INT NOT NULL,
            DNSTiming INT NOT NULL,
            ConnectTiming INT NOT NULL,
            ResponseStartTiming INT NOT NULL,
            ResponseEndTiming INT NOT NULL,
            FetchTiming INT NOT NULL,
            SocialSourceNetworkID SMALLINT NOT NULL,
            SocialSourcePage STRING NOT NULL,
            ParamPrice BIGINT NOT NULL,
            ParamOrderID STRING NOT NULL,
            ParamCurrency STRING NOT NULL,
            ParamCurrencyID SMALLINT NOT NULL,
            OpenstatServiceName STRING NOT NULL,
            OpenstatCampaignID STRING NOT NULL,
            OpenstatAdID STRING NOT NULL,
            OpenstatSourceID STRING NOT NULL,
            UTMSource STRING NOT NULL,
            UTMMedium STRING NOT NULL,
            UTMCampaign STRING NOT NULL,
            UTMContent STRING NOT NULL,
            UTMTerm STRING NOT NULL,
            FromTag STRING NOT NULL,
            HasGCLID SMALLINT NOT NULL,
            RefererHash BIGINT NOT NULL,
            URLHash BIGINT NOT NULL,
            CLID INT NOT NULL
        )
        DUPLICATE KEY (CounterID, EventDate, UserID, EventTime, WatchID) 
        DISTRIBUTED BY HASH(UserID) BUCKETS 16
        PROPERTIES (\"replication_num\"=\"1\");
        "
        ${clt} -D"${DB}" -e"${ddl}"

        echo "#### load data by INSERT INTO SELECT"
        insert_into_select_time=0
        insert_into_select_rows=10000000
        start=$(date +%s%3N)
        if ${clt} -e"insert into ${DB}.hits_insert_into_select select * from clickbench.hits limit ${insert_into_select_rows};"; then
            end=$(date +%s%3N)
            insert_into_select_time=$(echo "scale=1; (${end} - ${start})/1000" | bc)
        else
            echo "ERROR: failed to insert into ${DB}.hits_insert_into_select select * from clickbench.hits limit ${insert_into_select_rows};"
            return 1
        fi
        sleep 5
        if [[ $(${clt} -D"${DB}" -e"select count(*) from hits_insert_into_select" | sed -n '2p') != "${insert_into_select_rows}" ]]; then echo "check load fail..." && return 1; fi

        echo "#### record load test result"
        insert_into_select_speed=$(echo "${insert_into_select_rows} / 1000 / ${insert_into_select_time}" | bc)
        export insert_into_select_rows
        export insert_into_select_time
        export insert_into_select_speed
    }

    echo "#### 1. Restart doris"
    if ! restart_doris; then echo "ERROR: Restart doris failed" && exit 1; fi

    echo "#### 3. run streamload test"
    set_session_variable runtime_filter_mode global
    ${clt} -e "DROP DATABASE IF EXISTS ${DB}" && sleep 1
    ${clt} -e "CREATE DATABASE IF NOT EXISTS ${DB}" && sleep 5
    if ! stream_load_json; then exit 1; fi
    if ! stream_load_orc; then exit 1; fi
    if ! stream_load_parquet; then exit 1; fi
    if ! insert_into_select; then exit 1; fi
    if ! check_load_performance; then exit 1; fi

    echo "#### 4. comment result on tpch"
    comment_body="Load test result on commit ${commit_id:-} with default conf and session variables"
    if [[ -n ${stream_load_json_time} ]]; then comment_body="${comment_body}\n stream load json:         ${stream_load_json_time} seconds loaded ${stream_load_json_size} Bytes, about ${stream_load_json_speed} MB/s"; fi
    if [[ -n ${stream_load_orc_time} ]]; then comment_body="${comment_body}\n stream load orc:          ${stream_load_orc_time} seconds loaded ${stream_load_orc_size} Bytes, about ${stream_load_orc_speed} MB/s"; fi
    if [[ -n ${stream_load_parquet_time} ]]; then comment_body="${comment_body}\n stream load parquet:          ${stream_load_parquet_time} seconds loaded ${stream_load_parquet_size} Bytes, about ${stream_load_parquet_speed} MB/s"; fi
    if [[ -n ${insert_into_select_time} ]]; then comment_body="${comment_body}\n insert into select:          ${insert_into_select_time} seconds inserted ${insert_into_select_rows} Rows, about ${insert_into_select_speed}K ops/s"; fi

    comment_body=$(echo "${comment_body}" | sed -e ':a;N;$!ba;s/\t/\\t/g;s/\n/\\n/g') # 将所有的 Tab字符替换为\t 换行符替换为\n
    create_an_issue_comment_tpch "${pull_request_num:-}" "${comment_body}"
)
exit_flag="$?"

echo "#### 5. check if need backup doris logs"
if [[ ${exit_flag} != "0" ]]; then
    print_doris_fe_log
    print_doris_be_log
    if file_name=$(archive_doris_logs "${pull_request_num}_${commit_id}_doris_logs.tar.gz"); then
        upload_doris_log_to_oss "${file_name}"
    fi
fi

exit "${exit_flag}"
