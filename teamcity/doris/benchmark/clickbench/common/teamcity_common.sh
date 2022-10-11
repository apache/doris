#!/bin/bash
#Teamcity simple api

zcp_TOKEN="eyJ0eXAiOiAiVENWMiJ9.bW1DcHJpRWNCSktnREhLaHd5Y1lndmpycnJ3.NmEwM2E3YTEtMTVmYy00NGU0LWI1OTAtMzU3MzljODE0Njdk"
#TOKEN="eyJ0eXAiOiAiVENWMiJ9.SmFOcnhYZ1FpdjRyMl9HSUlkQ2M5R2VMZ2FV.NzZjNTlmNDUtOWE4NC00Zjk3LThkMGMtZDZlZTEzODFiYmI0"
TOKEN="eyJ0eXAiOiAiVENWMiJ9.ckhoYmFHUGswVkN0di1fbGFENThnSUN2RVk0.ODhiZjdlNzctZmZiNy00MzNiLWE4ZTQtZWNmNjQ1NDQ2Y2Zj"
zcp_TEAMCITY_SERVER="http://43.129.232.36:8111"
TEAMCITY_SERVER="http://43.132.222.7:8111"
HEADER="--header"
AUTHORIZATION="Authorization: Bearer"
JSON_HEADER="--header \"Accept: application/json\""

usage() {
  echo "
Usage: $0 <options>
  Optional options:
     --cancel_running_build build_id    cancel a specific running build
     --cancel_pending_build build_id    cancel a specific pending build
     --show_build_status build_id       show a specific build status (FAILURE/SUCCESS/UNKOWN)
     --show_build_state build_id        show a specific build state (finished/queued/running)
     --show_latest_builds               show 100 latest builds
     --show_queued_builds               show all queued builds
     --get_all_builds_of_pr_id pr_id    get all pr builds

  Eg.
    $0                                  teamcity api
    $0 --cancel_running_build 303       cancel 303 running build
    $0 --cancel_pending_build 304       cancel 304 pending build
    $0 --show_build_status 305          show 305 build status
    $0 --show_build_state 306           show 306 build state
    $0 --show_latest_builds             show 100 latest builds
    $0 --show_queued_builds             show all queued builds
    $0 --get_all_builds_of_pr_id 9151   get all 9151 builds
  "
  exit 1
}

cancel_running_build() {
    build_id=$1
    state=$(show_build_state $build_id)
    if [[ $state == "running" ]];then
        #curl --header "Authorization: Bearer eyJ0eXAiOiAiVENWMiJ9.bW1DcHJpRWNCSktnREhLaHd5Y1lndmpycnJ3.NmEwM2E3YTEtMTVmYy00NGU0LWI1OTAtMzU3MzljODE0Njdk" http://43.129.232.36:8111/app/rest/builds/id:333 -X POST -H 'Content-Type: application/json' -d '{ "buildCancelRequest": {       "comment": "Already running builds will be stopped.",      "readdIntoQueue": "false"    }  }'
        cmd="/app/rest/builds/id:$build_id"
        cmd=$cmd" -X POST -H \'Content-Type: application/json\' -d \'{ \"buildCancelRequest\": {       \"comment\": \"Stop obsolete running builds.\",      \"readdIntoQueue\": \"false\"    }  }'"
        url="curl -s $HEADER \"$AUTHORIZATION $TOKEN\" $TEAMCITY_SERVER/$cmd"
        res=$(eval $eval)
        check=$(echo $res |grep status="UNKNOWN")
        if [[ -z $check ]];then
           echo "cancel fail!"
        fi
    fi

}

cancel_pending_build() {
    build_id=$1
    state=$(show_build_state $build_id)
    if [[ $state == "queued" ]];then
        cmd="app/rest/buildQueue/id:$build_id"
        cmd=$cmd" -X POST -H \'Content-Type: application/json\' -d \'{ \"buildCancelRequest\": {       \"comment\": \"Cancel obsolete queue build.\",      \"readdIntoQueue\": \"false\"    }  }'"
        url="curl -s $HEADER \"$AUTHORIZATION $TOKEN\" $TEAMCITY_SERVER/$cmd"
        #echo $url
        res=$(eval $eval)
        check=$(echo $res |grep status="UNKNOWN")
        if [[ -z $check ]];then
           echo "cancel fail!"
        fi
    fi
}

show_build_state() {
    build_id=$1
    cmd="app/rest/builds?locator=id:$build_id"
    url="curl -s $HEADER \"$AUTHORIZATION $TOKEN\" $TEAMCITY_SERVER/$cmd"
    #echo $url
    res=`eval $url`
    
    #check build exist or not
    check=$(echo $res|grep "<builds count=\"1\"")
    if [[ -z $check ]]; then
        return
    fi
    #get state
    state=`echo $res |awk -F ' ' '{print $9" "$10" "$11}'|awk -F "state=" '{print $2}'|cut -d ' ' -f 1|sed 's/\"//g'`
    #only 2 state: queued and finished
    echo $state
}

show_build_status() {
    build_id=$1
    cmd="app/rest/builds?locator=id:$build_id"
    url="curl -s $HEADER \"$AUTHORIZATION $TOKEN\" $TEAMCITY_SERVER/$cmd"
    #echo $url
    res=`eval $url`
    #check build exist or not
    check1=$(echo $res|grep "<builds count=\"1\"")
    if [[ -z $check1 ]]; then
        return
    fi
    #check is pending build
    #check2=$(echo $res|grep "state=\"queued\"")
    #if [[ -z $check ]]; then
    #    return
    #fi
    state=`echo $res |awk -F ' ' '{print $9" "$10" "$11}'|awk -F "status=" '{print $2}'|cut -d ' ' -f 1|sed 's/\"//g'`
    #only 2 state: 
    echo $state
}

get_all_builds() {
    branch=$1
    cmd="app/rest/builds?locator=branch:$branch"
    url="curl -s $HEADER \"$AUTHORIZATION $TOKEN\" $TEAMCITY_SERVER/$cmd"
    res=`eval $url`
}

get_latest_builds() {
    cmd="app/rest/builds"
    url="curl -s $HEADER \"$AUTHORIZATION $TOKEN\" $TEAMCITY_SERVER/$cmd"
    #echo $url
    res=eval $url
}

get_queued_builds() {
    cmd="app/rest/buildQueue"
    url="curl -s $HEADER \"$AUTHORIZATION $TOKEN\" $JSON_HEADER $TEAMCITY_SERVER/$cmd"
    #echo $url
    res=`eval $url`
    #a=$(echo $res|grep -oP 'build id=.*'|head -1)
    
    
    #a=$res
    #while [[ "check"$a != "check" ]]
    #do
    #    a=$(echo $a|awk -F 'build id="' '{print $2}')
    #done
}

