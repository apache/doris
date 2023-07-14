#!/bin/bash

# specified sevices: ali,hw,tx
export SERVICE=ali
# doris host
export HOST=127.0.0.1
# doris uer
export USER=root
# doris mysql cli port
export PORT=9030

# prepare endpoint,region,ak/sk
if [[ ${SERVICE} == 'ali' ]]; then
  export CASE=ping
  export AK=ak
  export SK=sk
  export ENDPOINT=oss-cn-beijing-internal.aliyuncs.com
  export REGION=oss-cn-beijing
elif [[ ${SERVICE} == 'hw' ]]; then 
  export CASE=ping
  export AK=ak
  export SK=sk
  export ENDPOINT=obs.cn-north-4.myhuaweicloud.com
  export REGION=cn-north-4
elif [[ ${SERVICE} == 'tx' ]]; then 
  export CASE=ping
  export AK=ak
  export SK=sk
  export ENDPOINT=cos.ap-beijing.mycloud.com
  export REGION=ap-beijing
fi




