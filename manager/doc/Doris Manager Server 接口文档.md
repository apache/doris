[TOC]

---

### DorisManager-Agent Server 接口文档

#### 1.安装Agent

**接口功能**

> 给指定机器安装Agent，并启动。

**URL**

> /api/server/installAgent

**支持格式**

> JSON

**HTTP请求方式**

> POST

**请求参数**

> |参数|必选|类型|说明|
> |:-----  |:-------|:-----|-----                               |
> |clusterId|true|String|集群ID ,新建集群后返回的id|
> |hosts    |true    |List|机器列表                          |
> |user    |true    |String   |ssh 用户|
> |port |true |int |ssh 端口|
> |sshKey |true |String |ssh 私钥|
> |packageUrl |true |String |doris编译包下载地址，只支持http方式。|
> |installDir |true |String |安装路径|

**返回字段**

> |返回字段|字段类型|说明                              |
> |:-----   |:------|:-----------------------------   |
> |msg   |String    |调用信息   |
> |code  |String | 结果状态。0：正常  |
> |data |Int | 当前安装的流程ID |

**接口示例**

> 地址：http://localhost:9601/api/server/installAgent

> 请求参数：
``` json
{
    "clusterId":"1",
    "packageUrl":"https://palo-cloud-repo-bd.bd.bcebos.com/baidu-doris-release/PALO-0.15.1-rc03-no-avx2-binary.tar.gz",
    "installDir":"/usr/local/doris",
    "hosts":["10.10.10.11"],
    "user":"root",
    "sshPort":22,
    "sshKey": "-----BEGIN RSA PRIVATE KEY-----....."
}
```
> 返回参数：
``` json
{
    "msg": "success",
    "code": 0,
    "data":1
}
```



#### 2.查看Agent列表

**接口功能**

> 查看已安装的Agent列表

**URL**

> /api/server/agentList

**支持格式**

> JSON

**HTTP请求方式**

> GET

**请求参数**
> |返回字段|字段类型|说明                              |
> |:-----   |:------|:-----------------------------   |
> |clusterId   |int    |集群id   |

**返回字段**

> |返回字段|字段类型|说明                              |
> |:-----   |:------|:-----------------------------   |
> |msg   |String    |调用信息   |
> |code  |String | 结果状态。0：正常  |
> |data.id  |int | agentId |
> |data.host  |String |agent host  |
> |data.port  |String | agent port |
> |data.status  |String | agent状态 ：RUNNING STOP|
> |data.registerTime  |Date | agent注册时间  |
> |data.lastReportedTime  |Date | agent最后上报时间  |

**接口示例**

> 地址：http://localhost:9601/api/server/agentList

> 请求参数：无

> 返回参数：
``` json
{
    "msg": "success",
    "code": 0,
    "data": [
        {
            "id": 1,
            "host": "10.10.10.11",
            "port": 9602,
            "status": "RUNNING",
            "registerTime": "2021-08-10T11:07:34.000+00:00",
            "lastReportedTime": "2021-08-12T03:02:46.000+00:00"
        }
    ]
}
```

#### 3.安装Doris

**接口功能**

> 给指定机器安装Doris

**URL**

> /api/agent/installService

**支持格式**

> JSON

**HTTP请求方式**

> POST

**请求参数**

> |参数|必选|类型|说明|
> |:-----  |:-------|:-----|-----                               |
> |processId|true|int|当前安装的流程ID，接口1返回的结果|
> |installInfos.host    |ture    | String  |指定安装doris的机器                          |
> |installInfos.role    |true    |String   |doris角色：FE、BE、BROKER|
> |installInfos.feNodeType    |false    |String   |角色为FE时:FOLLOWER / OBSERVER|

**返回字段**


> |返回字段|字段类型|说明                              |
> |:-----   |:------|:-----------------------------   |
> |msg   |String    |调用信息   |
> |code  |String | 结果状态。0：正常  |
> 
**接口示例**

> 地址：http://localhost:9601/api/agent/installService

> 请求参数：
``` json
{
    "processId":1,
    "installInfos":[{
        "host":"10.220.147.155",
        "role":"FE",
        "feNodeType":"FOLLOWER"
    },
    {
        "host":"10.220.147.155",
        "role":"BE"
        
    }]
}
```
> 返回参数：
``` json
{
    "msg": "success",
    "code": 0
}
```

#### 4.分发配置

**接口功能**

> 分发FE、BE的配置

**URL**

> /api/agent/deployConfig

**支持格式**

> JSON

**HTTP请求方式**

> POST

**请求参数**

> |参数|必选|类型|说明|
> |:-----  |:-------|:-----|-----                               |
> |processId|true|int|当前安装的流程ID，接口1返回的结果|
> |deployConfigs.hosts |true |List<String> |指定的机器列表|
> |deployConfigs.role    |true    |String   |doris角色：FE、BE、BROKER|
> |deployConfigs.conf |true |String |配置文件内容|

**返回字段**

> |返回字段|字段类型|说明                              |
> |:-----   |:------|:-----------------------------   |
> |msg   |String    |调用信息   |
> |code  |String | 结果状态。0：正常  |

**接口示例**

> 地址：http://localhost:9601/api/agent/deployConfig

> 请求参数：
``` json
{
    "processId":"1",
    "deployConfigs":[{
        "hosts":["10.220.147.155"],
        "role":"FE",
        "conf":"LOG_DIR = ${DORIS_HOME}/log\nDATE = `date +%Y%m%d-%H%M%S`\nJAVA_OPTS=\"-Xmx4096m -XX:+UseMembar -XX:SurvivorRatio=8 -XX:MaxTenuringThreshold=7 -XX:+PrintGCDateStamps -XX:+PrintGCDetails -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:+CMSClassUnloadingEnabled -XX:-CMSParallelRemarkEnabled -XX:CMSInitiatingOccupancyFraction=80 -XX:SoftRefLRUPolicyMSPerMB=0 -Xloggc:$DORIS_HOME/log/fe.gc.log.$DATE\"\nJAVA_OPTS_FOR_JDK_9=\"-Xmx4096m -XX:SurvivorRatio=8 -XX:MaxTenuringThreshold=7 -XX:+CMSClassUnloadingEnabled -XX:-CMSParallelRemarkEnabled -XX:CMSInitiatingOccupancyFraction=80 -XX:SoftRefLRUPolicyMSPerMB=0 -Xlog:gc*:$DORIS_HOME/log/fe.gc.log.$DATE:time\"\nsys_log_level = INFO\nmeta_dir = /usr/local/doris/fe/doris-meta\nhttp_port = 8030\nrpc_port = 9020\nquery_port = 9030\nedit_log_port = 9010\nmysql_service_nio_enabled = true"
    },{
        "hosts":["10.220.147.155"],
        "role":"BE",
        "conf":"PPROF_TMPDIR=\"$DORIS_HOME/log/\"\nsys_log_level = INFO\nbe_port = 19060\nbe_rpc_port = 19070\nwebserver_port = 18040\nheartbeat_service_port = 19050\nbrpc_port = 18060\nstorage_root_path = /usr/local/doris/be/storage\n"
    }]
}
```
> 返回参数：
``` json
{
    "msg": "success",
    "code": 0
}
```

#### 5.启动Doris

**接口功能**

> 启动指定机器的doris

**URL**

> /api/agent/startService

**支持格式**

> JSON

**HTTP请求方式**

> POST

**请求参数**

> |参数|必选|类型|说明|
> |:-----  |:-------|:-----|-----                               |
> |processId|true|int|当前安装的流程ID，接口1返回的结果|
> |dorisStarts.host    |true    |String   |指定机器|
> |dorisStarts.role |true |String |doris角色：FE、BE、BROKER|

**返回字段**

> |返回字段|字段类型|说明                              |
> |:-----   |:------|:-----------------------------   |
> |msg   |String    |调用信息   |
> |code  |String | 结果状态。0：正常  |

**接口示例**

> 地址：http://localhost:9601/api/agent/startService

> 请求参数：
``` json
{
    "processId":"1",
    "dorisStarts":[{
        "host":"10.220.147.155",
        "role":"FE"
    },{
        "host":"10.220.147.155",
        "role":"BE"
    }]
}
```
> 返回参数：
``` json
{
    "msg": "success",
    "code": 0
}
```

#### 5.启动组件集群

**接口功能**

> 组件集群，执行add fe, add be ,add broker 操作

**URL**

> /api/agent/buildCluster

**支持格式**

> JSON

**HTTP请求方式**

> POST

**请求参数**

> |参数|必选|类型|说明|
> |:-----  |:-------|:-----|-----                               |
> |processId|true|int|当前安装的流程ID，接口1返回的结果|
> |feHosts    |true    |List<String>   |FE列表|
> |beHosts    |true    |List<String>   |BE列表|

**返回字段**

> |返回字段|字段类型|说明                              |
> |:-----   |:------|:-----------------------------   |
> |msg   |String    |调用信息   |
> |code  |String | 结果状态。0：正常  |

**接口示例**

> 地址：http://localhost:9601/api/agent/buildCluster

> 请求参数：
``` json
{
    "processId":1,
    "feHosts":["10.220.147.155"],
    "beHosts":["10.220.147.155"]
}
```
> 返回参数：
``` json
{
    "msg": "success",
    "code": 0
}
```

#### 6.完成安装

**接口功能**

> 完成安装，完成安装后调用

**URL**

> /api/process/installComplete/{processId}

**支持格式**

> JSON

**HTTP请求方式**

> POST

**请求参数**

> |参数|必选|类型|说明|
> |:-----  |:-------|:-----|-----                               |
> |processId|true|int|当前安装的流程ID，接口1返回的结果|


**返回字段**

> |返回字段|字段类型|说明                              |
> |:-----   |:------|:-----------------------------   |
> |msg   |String    |调用信息   |
> |code  |String | 结果状态。0：正常  |

**接口示例**

> 地址：http://localhost:9601/api/process/installComplete/1

> 请求参数：
``` json
{
    "processId":1,
    "hosts":["10.220.147.155"]
}
```
> 返回参数：
``` json
{
    "msg": "success",
    "code": 0
}
```

#### 7.查询该用户当前安装的流程

**接口功能**

> 查询该用户当前安装的流程

**URL**

> /api/process/currentProcess

**支持格式**

> JSON

**HTTP请求方式**

> POST

**请求参数**

**返回字段**

> |返回字段|字段类型|说明                              |
> |:-----   |:------|:-----------------------------   |
> |msg   |String    |调用信息   |
> |code  |String | 结果状态。0：正常  |
> |data.id  |String | 当前安装流程id |
> |data.processType   |String | 当前安装流程类型(进度)：INSTALL_AGENT 安装Agent,INSTALL_SERVICE 安装服务,DEPLOY_CONFIG 分发配置,START_SERVICE 启动服务,BUILD_CLUSTER 组件集群  |
> |data.processStep   |int | 流程步骤:0,1,2,3  |
> |data.finish   |String | 流程完成标志  |
> |data.status   |String | 流程状态  |
> |data.createTime   |Date | 创建时间  |
> |data.updateTime   |Date | 更新时间  |

**接口示例**

> 地址：http://localhost:9601/api/process/currentProcess

> 返回参数：
``` json
{
    "msg": "success",
    "code": 0,
    "data": {
        "id": 1,
        "clusterId": 0,
        "processType": "START_SERVICE",
        "processStep": 0,
        "createTime": "2021-11-02T01:29:58.000+00:00",
        "updateTime": "2021-11-02T01:29:58.000+00:00",
        "finish": "NO"
    }
}
```


#### 8.查看当前流程当前安装任务状态

**接口功能**

> 查看当前流程当前安装任务状态

**URL**

> /api/process/{processId}/currentTasks

**支持格式**

> JSON

**HTTP请求方式**

> POST

**请求参数**

> |参数|必选|类型|说明|
> |:-----  |:-------|:-----|-----                               |
> |processId    |ture    | String  |流程ID，接口1返回                        |

**返回字段**

> |返回字段|字段类型|说明                              |
> |:-----   |:------|:-----------------------------   |
> |msg   |String    |调用信息   |
> |code  |String | 结果状态。0：正常  |
> |data.id |int | 任务ID |
> |data.processId |int | 流程id |
> |data.host |String | 任务运行host |
> |data.processType |String | 当前安装类型(进度)：INSTALL_AGENT 安装Agent,INSTALL_SERVICE 安装服务,DEPLOY_CONFIG 分发配置,START_SERVICE 启动服务,BUILD_CLUSTER 组件集群     |
> |data.taskType |String | 任务类型 |
> |data.status |String | 任务执行状态 |
> |data.startTime |Date | 任务开始时间 |
> |data.endTime |Date | 任务终止时间      |
> |data.finish |int | 任务执行完成标志 |
> |data.taskRole |String | 任务所属角色 FE BE BROKER，安装agent和组件集群为空 |
> |data.response |String | 任务执行状态信息 |


**接口示例**

> 地址：http://localhost:9601/api/process/1/currentTasks

> 请求参数：
无
> 返回参数：
``` json
{
    "msg": "success",
    "code": 0,
    "data": [
        {
            "id": 6,
            "processId": 1,
            "host": "10.220.147.155",
            "processType": "START_SERVICE",
            "taskType": "START_FE",
            "status": "SUCCESS",
            "startTime": "2021-11-02T01:33:08.000+00:00",
            "endTime": "2021-11-02T01:33:20.000+00:00",
            "executorId": "377bb55156774cb7a72804fbba207e94",
            "result": null,
            "finish": "YES",
            "taskRole": "FE",
            "response": "install fe success"
        },
        {
            "id": 7,
            "processId": 1,
            "host": "10.220.147.155",
            "processType": "START_SERVICE",
            "taskType": "START_BE",
            "status": "SUCCESS",
            "startTime": "2021-11-02T01:33:08.000+00:00",
            "endTime": "2021-11-02T01:33:20.000+00:00",
            "executorId": "f052ba23ad9d4428900d328a7979d7a2",
            "result": null,
            "finish": "YES",
            "taskRole": "BE",
            "response": "install be success"
        }
    ]
}
```

#### 9.查看任务执行信息

**接口功能**

> 查看任务执行信息

**URL**

> /api/process/task/info/{taskId}

**支持格式**

> JSON

**HTTP请求方式**

> GET

**请求参数**

> |参数|必选|类型|说明|
> |:-----  |:-------|:-----|-----                               |
> |taskId    |true    |String   |任务id|

**返回字段**

> |返回字段|字段类型|说明                              |
> |:-----   |:------|:-----------------------------   |
> |msg   |String    |调用信息   |
> |code  |String | 结果状态。0：正常  |
> |data.id |int | 任务ID |
> |data.processId |int | 任务所属流程ID |
> |data.host |Date | 任务执行host |
> |data.processType |String | 安装类型      |
> |data.taskType |String | 任务类型。  |
> |data.status |int | 任务返回状态。 SUBMITTED 已提交,RUNNING 运行中,SUCCESS 成功,FAILURE 失败 |
> |data.startTime |Date | 任务开始时间 |
> |data.endTime |Date | 任务结束时间 |
> |data.result |String | 任务返回结果 |
> |data.finish |String | 任务是否执行成功。YES NO|

**接口示例**

> 地址：http://localhost:9601/api/process/task/info/9

```
> 返回参数：
​``` json
{
    "msg": "success",
    "code": 0,
    "data": {
        "id": 1,
        "processId": 1,
        "host": "10.220.147.155",
        "processType": "INSTALL_AGENT",
        "taskType": "INSTALL_AGENT",
        "status": "SUCCESS",
        "startTime": "2021-11-05T10:33:23.000+00:00",
        "endTime": "2021-11-05T10:33:26.000+00:00",
        "executorId": null,
        "result": "SUCCESS",
        "finish": "YES"
    }
}
```


#### 10.查看任务执行日志

**接口功能**

> 查看任务最近1MB日志

**URL**

> /api/process/task/log/{taskId}

**支持格式**

> JSON

**HTTP请求方式**

> GET

**请求参数**

> |参数|必选|类型|说明|
> |:-----  |:-------|:-----|-----                               |
> |taskId    |true    |String   |任务id|

**返回字段**

> |返回字段|字段类型|说明                              |
> |:-----   |:------|:-----------------------------   |
> |msg   |String    |调用信息   |
> |code  |String | 结果状态。0：正常  |
> |data.logPath |String | 任务日志路径 |
> |data.log |String | 日志内容 |

**接口示例**

> 地址：http://localhost:9601/api/process/task/log/9

```
> 返回参数：
​``` json
{
    "msg": "success",
    "code": 0,
    "data": {
        "logPath": "/usr/local/doris/agent/log/task.log",
        "log": "2021-11-05 19:01:38.434 [pool-2-thread-1] INFO  TASK_LOG - scriptCmd:/bin/sh /usr/local/doris/agent/bin/install_fe.sh  --installDir /usr"
    }
}
```

#### 11.查看Agent上安装的角色列表

**接口功能**

> 查看Agent上安装的角色列表

**URL**

> /api/server/roleList

**支持格式**

> JSON

**HTTP请求方式**

> GET

**请求参数**
> |返回字段|字段类型|说明                              |
> |:-----   |:------|:-----------------------------   |
> |clusterId   |int    |集群id   |

**返回字段**

> |返回字段|字段类型|说明                              |
> |:-----   |:------|:-----------------------------   |
> |msg   |String    |调用信息   |
> |code  |String | 结果状态。0：正常  |
> |data.host  |String |agent host  |
> |data.role  |String |安装角色 FE BE BROKER |
> |data.feNodeType  |String | 角色类型 FOLLOWer OBserver|
> |data.register  |String | 安装后是否注册成功 |

**接口示例**

> 地址：http://localhost:9601/api/server/agentList

> 请求参数：无

> 返回参数：
``` json
{
    "msg": "success",
    "code": 0,
    "data": [
        {
            "id": 1,
            "host": "10.220.147.155",
            "clusterId": 0,
            "role": "FE",
            "feNodeType": null,
            "installDir": "/usr/local/doris/fe",
            "register": "YES"
        },
        {
            "id": 2,
            "host": "10.220.147.155",
            "clusterId": 0,
            "role": "BE",
            "feNodeType": null,
            "installDir": "/usr/local/doris/be",
            "register": "YES"
        }
    ]
}
```


#### 13.重试任务

**接口功能**

> 任务执行失败时，重试任务

**URL**

> /api/process/task/retry/{taskId}

**支持格式**

> JSON

**HTTP请求方式**

> POST

**请求参数**
无

**返回字段**

> |返回字段|字段类型|说明                              |
> |:-----   |:------|:-----------------------------   |
> |msg   |String    |调用信息   |
> |code  |String | 结果状态。0：正常  |

**接口示例**

> 地址：http://localhost:9601/api/process/task/retry/1

> 请求参数：无

> 返回参数：
``` json
{
    "msg": "success",
    "code": 0
}
```

#### 14.跳过任务

**接口功能**

> 任务执行失败时跳过任务，跳过任务

**URL**

> /api/process/task/skip/{taskId}

**支持格式**

> JSON

**HTTP请求方式**

> POST

**请求参数**
无

**返回字段**

> |返回字段|字段类型|说明                              |
> |:-----   |:------|:-----------------------------   |
> |msg   |String    |调用信息   |
> |code  |String | 结果状态。0：正常  |

**接口示例**

> 地址：http://localhost:9601/api/process/task/skip/1

> 请求参数：无

> 返回参数：
``` json
{
    "msg": "success",
    "code": 0
}
```


#### 15.查看节点硬件信息

**接口功能**

> 查看节点硬件信息

**URL**

> /api/agent/hardware/{clusterId}

**支持格式**

> JSON

**HTTP请求方式**

> GET

**请求参数**
无

**返回字段**

> |返回字段|字段类型|说明                              |
> |:-----   |:------|:-----------------------------   |
> |msg   |String    |调用信息   |
> |code  |String | 结果状态。0：正常  |
> |data.cpu  |String | cpu  |
> |data.totalMemory  |String | 总内存  |

**接口示例**

> 地址：http://localhost:9601/api/agent/hardware/1

> 请求参数：无

> 返回参数：
``` json
{
    "msg": "success",
    "code": 0,
    "data": [{
        "cpu": "Intel(R) Xeon(R) Silver 4116 CPU @ 2.10GHz",
        "totalMemory": "15.5 GiB",
        "host": "127.0.0.1"
    }]
}
```


#### 16.返回上一步

**接口功能**

> 返回上一步，只有第四步可以返回上一步，其余均不可返回上一步

**URL**

> /api/process/back/{processId}

**支持格式**

> JSON

**HTTP请求方式**

> POST

**请求参数**
无

**返回字段**

> |返回字段|字段类型|说明                              |
> |:-----   |:------|:-----------------------------   |
> |msg   |String    |调用信息   |
> |code  |String | 结果状态。0：正常  |

**接口示例**

> 地址：http://localhost:9601/api/process/back/1

> 请求参数：无

> 返回参数：
``` json
{
    "msg": "success",
    "code": 0
}
```