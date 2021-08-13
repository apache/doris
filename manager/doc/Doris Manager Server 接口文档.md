[TOC]

---

### Doris Manager Server 接口文档

#### 1.安装Agent

**接口功能**

> 给指定机器安装Agent，并启动。

**URL**

> /server/installAgent

**支持格式**

> JSON

**HTTP请求方式**

> POST

**请求参数**

> |参数|必选|类型|说明|
> |:-----  |:-------|:-----|-----                               |
> |hosts    |ture    |List|机器列表                          |
> |user    |true    |String   |ssh 用户|
> |port |true |int |ssh 端口|
> |sshKey |true |String |ssh 私钥|

**返回字段**

> |返回字段|字段类型|说明                              |
|:-----   |:------|:-----------------------------   |
|msg   |String    |调用信息   |
|code  |String | 结果状态。0：正常  |

**接口示例**

> 地址：http://localhost:9601/server/installAgent

> 请求参数：
``` json
{
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
    "code": 0
}
```

#### 2.查看Agent列表

**接口功能**

> 查看已安装的Agent列表

**URL**

> /server/agentList

**支持格式**

> JSON

**HTTP请求方式**

> POST

**请求参数**

无

**返回字段**

> |返回字段|字段类型|说明                              |
|:-----   |:------|:-----------------------------   |
|msg   |String    |调用信息   |
|code  |String | 结果状态。0：正常  |
|data.id  |int | agentId |
|data.host  |String |agent host  |
|data.port  |String | agent port |
|data.status  |String | agent状态 ：RUNNING STOP|
|data.registerTime  |Date | agent注册时间  |
|data.lastReportedTime  |Date | agent最后上报时间  |

**接口示例**

> 地址：http://localhost:9601/server/agentList

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

> /agent/installDoris

**支持格式**

> JSON

**HTTP请求方式**

> POST

**请求参数**

> |参数|必选|类型|说明|
> |:-----  |:-------|:-----|-----                               |
> |host    |ture    | String  |指定安装doris的机器                          |
> |role    |true    |String   |doris角色：FE、BE|
> |packageUrl |true |String |doris编译包下载地址，只支持http方式。|
> |mkFeMetadir |false |boolean |安装FE时是否新建 元数据目录|
> |mkBeStorageDir |false |boolean |安装BE时是否新建 数据存储目录|
> |installDir |true |String |安装路径|

**返回字段**

> |返回字段|字段类型|说明                              |
> |:-----   |:------|:-----------------------------   |
> |msg   |String    |调用信息   |
> |code  |String | 结果状态。0：正常  |
> |data.taskResult.taskId |String | 任务ID |
> |data.taskResult.submitTime |Date | 任务提交时间 |
> |data.taskResult.startTime |Date | 任务开始时间 |
> |data.taskResult.endTime |Date | 任务终止时间      |
> |data.taskResult.taskState |String | 任务状态 |
> |data.taskResult.retCode |int | 任务返回状态。0：正常 |
> |data.stdlogs |List | 任务输出日志 |
> |data.errlogs |List | 任务错误日志 |

**接口示例**

> 地址：http://localhost:9601/agent/installDoris

> 请求参数：
``` json
{
    "installInfos":[{
        "host":"10.10.10.11",
        "role":"FE",
        "packageUrl":"http://10.10.10.11/fileupload/doris-fe.tar.gz",
        "mkFeMetadir":true,
        "installDir":"/usr/local/doris-fe"
    }]
}
```
> 返回参数：
``` json
{
    "msg": "success",
    "code": 0,
    "data": [
        {
            "taskResult": {
                "taskId": "a3b966cc10794f4c8b9b2c0dc1fa9b26",
                "submitTime": "2021-08-12T02:37:11.757+00:00",
                "startTime": "2021-08-12T02:37:11.759+00:00",
                "endTime": null,
                "taskState": "RUNNING",
                "retCode": null
            },
            "stdlogs": [],
            "errlogs": []
        }
    ]
}
```

#### 4.启停Doris

**接口功能**

> 对指定机器的doris进行启动、停止

**URL**

> /agent/execute

**支持格式**

> JSON

**HTTP请求方式**

> POST

**请求参数**

> |参数|必选|类型|说明|
> |:-----  |:-------|:-----|-----                               |
> |command    |ture    | String  | 执行命令: START STOP                          |
> |dorisExecs.host    |true    |String   |指定的机器列表|
> |dorisExecs.role |true |String |doris角色：FE、BE|

**返回字段**

> |返回字段|字段类型|说明                              |
> |:-----   |:------|:-----------------------------   |
> |msg   |String    |调用信息   |
> |code  |String | 结果状态。0：正常  |
> |data.taskResult.taskId |String | 任务ID |
> |data.taskResult.submitTime |Date | 任务提交时间 |
> |data.taskResult.startTime |Date | 任务开始时间 |
> |data.taskResult.endTime |Date | 任务终止时间      |
> |data.taskResult.taskState |String | 任务状态 |
> |data.taskResult.retCode |int | 任务返回状态。0：正常 |
> |data.stdlogs |List | 任务输出日志 |
> |data.errlogs |List | 任务错误日志 |

**接口示例**

> 地址：http://localhost:9601/agent/execute

> 请求参数：
``` json
{
    "command":"START",
    "dorisExecs":[{
        "host":"10.10.10.11",
        "role":"FE"
    }]
}
```
> 返回参数：
``` json
{
    "msg": "success",
    "code": 0,
    "data": [
        {
            "taskResult": {
                "taskId": "a3b966cc10794f4c8b9b2c0dc1fa9b26",
                "submitTime": "2021-08-12T02:37:11.757+00:00",
                "startTime": "2021-08-12T02:37:11.759+00:00",
                "endTime": null,
                "taskState": "RUNNING",
                "retCode": null
            },
            "stdlogs": [],
            "errlogs": []
        }
    ]
}
```

#### 5.查看任务执行状态

**接口功能**

> 查看任务执行状态

**URL**

> /agent/task

**支持格式**

> JSON

**HTTP请求方式**

> POST

**请求参数**

> |参数|必选|类型|说明|
> |:-----  |:-------|:-----|-----                               |
> |host    |ture    | String  |agent 机器host                          |
> |taskId    |true    |String   |任务id|

**返回字段**

> |返回字段|字段类型|说明                              |
> |:-----   |:------|:-----------------------------   |
> |msg   |String    |调用信息   |
> |code  |String | 结果状态。0：正常  |
> |data.taskResult.taskId |String | 任务ID |
> |data.taskResult.submitTime |Date | 任务提交时间 |
> |data.taskResult.startTime |Date | 任务开始时间 |
> |data.taskResult.endTime |Date | 任务终止时间      |
> |data.taskResult.taskState |String | 任务状态 |
> |data.taskResult.retCode |int | 任务返回状态。0：正常 |
> |data.stdlogs |List | 任务输出日志 |
> |data.errlogs |List | 任务错误日志 |

**接口示例**

> 地址：http://localhost:9601/agent/task

> 请求参数：
``` json
{
    "host":"10.10.10.11",
    "taskId":"a3b966cc10794f4c8b9b2c0dc1fa9b26"
}
```
> 返回参数：
``` json
{
    "msg": "success",
    "code": 0,
    "data": {
        "taskResult": {
            "taskId": "a3b966cc10794f4c8b9b2c0dc1fa9b26",
            "submitTime": "2021-08-12T02:37:11.757+00:00",
            "startTime": "2021-08-12T02:37:11.759+00:00",
            "endTime": "2021-08-12T02:37:14.759+00:00",
            "taskState": "FINISHED",
            "retCode": 0
        },
        "stdlogs": [
            "bin/",
            "bin/start_fe.sh",
            "bin/stop_fe.sh",
            "conf/",
            "conf/fe.conf",
            "lib/",
            "lib/RoaringBitmap-0.8.13.jar",
            "lib/activation-1.1.1.jar",
            "lib/aircompressor-0.10.jar",
            "lib/amqp-client-5.7.3.jar",
            "lib/annotations-2.15.45.jar"
            .....
        ],
        "errlogs": []
    }
}
```

#### 6.查看任务输出日志

**接口功能**

> 查看任务输出日志

**URL**

> /agent/stdlog

**支持格式**

> JSON

**HTTP请求方式**

> POST

**请求参数**

> |参数|必选|类型|说明|
> |:-----  |:-------|:-----|-----                               |
> |host    |ture    | String  |agent 机器host                          |
> |taskId    |true    |String   |任务id|
> |offset    |false    |int   |日志偏移量|

**返回字段**

> |返回字段|字段类型|说明                              |
> |:-----   |:------|:-----------------------------   |
> |msg   |String    |调用信息   |
> |code  |String | 结果状态。0：正常  |
> |data.key |String | 下一页起始偏移量 |
> |data.value |List | 日志 |

**接口示例**

> 地址：http://localhost:9601/agent/stdlog

> 请求参数：
``` json
{
    "host":"10.10.10.11",
    "taskId":"a3b966cc10794f4c8b9b2c0dc1fa9b26",
    "offset":10,
}
```
> 返回参数：
``` json
{
    "msg": "success",
    "code": 0,
    "data": {
        "key": 366,
        "value": [
            "lib/annotations-2.15.45.jar",
            "lib/antlr4-runtime-4.7.jar",
            .....
        ]
    }
}
```

#### 7.查看任务错误日志

**接口功能**

> 查看任务错误日志

**URL**

> /agent/errlog

**支持格式**

> JSON

**HTTP请求方式**

> POST

**请求参数**

> |参数|必选|类型|说明|
> |:-----  |:-------|:-----|-----                               |
> |host    |ture    | String  |agent 机器host                          |
> |taskId    |true    |String   |任务id|
> |offset    |false    |int   |日志偏移量|

**返回字段**

> |返回字段|字段类型|说明                              |
> |:-----   |:------|:-----------------------------   |
> |msg   |String    |调用信息   |
> |code  |String | 结果状态。0：正常  |
> |data.key |String | 下一页起始偏移量 |
> |data.value |List | 日志 |

**接口示例**

> 地址：http://localhost:9601/agent/errlog

> 请求参数：
``` json
{
    "host":"10.10.10.11",
    "taskId":"a3b966cc10794f4c8b9b2c0dc1fa9b26",
    "offset":10,
}
```
> 返回参数：
``` json
{
    "msg": "success",
    "code": 0,
    "data": {
        "key": 366,
        "value": [
            .....
        ]
    }
}
```