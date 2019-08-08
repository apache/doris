# RESTORE TABLET
## description
   
    该功能用于恢复trash目录中被误删的tablet数据。

    说明：这个功能暂时只在be服务中提供一个http接口。如果要使用，
    需要向要进行数据恢复的那台be机器的http端口发送restore tablet api请求。api格式如下：
    METHOD: POST
    URI: http://be_host:be_http_port/api/restore_tablet?tablet_id=xxx&schema_hash=xxx

## example

    curl -X POST "http://hostname:8088/api/restore_tablet?tablet_id=123456\&schema_hash=1111111"

##keyword

    RESTORE,TABLET,RESTORE,TABLET
