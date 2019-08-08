# CANCEL LABEL
## description
    NAME:
        cancel_label: cancel a transaction with label
        
    SYNOPSIS
        curl -u user:passwd -XPOST http://host:port/api/{db}/{label}/_cancel

    DESCRIPTION
        该命令用于cancel一个指定Label对应的事务，事务在Prepare阶段能够被成功cancel

    RETURN VALUES
        执行完成后，会以Json格式返回这次导入的相关内容。当前包括一下字段
        Status: 是否成功cancel
            Success: 成功cancel事务
            其他: cancel失败
        Message: 具体的失败信息
           
    ERRORS
    
## example

    1. cancel testDb, testLabel的作业
        curl -u root -XPOST http://host:port/api/testDb/testLabel/_cancel
 
## keyword
    CANCEL，LABEL






