# GET LABEL STATE
## description
    NAME:
        get_label_state: get label's state
        
    SYNOPSIS
        curl -u user:passwd http://host:port/api/{db}/{label}/_state

    DESCRIPTION

        Check the status of a transaction
        
    RETURN VALUES

        Return of JSON format string of the status of specified transaction:
        执行完毕后，会以Json格式返回这次导入的相关内容。当前包括一下字段
        Label: The specified label.
        Status: Success or not of this request.
        Message: Error messages
        State: 
           UNKNOWN/PREPARE/COMMITTED/VISIBLE/ABORTED
        
    ERRORS
    
## example

    1. Get status of label "testLabel" on database "testDb"

        curl -u root http://host:port/api/testDb/testLabel/_state
 
## keyword

    GET, LABEL, STATE

