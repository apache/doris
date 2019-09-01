# CANCEL LABEL
## description
    NAME:
        cancel_label: cancel a transaction with label
        
    SYNOPSIS
        curl -u user:passwd -XPOST http://host:port/api/{db}/{label}/_cancel

    DESCRIPTION

        This is to cancel a transaction with specified label.

    RETURN VALUES

        Return a JSON format string:

        Status: 
            Success: cancel succeed
            Others: cancel failed
        Message: Error message if cancel failed
           
    ERRORS
    
## example

    1. Cancel the transaction with label "testLabel" on database "testDb"

        curl -u root -XPOST http://host:port/api/testDb/testLabel/_cancel
 
## keyword

    CANCEL，LABEL






