35; Cancel Label
Description
NAME:
cancel_label: cancel a transaction with label

SYNOPSIS
curl -u user:passwd -XPOST http://host:port/api/{db}/{label}/_cancel

DESCRIPTION
This command is used to cancel a transaction corresponding to a specified Label, which can be successfully cancelled during the Prepare phase.

RETURN VALUES
When the execution is complete, the relevant content of this import will be returned in Json format. Currently includes the following fields
Status: Successful cancel
Success: 成功cancel事务
20854; 2018282: 22833; 361333;
Message: Specific Failure Information

ERRORS

'35;'35; example

1. cancel testDb, testLabel20316;- 19994;
curl -u root -XPOST http://host:port/api/testDb/testLabel/_cancel

## keyword
Cancel, Rabel
