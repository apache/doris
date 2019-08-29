# GET LABEL STATE
Description
NAME:
get_label_state: get label's state

SYNOPSIS
curl -u user:passwd http://host:port /api /{db}/{label}// u state

DESCRIPTION
This command is used to view the transaction status of a Label

RETURN VALUES
After execution, the relevant content of this import will be returned in Json format. Currently includes the following fields
Label: The imported label, if not specified, is a uuid.
Status: Whether this command was successfully executed or not, Success indicates successful execution
Message: Specific execution information
State: It only makes sense if Status is Success
UNKNOWN: No corresponding Label was found
PREPARE: The corresponding transaction has been prepared, but not yet committed
COMMITTED: The transaction has been committed and cannot be canceled
VISIBLE: Transaction submission, and data visible, cannot be canceled
ABORTED: The transaction has been ROLLBACK and the import has failed.

ERRORS

'35;'35; example

1. Obtain the state of testDb, testLabel
curl -u root http://host:port /api /testDb /testLabel / u state

## keyword
GET, LABEL, STATE
