# DROP REPOSITORY
## Description
This statement is used to delete a created warehouse. Only root or superuser users can delete the warehouse.
Grammar:
DROP REPOSITORY `repo_name`;

Explain:
1. Delete the warehouse, just delete the mapping of the warehouse in Palo, and do not delete the actual warehouse data. After deletion, you can map to the repository again by specifying the same broker and LOCATION.

## example
1. Delete the warehouse named bos_repo:
DROP REPOSITORY `bos_repo`;

## keyword
DROP REPOSITORY
