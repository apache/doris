# SHOW REPOSITORIES
## Description
This statement is used to view the currently created warehouse.
Grammar:
SHOW REPOSITORIES;

Explain:
1. Each column has the following meanings:
RepoId: Unique Warehouse ID
RepoName: Warehouse name
CreateTime: The first time the warehouse was created
IsReadOnly: Is it a read-only warehouse?
Location: The root directory in the repository for backing up data
Broker: Dependent Broker
ErrMsg: Palo regularly checks the connectivity of the warehouse, and if problems occur, error messages are displayed here.

## example
1. View the warehouse that has been created:
SHOW REPOSITORIES;

## keyword
SHOW, REPOSITORY, REPOSITORIES

