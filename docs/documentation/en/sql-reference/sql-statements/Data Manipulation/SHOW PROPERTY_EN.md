# SHOW PROPERTY
## Description
This statement is used to view user attributes
Grammar:
SHOW PROPERTY [FOR user] [LIKE key]

## example
1. View the attributes of the jack user
SHOW PROPERTY FOR 'jack'

2. View Jack user import cluster related properties
SHOW PROPERTY FOR 'jack' LIKE '%load_cluster%'

## keyword
SHOW, PROPERTY

