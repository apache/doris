# ADMIN SHOW CONFIG
## Description

This statement is used to show the configuration of the current cluster (currently only supporting the display of FE configuration items)

Grammar:

ADMIN SHOW FRONTEND CONFIG;

Explain:

The implications of the results are as follows:
1. Key: Configuration item name
2. Value: Configuration item value
3. Type: Configuration item type
4. IsMutable：  是否可以通过 ADMIN SET CONFIG 命令设置
5. MasterOnly： 是否仅适用于 Master FE
6. Comment: Configuration Item Description

## example

1. View the configuration of the current FE node

ADMIN SHOW FRONTEND CONFIG;

## keyword
ADMIN,SHOW,CONFIG
