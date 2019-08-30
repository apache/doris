# ADMIN SET CONFIG
## Description

This statement is used to set the configuration items for the cluster (currently only the configuration items for setting FE are supported).
Settable configuration items can be viewed through AMDIN SHOW FRONTEND CONFIG; commands.

Grammar:

ADMIN SET FRONTEND CONFIG ("key" = "value");

## example

1. "disable balance" true

ADMIN SET FRONTEND CONFIG ("disable_balance" = "true");

## keyword
ADMIN,SET,CONFIG
