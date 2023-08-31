#!/bin/bash

### 1. create table
bash ./create-ssb-tables.sh

### 2. load data
bash ./load-ssb-data.sh

### 3. exec ssb queries
bash ./exec-ssb-queries.sh

### 4. exec ssb flat queries
bash ./exec-ssb-flat-queries.sh