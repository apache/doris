#!/bin/bash

### 1. create table
bash ./create-tpch-tables.sh

### 2. load data
bash ./load-tpch-data.sh

### 3. exec queries
bash ./exec-tpch-queries.sh
