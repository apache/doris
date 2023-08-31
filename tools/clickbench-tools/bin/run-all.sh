#!/bin/bash

### 1. create table
bash ./create-clickbench-tables.sh

### 2. load data
bash ./load-clickbench-data.sh

### 3. exec queries
bash ./exec-clickbench-queries.sh