## Usage

These scripts are used to make tpc-h test.
follow the steps below:

### 1. build tpc-h dbgen tool.
    ./build-tpch-dbgen.sh
### 2. generate tpc-h data. use -h for more infomations.
    ./gen-tpch-data.sh -s 1
### 3. create tpc-h tables. modify `doris-cluster.conf` to specify doris info, then run script below.
    ./create-tpch-tables.sh
### 4. load tpc-h data. use -h for help.
    ./load-tpch-data.sh
### 5. run tpc-h queries.
    ./run-tpch-queries.sh
