#!/bin/bash

# Build Step: Command Line
: <<EOF
#!/bin/bash

# Execute step even if some of the previous steps failed
teamcity_build_checkoutDir="%teamcity.build.checkoutDir%"
if [[ -f "${teamcity_build_checkoutDir:-}"/regression-test/pipeline/tpch/tpch-sf100/clean.sh ]]; then
    cd "${teamcity_build_checkoutDir}"/regression-test/pipeline/tpch/tpch-sf100/
    bash -x clean.sh
else
    echo "Build Step file missing: regression-test/pipeline/tpch/tpch-sf100/clean.sh" && exit 1
fi
EOF

## clean.sh content ##

# stop_doris
source ../../common/doris-utils.sh

DORIS_HOME="${teamcity_build_checkoutDir}/output"
export DORIS_HOME
stop_doris
