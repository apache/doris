#!/bin/bash

# Build Step: Command Line
: <<EOF
#!/bin/bash

teamcity_build_checkoutDir="%teamcity.build.checkoutDir%"
if [[ -f "${teamcity_build_checkoutDir}"/regression-test/pipeline/tpch/tpch-sf100/prepare.sh ]]; then
    cd "${teamcity_build_checkoutDir}"/regression-test/pipeline/tpch/tpch-sf100/
    bash -x prepare.sh
else
    echo "Build Step file missing: regression-test/pipeline/tpch/tpch-sf100/prepare.sh" && exit 1
fi
EOF

echo "Prepare to run tpch sf100 test"
