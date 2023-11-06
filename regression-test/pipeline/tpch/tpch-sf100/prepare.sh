#!/bin/bash

# Build Step: Command Line
: <<EOF
#!/bin/bash

teamcity_build_checkoutDir="%teamcity.build.checkoutDir%"
if [[ -f "${teamcity_build_checkoutDir:-}"/regression-test/pipeline/tpch/tpch-sf100/prepare.sh ]]; then
    cd "${teamcity_build_checkoutDir}"/regression-test/pipeline/tpch/tpch-sf100/
    bash -x prepare.sh
else
    echo "Build Step file missing: regression-test/pipeline/tpch/tpch-sf100/prepare.sh" && exit 1
fi
EOF

## run.sh content ##

echo "Prepare to run tpch sf100 test"
if [[ -z ${commit_id:-} || -z ${pull_request_id:-} ]]; then
    echo "ERROR: env commit_id or pull_request_id not set" && exit 1
else
    commit_id_from_checkout=${commit_id}
fi

if ${DEBUG:-false}; then commit_id_from_trigger=${commit_id}; fi

if [[ "${commit_id_from_trigger}" != "${commit_id_from_checkout}" ]]; then
    echo -e "ERROR: PR(${pull_request_id}),
    the lastest commit id
    ${commit_id_from_checkout}
    not equail to the commit_id_from_trigger
    ${commit_id_from_trigger}
    commit_id_from_trigger is outdate"
    exit 1
fi
