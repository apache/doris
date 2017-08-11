# This file build all docs
#

BUILD_DIR = ${CURDIR}/build

all: zip_help
.PHONY: all

${BUILD_DIR}:
	mkdir -p ${BUILD_DIR}

# build help zip
HELP_OUTPUT = ${BUILD_DIR}/help-resource.zip
${HELP_OUTPUT}: help | ${BUILD_DIR}
	zip -r $@ $<
.PHONY: zip_help
zip_help: ${HELP_OUTPUT}

clean:
	rm -rf ${BUILD_DIR}
.PHONY: clean
