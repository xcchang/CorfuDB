#!/bin/bash

# Update corfu version. Format: {major}.{minor}.{incremental}.{date yyyy-mm-dd}.{build number}

BUILD_NUMBER="$1"

# Show warning if build number is not set
if [[ -z "${BUILD_NUMBER}" ]]; then
    echo "Please pass BUILD_NUMBER"
else
    ./mvnw build-helper:parse-version versions:set \
    -DnewVersion=\${parsedVersion.majorVersion}.\${parsedVersion.minorVersion}.\${parsedVersion.incrementalVersion}.$(date +%Y-%m-%d).${BUILD_NUMBER} \
    -DgenerateBackupPoms=false \
    -q
fi

