#!/bin/bash
newVersion=$1
echo "$newVersion"
if [ -z "$newVersion"  ] ; then
    echo Expected new version as first argument
    exit 1
fi

mvn -q versions:set -DnewVersion="$newVersion" -f pom.xml
echo Done
