#!/bin/bash
mvn clean deploy -q -DperformRelease=true -DskipTests -DaltDeploymentRepository=flipkart::default::http://artifactory.nm.flipkart.com:8081/artifactory/libs-release-local
mvn clean deploy -q -DperformRelease=true -DskipTests -DaltDeploymentRepository=flipkart::default::http://artifactory.nm.flipkart.com:8081/artifactory/libs-releases-local

