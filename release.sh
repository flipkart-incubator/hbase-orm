#!/bin/bash
mvn clean deploy -DperformRelease=true -DskipTests -DaltDeploymentRepository=flipkart::default::http://10.85.59.116/artifactory/v1.0/artifacts/libs-release-local
