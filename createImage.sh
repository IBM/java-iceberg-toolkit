#!/bin/bash

HELP_MESSAGE="
Usage: $0

Build and save image locally

Options:
    -h, --help      Show help message and exit
"

case "$1" in
    -h | --help )
    echo "${HELP_MESSAGE}"
    exit 0
    ;;
esac

if [ "$EUID" -ne 0 ]; then
    echo "Error: root privileges required"
    exit 1
fi

buildah rmi -f java-iceberg-cli:latest
buildah bud --tag java-iceberg-cli:latest --file Dockerfile .
if [[ $? -eq 0 ]]; then
    echo "java-iceberg-cli container image build succeeded"
else
    echo "Building java-iceberg-cli container image failed"
    exit 1
fi