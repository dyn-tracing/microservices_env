#!/bin/bash
FILE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
TOOLS_DIR="${FILE_DIR}/../microservices_env/tools/"
cd $TOOLS_DIR && curl -L https://istio.io/downloadIstio | \
    ISTIO_VERSION=1.12.1 TARGET_ARCH=x86_64 sh - && cd -
