FROM ghcr.io/translatorsri/renci-python-image:3.12.4

WORKDIR /apps
# variable "VERSION" must be passed as docker environment variables during the image build
# docker build --no-cache --build-arg VERSION=2.12.0 -t alpine/helm:2.12.0 .

#ARG HELM_VERSION=3.14.3

## ENV BASE_URL="https://storage.googleapis.com/kubernetes-helm"
#ENV BASE_URL="https://get.helm.sh"
#
#RUN case `uname -m` in \
#        x86_64) ARCH=amd64; ;; \
#        armv7l) ARCH=arm; ;; \
#        aarch64) ARCH=arm64; ;; \
#        ppc64le) ARCH=ppc64le; ;; \
#        s390x) ARCH=s390x; ;; \
#        *) echo "un-supported arch, exit ..."; exit 1; ;; \
#    esac && \
#    apk add --update --no-cache wget git curl bash yq rust cargo && \
#    wget ${BASE_URL}/helm-v${HELM_VERSION}-linux-${ARCH}.tar.gz -O - | tar -xz && \
#    mv linux-${ARCH}/helm /usr/bin/helm && \
#    chmod +x /usr/bin/helm && \
#    rm -rf linux-${ARCH}
#
COPY requirements.txt requirements.txt
RUN python -m pip install -r requirements.txt
COPY ./src  ./src
# @TODO figure out file access
USER root
WORKDIR ./src
ENV PYTHONPATH=/apps/src