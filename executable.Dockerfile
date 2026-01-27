# parts are copied from Dockerfile

# these need to be defined before any FROM - otherwise, the ARGs expand to empty strings.
# see build_x86_64_executable.sh and build_aarch64_executable.sh which define these.
ARG PYSPY_RUST_BUILDER_VERSION
ARG RBSPY_RUST_BUILDER_VERSION
ARG PERF_BUILDER_UBUNTU
ARG PHPSPY_BUILDER_UBUNTU
ARG AP_BUILDER_CENTOS
ARG AP_BUILDER_ALPINE
ARG AP_CENTOS_MIN
ARG BURN_BUILDER_GOLANG
ARG GPROFILER_BUILDER
ARG PYPERF_BUILDER_UBUNTU
ARG DOTNET_BUILDER
ARG NODE_PACKAGE_BUILDER_MUSL
ARG NODE_PACKAGE_BUILDER_GLIBC

# py-spy
FROM rust${PYSPY_RUST_BUILDER_VERSION} AS pyspy-builder
WORKDIR /tmp
ARG ENABLE_PYSPY=1

COPY scripts/prepare_machine-unknown-linux-musl.sh .
COPY scripts/libunwind_build.sh .
RUN if [ "$ENABLE_PYSPY" = "1" ]; then ./prepare_machine-unknown-linux-musl.sh; fi

COPY scripts/pyspy_build.sh .
COPY scripts/pyspy_commit.txt .
COPY scripts/pyspy_tag.txt .
RUN if [ "$ENABLE_PYSPY" = "1" ]; then \
      ./pyspy_build.sh && \
      mv "/tmp/py-spy/target/$(uname -m)-unknown-linux-musl/release/py-spy" /tmp/py-spy/py-spy; \
    else \
      mkdir -p /tmp/py-spy && touch /tmp/py-spy/py-spy; \
    fi

# rbspy
FROM rust${RBSPY_RUST_BUILDER_VERSION} AS rbspy-builder
WORKDIR /tmp
ARG ENABLE_RBSPY=1

COPY scripts/prepare_machine-unknown-linux-musl.sh .
COPY scripts/libunwind_build.sh .
RUN if [ "$ENABLE_RBSPY" = "1" ]; then ./prepare_machine-unknown-linux-musl.sh; fi

COPY scripts/rbspy_build.sh .
RUN if [ "$ENABLE_RBSPY" = "1" ]; then \
      ./rbspy_build.sh && \
      mv "/tmp/rbspy/target/$(uname -m)-unknown-linux-musl/release/rbspy" /tmp/rbspy/rbspy; \
    else \
      mkdir -p /tmp/rbspy && touch /tmp/rbspy/rbspy; \
    fi

# dotnet-trace
FROM mcr.microsoft.com/dotnet/sdk${DOTNET_BUILDER} as dotnet-builder
WORKDIR /tmp
ARG ENABLE_DOTNET=1
RUN if [ "$ENABLE_DOTNET" = "1" ]; then \
      apt-get update && \
      dotnet tool install --global dotnet-trace --version 6.0.351802 && \
      apt-get install -y --no-install-recommends patchelf; \
    fi

RUN if [ "$ENABLE_DOTNET" = "1" ]; then \
      cp -r "$HOME/.dotnet" "/tmp/dotnet"; \
    else \
      mkdir -p /tmp/dotnet/tools && mkdir -p /tmp/dotnet/deps && \
      mkdir -p /usr/share/dotnet && touch /usr/share/dotnet/host; \
    fi
COPY scripts/dotnet_prepare_dependencies.sh .
COPY scripts/dotnet_trace_dependencies.txt .
RUN if [ "$ENABLE_DOTNET" = "1" ]; then ./dotnet_prepare_dependencies.sh; fi

# perf
FROM ubuntu${PERF_BUILDER_UBUNTU} AS perf-builder
WORKDIR /tmp

COPY scripts/perf_env.sh .
RUN ./perf_env.sh

COPY scripts/libunwind_build.sh .
RUN ./libunwind_build.sh

COPY scripts/perf_build.sh .
RUN ./perf_build.sh

# phpspy
FROM ubuntu${PHPSPY_BUILDER_UBUNTU} as phpspy-builder
WORKDIR /tmp
ARG ENABLE_PHPSPY=1
COPY scripts/phpspy_env.sh .
RUN if [ "$ENABLE_PHPSPY" = "1" ]; then ./phpspy_env.sh; fi
COPY scripts/phpspy_build.sh .
RUN if [ "$ENABLE_PHPSPY" = "1" ]; then \
      ./phpspy_build.sh; \
    else \
      mkdir -p /tmp/phpspy && touch /tmp/phpspy/phpspy && \
      mkdir -p /tmp/binutils/binutils-2.25/bin/bin && \
      touch /tmp/binutils/binutils-2.25/bin/bin/objdump && \
      touch /tmp/binutils/binutils-2.25/bin/bin/strings; \
    fi

# async-profiler glibc
FROM centos${AP_BUILDER_CENTOS} AS async-profiler-builder-glibc
WORKDIR /tmp
COPY scripts/async_profiler_env_glibc.sh scripts/fix_centos7.sh scripts/pdeathsigger.c ./
RUN if grep -q "CentOS Linux 7" /etc/os-release ; then \
      ./fix_centos7.sh; \
    fi
RUN ./async_profiler_env_glibc.sh && \
    gcc -static -o pdeathsigger pdeathsigger.c

COPY scripts/async_profiler_build_shared.sh .
RUN ./async_profiler_build_shared.sh

# a build step to ensure the minimum CentOS version that we require can "ldd" our libasyncProfiler.so file.
FROM centos${AP_CENTOS_MIN} AS async-profiler-centos-min-test-glibc
SHELL ["/bin/bash", "-c", "-euo", "pipefail"]
COPY --from=async-profiler-builder-glibc /tmp/async-profiler/build/lib/libasyncProfiler.so /libasyncProfiler.so
RUN if ldd /libasyncProfiler.so 2>&1 | grep -q "not found" ; then echo "libasyncProfiler.so is not compatible with minimum CentOS!"; readelf -Ws /libasyncProfiler.so; ldd /libasyncProfiler.so; exit 1; fi

# async-profiler musl
FROM alpine${AP_BUILDER_ALPINE} AS async-profiler-builder-musl
WORKDIR /tmp

COPY scripts/async_profiler_env_musl.sh .
RUN ./async_profiler_env_musl.sh
COPY scripts/async_profiler_build_shared.sh .
RUN ./async_profiler_build_shared.sh

FROM golang${BURN_BUILDER_GOLANG} AS burn-builder
WORKDIR /tmp

COPY scripts/burn_build.sh .
COPY scripts/burn_version.txt .
RUN ./burn_build.sh

# node-package-builder-musl
FROM alpine${NODE_PACKAGE_BUILDER_MUSL} AS node-package-builder-musl
WORKDIR /tmp
ARG ENABLE_NODE=1
COPY scripts/node_builder_musl_env.sh .
RUN if [ "$ENABLE_NODE" = "1" ]; then ./node_builder_musl_env.sh; fi
COPY scripts/build_node_package.sh .
RUN if [ "$ENABLE_NODE" = "1" ]; then \
      ./build_node_package.sh; \
    else \
      mkdir -p /tmp/module_build; \
    fi

# building bcc along with helpers
# built on newer Ubuntu because they require new clang (newer than available in GPROFILER_BUILDER's CentOS 7)
# these are only relevant for modern kernels, so there's no real reason to build them on CentOS 7 anyway.
FROM ubuntu${PYPERF_BUILDER_UBUNTU} AS bcc-build
COPY --from=perf-builder /bpftool /bpftool

WORKDIR /bcc
COPY scripts/staticx_for_pyperf_patch.diff .
COPY scripts/bcc_helpers_build.sh .
COPY scripts/pyperf_env.sh .
RUN ./pyperf_env.sh --with-staticx

WORKDIR /tmp
COPY ./scripts/libunwind_build.sh .
RUN if [ "$(uname -m)" = "aarch64" ]; then \
      exit 0; \
    fi && \
    ./libunwind_build.sh

WORKDIR /bcc
COPY scripts/pyperf_build.sh .
RUN ./pyperf_build.sh --with-staticx

# gprofiler
FROM centos${GPROFILER_BUILDER} AS build-prepare

WORKDIR /tmp
COPY scripts/fix_centos7.sh scripts/fix_centos8.sh ./
# fix repo links for CentOS 8, and enable powertools (required to download glibc-static)
RUN if grep -q "CentOS Linux 8" /etc/os-release ; then \
        ./fix_centos8.sh; \
    elif grep -q "CentOS Linux 7" /etc/os-release ; then \
        ./fix_centos7.sh; \
    fi

# python 3.10 installation && yum clean all
WORKDIR /python
COPY scripts/prepare_centos.sh .
RUN ./prepare_centos.sh

COPY ./scripts/openssl_build.sh .
RUN ./openssl_build.sh
COPY ./scripts/python310_build.sh .
RUN ./python310_build.sh

ARG PROXY=""
ENV PIP_TRUST_HOSTS=${PROXY}

RUN set -e; \
    if [ "$PIP_TRUST_HOSTS" != "" ]; then \
        python3 -m pip config set global.trusted-host "$(python3 -m pip config get global.trusted-host) $PIP_TRUST_HOSTS"; \
    fi

# gProfiler part

WORKDIR /app

RUN yum --setopt=skip_missing_names_on_install=False install -y \
        gcc \
        curl \
        glibc-static \
        libicu && \
    yum clean all

# needed for aarch64 (for staticx)
RUN set -e; \
    if [ "$(uname -m)" = "aarch64" ]; then \
        yum install -y glibc-static zlib-devel.aarch64 && \
        yum clean all; \
    fi
# needed for aarch64, scons & wheel are needed to build staticx
RUN set -e; \
    if [ "$(uname -m)" = "aarch64" ]; then \
         ln -s /usr/lib64/python3.10/lib-dynload /usr/lib/python3.10/lib-dynload; \
    fi

# we want the latest pip
# hadolint ignore=DL3013
RUN python3 -m pip install --no-cache-dir --upgrade pip

FROM ${NODE_PACKAGE_BUILDER_GLIBC} as node-package-builder-glibc
USER 0
WORKDIR /tmp
ARG ENABLE_NODE=1
COPY scripts/node_builder_glibc_env.sh scripts/fix_centos7.sh ./
RUN if [ "$ENABLE_NODE" = "1" ] && grep -q "CentOS Linux 7" /etc/os-release ; then \
      ./fix_centos7.sh; \
    fi
RUN if [ "$ENABLE_NODE" = "1" ]; then ./node_builder_glibc_env.sh; fi
COPY scripts/build_node_package.sh .
RUN if [ "$ENABLE_NODE" = "1" ]; then \
      ./build_node_package.sh; \
    else \
      mkdir -p /tmp/module_build; \
    fi
# needed for hadolint
WORKDIR /app
USER 1001

FROM build-prepare as build-stage

ARG STATICX=true
ENV STATICX=${STATICX}
ARG ENABLE_PYSPY=1
ARG ENABLE_RBSPY=1
ARG ENABLE_DOTNET=1
ARG ENABLE_PHPSPY=1
ARG ENABLE_NODE=1

COPY requirements.txt requirements.txt
COPY granulate-utils/setup.py granulate-utils/requirements.txt granulate-utils/README.md granulate-utils/
COPY granulate-utils/granulate_utils granulate-utils/granulate_utils
COPY granulate-utils/glogger granulate-utils/glogger
RUN python3 -m pip install --no-cache-dir -r requirements.txt

COPY exe-requirements.txt exe-requirements.txt
RUN python3 -m pip install --no-cache-dir -r exe-requirements.txt

# copy PyPerf, licenses and notice file.
RUN mkdir -p gprofiler/resources/ruby && \
    mkdir -p gprofiler/resources/python/pyperf
COPY --from=bcc-build /bcc/bcc/LICENSE.txt gprofiler/resources/python/pyperf/
COPY --from=bcc-build /bcc/bcc/licenses gprofiler/resources/python/pyperf/licenses
COPY --from=bcc-build /bcc/bcc/NOTICE gprofiler/resources/python/pyperf/
COPY --from=bcc-build /bcc/root/share/bcc/examples/cpp/PyPerf gprofiler/resources/python/pyperf/
COPY --from=bcc-build /bpf_get_fs_offset/get_fs_offset gprofiler/resources/python/pyperf/
COPY --from=bcc-build /bpf_get_stack_offset/get_stack_offset gprofiler/resources/python/pyperf/

COPY --from=pyspy-builder /tmp/py-spy/py-spy gprofiler/resources/python/py-spy
RUN if [ "$ENABLE_PYSPY" != "1" ]; then rm gprofiler/resources/python/py-spy; fi

COPY --from=rbspy-builder /tmp/rbspy/rbspy gprofiler/resources/ruby/rbspy
RUN if [ "$ENABLE_RBSPY" != "1" ]; then rm gprofiler/resources/ruby/rbspy; fi

COPY --from=perf-builder /perf gprofiler/resources/perf
COPY perfspect/perfspect gprofiler/resources/perfspect/perfspect

COPY --from=dotnet-builder /usr/share/dotnet/host gprofiler/resources/dotnet/host
COPY --from=dotnet-builder /tmp/dotnet/deps gprofiler/resources/dotnet/shared/Microsoft.NETCore.App/6.0.7
COPY --from=dotnet-builder /tmp/dotnet/tools gprofiler/resources/dotnet/tools
RUN if [ "$ENABLE_DOTNET" != "1" ]; then \
      rm -rf gprofiler/resources/dotnet; \
    fi

COPY --from=phpspy-builder /tmp/phpspy/phpspy gprofiler/resources/php/phpspy
COPY --from=phpspy-builder /tmp/binutils/binutils-2.25/bin/bin/objdump gprofiler/resources/php/objdump
COPY --from=phpspy-builder /tmp/binutils/binutils-2.25/bin/bin/strings gprofiler/resources/php/strings
RUN if [ "$ENABLE_PHPSPY" != "1" ]; then \
      rm gprofiler/resources/php/phpspy gprofiler/resources/php/objdump gprofiler/resources/php/strings; \
    fi

# copying from async-profiler-builder as an "old enough" centos.
COPY --from=async-profiler-builder-glibc /usr/bin/awk gprofiler/resources/php/awk
COPY --from=async-profiler-builder-glibc /usr/bin/xargs gprofiler/resources/php/xargs

COPY --from=async-profiler-builder-glibc /tmp/async-profiler/build/bin/asprof gprofiler/resources/java/asprof
COPY --from=async-profiler-builder-glibc /tmp/async-profiler/build/async-profiler-version gprofiler/resources/java/async-profiler-version
COPY --from=async-profiler-builder-glibc /tmp/pdeathsigger gprofiler/resources/pdeathsigger
COPY --from=async-profiler-centos-min-test-glibc /libasyncProfiler.so gprofiler/resources/java/glibc/libasyncProfiler.so
COPY --from=async-profiler-builder-musl /tmp/async-profiler/build/lib/libasyncProfiler.so gprofiler/resources/java/musl/libasyncProfiler.so
COPY --from=node-package-builder-musl /tmp/module_build gprofiler/resources/node/module/musl
COPY --from=node-package-builder-glibc /tmp/module_build gprofiler/resources/node/module/glibc
RUN if [ "$ENABLE_NODE" != "1" ]; then \
      rm -rf gprofiler/resources/node; \
    fi

COPY --from=burn-builder /tmp/burn/burn gprofiler/resources/burn

COPY gprofiler gprofiler

# run PyInstaller and make sure no 'gprofiler.*' modules are missing.
# see https://pyinstaller.readthedocs.io/en/stable/when-things-go-wrong.html
# from a quick look I didn't see how to tell PyInstaller to exit with an error on this, hence
# this check in the shell.
COPY pyi_build.py pyinstaller.spec scripts/check_pyinstaller.sh ./

RUN pyinstaller pyinstaller.spec \
    && echo \
    && test -f build/pyinstaller/warn-pyinstaller.txt \
    && ./check_pyinstaller.sh

# We need staticx main as version wasn't released yet for PyInstaller hooks throwing on non elfs.
# Removed build isolation as from some reason there's an issue with scons on the isolated env.
# hadolint ignore=DL3003
RUN if [ "$(uname -m)" = "aarch64" ]; then \
        git clone  https://github.com/Granulate/staticx.git && \
        cd staticx && \
        git checkout 33eefdadc72832d5aa67c0792768c9e76afb746d && \
        ln -s libnss_files.so.2 /lib64/libnss_files.so && \
        ln -s libnss_dns.so.2 /lib64/libnss_dns.so && \
        python3 -m pip install --no-cache-dir . ; \
    fi

RUN yum install -y patchelf upx && yum clean all

COPY ./scripts/list_needed_libs.sh ./scripts/list_needed_libs.sh
# staticx packs dynamically linked app with all of their dependencies, it tries to figure out which dynamic libraries are need for its execution
# in some cases, when the application is lazily loading some DSOs, staticx doesn't handle it.
# we use list_needed_libs.sh to list the dynamic dependencies of *all* of our resources,
# and make staticx pack them as well.
# hadolint ignore=SC2086
RUN set -e; \
    if [ "$STATICX" = "true" ]; then \
        LIBS=$(./scripts/list_needed_libs.sh) && \
        staticx $LIBS dist/gprofiler dist/gprofiler.output ; \
    else \
        mv dist/gprofiler dist/gprofiler.output ; \
    fi

FROM scratch AS export-stage

COPY --from=build-stage /app/dist/gprofiler.output /gprofiler
