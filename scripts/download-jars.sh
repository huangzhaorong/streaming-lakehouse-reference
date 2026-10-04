#!/bin/bash
# Download connector JARs required by the Flink image.
#
# All JARs are fetched from Maven Central or GitHub Releases with SHA-256
# verification. 部署时由 flink role 在数据面执行（/opt/slr/scripts/），产物拷入
# /opt/flink/lib/；本地执行一次可预热 jars/ 并核对 checksum。
#
# Usage:
#   ./scripts/download-jars.sh

set -Eeuo pipefail

JARS_DIR="$(cd "$(dirname "$0")/../jars" && pwd)"
mkdir -p "$JARS_DIR"

MAVEN="https://repo1.maven.org/maven2"

# name | URL | SHA-256
JARS=(
  "flink-connector-iggy.jar|https://github.com/gordonmurray/flink-connector-iggy/releases/download/v0.1.1/flink-connector-iggy-0.1.1-SNAPSHOT.jar|17979f13c13b9a17e7b92d304a07622fed1d94682b806e961732fa82a1bd80df"
  "fluss-flink-1.20-0.9.0-incubating.jar|${MAVEN}/org/apache/fluss/fluss-flink-1.20/0.9.0-incubating/fluss-flink-1.20-0.9.0-incubating.jar|06e3a25461e3c4de9f1875050593287a9b61f794c8216c33e8464b2e85776489"
  "paimon-flink-1.20-1.3.1.jar|${MAVEN}/org/apache/paimon/paimon-flink-1.20/1.3.1/paimon-flink-1.20-1.3.1.jar|a333b7a3df6143b782d129895f64575f0007912c4a2c85502d421b8198f42ce1"
  "iceberg-flink-runtime-1.20-1.10.1.jar|${MAVEN}/org/apache/iceberg/iceberg-flink-runtime-1.20/1.10.1/iceberg-flink-runtime-1.20-1.10.1.jar|f06b3f2fbd004feeb10adc8957f27d43203a0dc526a9ae2e0a42219fbcdbcfe7"
  "flink-sql-parquet-1.20.3.jar|${MAVEN}/org/apache/flink/flink-sql-parquet/1.20.3/flink-sql-parquet-1.20.3.jar|f02a6a68ba9c17713efffa69eecc80bedf1aa1c71f9ad216c63972793cd21c45"
  "hadoop-client-api-3.3.6.jar|${MAVEN}/org/apache/hadoop/hadoop-client-api/3.3.6/hadoop-client-api-3.3.6.jar|f3d2347a6e1c6885d5bcfd4f60c3ac3810ec11068fc161e04329baabf412d963"
  "hadoop-client-runtime-3.3.6.jar|${MAVEN}/org/apache/hadoop/hadoop-client-runtime/3.3.6/hadoop-client-runtime-3.3.6.jar|15f01bc804294df06d2effc87de363a83cf589f50558bdbf48f72541ad8de854"
  "commons-logging-1.2.jar|${MAVEN}/commons-logging/commons-logging/1.2/commons-logging-1.2.jar|daddea1ea0be0f56978ab3006b8ac92834afeefbd9b7e4e6316fca57df0fa636"
)

FAILED=0

for entry in "${JARS[@]}"; do
  IFS='|' read -r name url checksum <<< "$entry"
  dest="${JARS_DIR}/${name}"

  if [ -f "$dest" ]; then
    actual=$(sha256sum "$dest" | awk '{print $1}')
    if [ "$actual" = "$checksum" ]; then
      echo "OK  $name (already present)"
      continue
    else
      echo "MISMATCH  $name — re-downloading"
      rm -f "$dest"
    fi
  fi

  echo "Downloading $name ..."
  if ! curl --fail --show-error --silent --location -o "$dest" "$url"; then
    echo "FAILED to download $name from $url"
    FAILED=1
    continue
  fi

  actual=$(sha256sum "$dest" | awk '{print $1}')
  if [ "$actual" != "$checksum" ]; then
    echo "CHECKSUM FAILED for $name"
    echo "  expected: $checksum"
    echo "  got:      $actual"
    rm -f "$dest"
    FAILED=1
  else
    echo "OK  $name"
  fi
done

if [ "$FAILED" -ne 0 ]; then
  echo ""
  echo "Some JARs failed to download or verify. Check output above."
  exit 1
fi

# ─── JindoSDK（OSS-HDFS connector + JindoFuse 同源）────────────────────────
# tarball 一次下载，解出 Flink 所需 3 jar + native so；fuse 用同一 tarball
#（jindofuse role 独立下载整包，此处只取 jar/so）。
JINDOSDK_VERSION="6.10.8"
JINDOSDK_URL="https://jindodata-binary.oss-cn-shanghai.aliyuncs.com/release/${JINDOSDK_VERSION}/jindosdk-${JINDOSDK_VERSION}-linux.tar.gz"
# 实测 sha256（2026-10-04 下载核对）
JINDOSDK_SHA256="2377f2ce3982dadc860432b507517431333a1bf726bb0860c3484834063953eb"

sha256_of() { sha256sum "$1" 2>/dev/null | awk '{print $1}' || shasum -a 256 "$1" | awk '{print $1}'; }

JINDO_FLINK_JAR="jindo-flink-${JINDOSDK_VERSION}-nextarch-full.jar"
if [ -f "${JARS_DIR}/${JINDO_FLINK_JAR}" ] && [ -f "${JARS_DIR}/native/libjindosdk_java.so" ]; then
  echo "OK  jindosdk jars (already present)"
else
  TARBALL="${JARS_DIR}/jindosdk-${JINDOSDK_VERSION}-linux.tar.gz"
  if [ ! -f "$TARBALL" ] || [ "$(sha256_of "$TARBALL")" != "$JINDOSDK_SHA256" ]; then
    echo "Downloading jindosdk-${JINDOSDK_VERSION}-linux.tar.gz (~463MB) ..."
    curl --fail --show-error --silent --location -o "$TARBALL" "$JINDOSDK_URL"
    actual=$(sha256_of "$TARBALL")
    if [ "$actual" != "$JINDOSDK_SHA256" ]; then
      echo "CHECKSUM FAILED for jindosdk tarball"; echo "  expected: $JINDOSDK_SHA256"; echo "  got:      $actual"
      rm -f "$TARBALL"; exit 1
    fi
  fi
  TOP="jindosdk-${JINDOSDK_VERSION}-linux"
  mkdir -p "${JARS_DIR}/native"
  tar xzf "$TARBALL" -C "${JARS_DIR}" \
    "${TOP}/plugins/flink/${JINDO_FLINK_JAR}" \
    "${TOP}/lib/jindo-sdk-${JINDOSDK_VERSION}-nextarch.jar" \
    "${TOP}/lib/jindo-core-${JINDOSDK_VERSION}-nextarch.jar" \
    "${TOP}/lib/native/libjindosdk_java.so" \
    "${TOP}/lib/native/libjindosdk_c.so" \
    "${TOP}/lib/native/libjindo-csdk.so" \
    "${TOP}/lib/native/libjemalloc.so"
  mv -f "${JARS_DIR}/${TOP}/plugins/flink/${JINDO_FLINK_JAR}" "${JARS_DIR}/"
  mv -f "${JARS_DIR}/${TOP}"/lib/jindo-*.jar "${JARS_DIR}/"
  mv -f "${JARS_DIR}/${TOP}"/lib/native/*.so "${JARS_DIR}/native/"
  rm -rf "${JARS_DIR}/${TOP}"
  rm -f "$TARBALL"  # 463MB 中间产物不留（jar 就位即幂等跳过）
  echo "OK  jindosdk: ${JINDO_FLINK_JAR} + jindo-sdk/core nextarch + native/*.so → jars/native/"
fi

echo ""
echo "All JARs downloaded and verified in ${JARS_DIR}/"
