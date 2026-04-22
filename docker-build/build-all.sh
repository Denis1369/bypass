#!/usr/bin/env bash
set -euo pipefail

ROOT=/workspace
REPO="${ROOT}/whitelist-bypass"

export HOME=/root
export ANDROID_HOME=/root/Library/Android/sdk
export ANDROID_SDK_ROOT=${ANDROID_HOME}
export ANDROID_NDK_HOME=${ANDROID_HOME}/ndk/29.0.14206865
export GRADLE_USER_HOME=/opt/gradle
export PATH=/usr/local/go/bin:/root/go/bin:${ANDROID_HOME}/cmdline-tools/latest/bin:${ANDROID_HOME}/platform-tools:${PATH}

normalize_file() {
  local file="$1"
  [ -f "${file}" ] || return 0
  local tmp
  tmp="$(mktemp)"
  tr -d '\r' < "${file}" > "${tmp}"
  cat "${tmp}" > "${file}"
  rm -f "${tmp}"
}

# Windows checkouts often mount scripts with CRLF into the container.
# Normalize line endings without rename-overwrite, which is unreliable on bind mounts.
while IFS= read -r -d '' file; do
  normalize_file "${file}"
done < <(find "${REPO}" -path "${REPO}/.git" -prune -o -type f -name '*.sh' -print0)
normalize_file "${REPO}/android-app/gradlew"

cd "${REPO}/relay"
gomobile init -v

cd "${REPO}"
echo ""
echo "=== build-go.sh ==="
bash ./build-go.sh

cd "${REPO}"
echo ""
echo "=== build-app.sh ==="
bash ./build-app.sh

cd "${REPO}"
echo ""
echo "=== build-creator.sh ==="
bash ./build-creator.sh

cd "${REPO}"
echo ""
echo "=== ALL BUILDS DONE ==="
ls -lh "${REPO}/prebuilts" || true
