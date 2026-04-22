#!/usr/bin/env bash
set -euo pipefail

if [ "$(id -u)" -eq 0 ]; then
  SUDO=""
else
  SUDO="sudo"
fi

if ! command -v apt-get >/dev/null 2>&1; then
  echo "This installer is intended for Ubuntu."
  exit 1
fi

if [ -r /etc/os-release ]; then
  . /etc/os-release
else
  echo "Cannot read /etc/os-release"
  exit 1
fi

if [ "${ID:-}" != "ubuntu" ]; then
  echo "This installer is intended for Ubuntu, found: ${ID:-unknown}"
  exit 1
fi

echo "Removing conflicting Docker packages if present..."
${SUDO} apt-get remove -y docker.io docker-compose docker-compose-v2 docker-doc podman-docker containerd runc || true

echo "Installing Docker repository prerequisites..."
${SUDO} apt-get update
${SUDO} apt-get install -y ca-certificates curl
${SUDO} install -m 0755 -d /etc/apt/keyrings
${SUDO} curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
${SUDO} chmod a+r /etc/apt/keyrings/docker.asc

echo "Configuring Docker apt repository..."
${SUDO} tee /etc/apt/sources.list.d/docker.sources >/dev/null <<EOF
Types: deb
URIs: https://download.docker.com/linux/ubuntu
Suites: ${UBUNTU_CODENAME:-$VERSION_CODENAME}
Components: stable
Architectures: $(dpkg --print-architecture)
Signed-By: /etc/apt/keyrings/docker.asc
EOF

echo "Installing Docker Engine, Buildx and Compose plugin..."
${SUDO} apt-get update
${SUDO} apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
${SUDO} systemctl enable --now docker

LOGIN_USER="${SUDO_USER:-${USER:-}}"
if [ -n "${LOGIN_USER}" ] && [ "${LOGIN_USER}" != "root" ]; then
  echo "Adding ${LOGIN_USER} to docker group..."
  ${SUDO} usermod -aG docker "${LOGIN_USER}" || true
  echo "You may need to log out and back in for docker group membership to take effect."
fi

echo "Docker installation complete."
echo "Verify with: docker --version && docker compose version"
