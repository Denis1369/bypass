#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ENV_FILE="${SCRIPT_DIR}/.env"
ENV_TEMPLATE="${SCRIPT_DIR}/.env.example"
COMPOSE_FILE="${SCRIPT_DIR}/docker-compose.yml"

if ! command -v docker >/dev/null 2>&1; then
  echo "Docker is not installed. Run ./docker-vps/install-docker-ubuntu24.sh first."
  exit 1
fi

if [ ! -f "${ENV_FILE}" ]; then
  cp "${ENV_TEMPLATE}" "${ENV_FILE}"
  echo "Created ${ENV_FILE}. Edit TELEMOST_JOIN_LINK and run again."
  exit 1
fi

if grep -q 'replace-me' "${ENV_FILE}"; then
  echo "Please edit ${ENV_FILE} and replace TELEMOST_JOIN_LINK before starting."
  exit 1
fi

mkdir -p "${SCRIPT_DIR}/data"

compose() {
  docker compose -f "${COMPOSE_FILE}" --env-file "${ENV_FILE}" "$@"
}

case "${1:-up}" in
  up)
    compose up --build -d
    ;;
  logs)
    compose logs -f
    ;;
  down)
    compose down
    ;;
  restart)
    compose up --build -d --force-recreate
    ;;
  ps)
    compose ps
    ;;
  config)
    compose config
    ;;
  *)
    compose "$@"
    ;;
esac
