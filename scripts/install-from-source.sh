#!/usr/bin/env bash
set -Eeuo pipefail

export DOCKER_BUILDKIT="${DOCKER_BUILDKIT:-1}"

compose=(docker compose -f docker-compose.yml -f docker-compose.build.yml)

docker compose version >/dev/null

echo "Building Panel..."
"${compose[@]}" build panel

echo "Building Checker..."
"${compose[@]}" build checker

echo "Starting services from local images..."
"${compose[@]}" up -d --no-build
"${compose[@]}" ps
