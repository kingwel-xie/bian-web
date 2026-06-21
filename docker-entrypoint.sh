#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -eq 0 ] || [ "${1:-}" = "serve" ]; then
  if [ "${1:-}" = "serve" ]; then
    shift
  fi
  exec python /app/app.py "$@"
fi

if [ "${1:-}" = "run" ]; then
  shift
elif [ "${1:-}" = "email" ]; then
  shift
  exec python /app/send_exports_email.py "$@"
fi

exec python /app/workflow.py "$@"
