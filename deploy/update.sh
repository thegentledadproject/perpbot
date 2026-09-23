#!/usr/bin/env bash
set -euo pipefail

# Redeploy the current REF to an already-bootstrapped box: fetch, checkout,
# reinstall, refresh the unit files if they changed, and restart. Run as
# root.
#
# Usage: update.sh [REF]
#   REF   branch/tag/sha to deploy (default: master)

REF="${1:-master}"

if [ "$(id -u)" -ne 0 ]; then
  echo "update.sh must be run as root (sudo)" >&2
  exit 1
fi

cd /opt/polyperps

sudo -u polyperps git fetch --prune origin

# --force: the box runs exactly what is committed. A previous deploy that
# died mid-checkout leaves tracked files modified, and without --force that
# blocks every later deploy with "local changes would be overwritten".
if sudo -u polyperps git rev-parse --verify --quiet "origin/${REF}" >/dev/null; then
  sudo -u polyperps git checkout --force --detach "origin/${REF}"
else
  sudo -u polyperps git checkout --force --detach "${REF}"
fi

sudo -u polyperps .venv/bin/pip install -e . --quiet

for unit in polyperps-feed.service polyperps-paper.service polyperps-dashboard.service polyperps-prune.service polyperps-prune.timer; do
  if ! cmp -s "deploy/${unit}" "/etc/systemd/system/${unit}"; then
    cp "deploy/${unit}" "/etc/systemd/system/${unit}"
    systemctl daemon-reload
  fi
done

systemctl enable --now polyperps-prune.timer
systemctl restart polyperps-feed polyperps-paper polyperps-dashboard

systemctl --no-pager status polyperps-feed polyperps-paper polyperps-dashboard || true

echo "deployed commit: $(sudo -u polyperps git rev-parse --short HEAD)"
