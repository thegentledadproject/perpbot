#!/usr/bin/env bash
set -euo pipefail

# First-time EC2 setup for polyperps. Run as root on a fresh Ubuntu 24.04
# box: installs system packages, creates the service user, clones the
# repo, builds the venv, installs the systemd units, and enables (but does
# NOT start) the services so you can edit /etc/polyperps/env first.
#
# Usage: bootstrap.sh REPO_URL [REF]
#   REPO_URL  public git URL to clone (required; not known ahead of time)
#   REF       branch/tag/sha to check out (default: master)

if [ "$#" -lt 1 ]; then
  echo "usage: bootstrap.sh REPO_URL [REF]" >&2
  exit 1
fi

REPO_URL="$1"
REF="${2:-master}"

if [ "$(id -u)" -ne 0 ]; then
  echo "bootstrap.sh must be run as root (sudo)" >&2
  exit 1
fi

echo "== installing packages =="
apt-get update && apt-get install -y python3 python3-venv git

echo "== service user =="
if ! id polyperps >/dev/null 2>&1; then
  useradd --system --home /var/lib/polyperps --shell /usr/sbin/nologin polyperps
fi

echo "== directories =="
mkdir -p /var/lib/polyperps /etc/polyperps /etc/credstore
chown polyperps:polyperps /var/lib/polyperps
chmod 700 /etc/credstore

echo "== code =="
# NOTE: re-running bootstrap.sh does not redeploy new code on an
# already-bootstrapped box - that is update.sh's job. This only clones
# (first run) or re-fetches/checks-out REF (later runs) so bootstrap.sh
# is safe to re-run for the non-code setup below (packages, units, env
# file). Once /opt/polyperps is chowned to polyperps, git refuses to
# operate on it as root ("detected dubious ownership"), so every git
# call after the chown runs as the polyperps user, same as update.sh.
if [ ! -d /opt/polyperps/.git ]; then
  git clone "$REPO_URL" /opt/polyperps
fi
chown -R polyperps:polyperps /opt/polyperps
sudo -u polyperps git -C /opt/polyperps fetch --prune origin
sudo -u polyperps git -C /opt/polyperps checkout "$REF"

echo "== venv =="
sudo -u polyperps bash -c '
  set -euo pipefail
  cd /opt/polyperps
  python3 -m venv .venv
  .venv/bin/pip install --upgrade pip
  .venv/bin/pip install -e .
'

echo "== env file =="
if [ ! -f /etc/polyperps/env ]; then
  cp /opt/polyperps/deploy/env.example /etc/polyperps/env
  chmod 640 /etc/polyperps/env
  chown root:polyperps /etc/polyperps/env
fi

echo "== systemd units =="
# Gate the copy on content so a re-run of bootstrap.sh doesn't clobber an
# operator's uncommented LoadCredential= lines in the installed paper unit.
for unit in polyperps-feed.service polyperps-paper.service; do
  if ! cmp -s "/opt/polyperps/deploy/${unit}" "/etc/systemd/system/${unit}"; then
    cp "/opt/polyperps/deploy/${unit}" "/etc/systemd/system/${unit}"
    systemctl daemon-reload
  fi
done
systemctl enable polyperps-feed polyperps-paper

cat <<'EOF'

== next steps ==
1. Edit /etc/polyperps/env (instrument ids, db path, hypothesis, run id).
2. Optionally drop Telegram secrets into /etc/credstore/ (root:root 0600)
   and uncomment the LoadCredential= lines in
   /etc/systemd/system/polyperps-paper.service, then: systemctl daemon-reload
3. One-time data setup as the service user (the paper run refuses to start
   without a stored fee row, and seeds its strategy history from candles):
     cd /opt/polyperps && sudo -u polyperps env $(grep -v '^#' /etc/polyperps/env | xargs)        .venv/bin/python scripts/store_fees.py
     cd /opt/polyperps && sudo -u polyperps env $(grep -v '^#' /etc/polyperps/env | xargs)        .venv/bin/python scripts/backfill.py --days 31 --interval 1h
4. Start the services:
     systemctl start polyperps-feed polyperps-paper
5. Watch them:
     journalctl -fu polyperps-feed
     journalctl -fu polyperps-paper
EOF
