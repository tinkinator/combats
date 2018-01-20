#!/bin/bash

set -e
VERSION=$(python version.py)
echo "Building Combats Update Daemon version $VERSION"
docker build . -t gcr.io/strategos-151306/combats-upd-daemon:$VERSION
gcloud docker -- push gcr.io/strategos-151306/combats-upd-daemon:$VERSION
kubectl set image deployment/combats-update combats-update=gcr.io/strategos-151306/combats-upd-daemon:$VERSION