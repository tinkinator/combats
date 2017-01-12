#!/bin/bash

set -e
VERSION=$(python version.py)
echo "Building Combats version $VERSION"
docker build . -t gcr.io/strategos-151306/combats:$VERSION
gcloud docker push gcr.io/strategos-151306/combats:$VERSION
kubectl set image deployment/combats combats=gcr.io/strategos-151306/combats:$VERSION