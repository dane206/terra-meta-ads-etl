#!/usr/bin/env bash
set -e

ENV=${1:-dev}

if [ "$ENV" == "dev" ]; then
  PROJECT="terra-analytics-dev"
elif [ "$ENV" == "prod" ]; then
  PROJECT="terra-analytics-prod"
else
  echo "❌ Unknown environment: $ENV"
  echo "Usage: ./deploy.sh [dev|prod]"
  exit 1
fi

SERVICE="terra-meta-ads-etl"
JOB="${SERVICE}-${ENV}"
IMAGE="gcr.io/${PROJECT}/${JOB}:latest"
REGION="us-central1"

# ── Git check ─────────────────────────────────────────────────────────────────
if ! git diff --quiet || ! git diff --cached --quiet; then
  echo "⚠️  Uncommitted changes detected. Commit before deploying."
  git status --short
  exit 1
fi

echo "🌍 Environment : $ENV"
echo "📦 Project     : $PROJECT"
echo "🐳 Image       : $IMAGE"
echo "🚀 Job         : $JOB"
echo ""

echo "🔨 Building and pushing image via Cloud Build..."
gcloud builds submit \
  --tag "$IMAGE" \
  --project "$PROJECT" \
  .

echo "🚀 Deploying Cloud Run job..."
gcloud run jobs deploy "$JOB" \
  --image "$IMAGE" \
  --region "$REGION" \
  --project "$PROJECT" \
  --command python3 \
  --args meta_ads_to_bigquery.py,--mode,incremental \
  --task-timeout 3600 \
  --memory 512Mi \
  --cpu 1 \
  --set-env-vars "^:^BQ_PROJECT=${PROJECT}:BQ_DATASET=sources:META_INSIGHTS_CHUNK_DAYS=7:META_ACCOUNT_IDS=act_994866890890084,act_2219077071728671,act_461423467875645" \
  --set-secrets "META_ACCESS_TOKEN=meta-access-token:latest,META_APP_ID=meta-app-id:latest,META_APP_SECRET=meta-app-secret:latest" \
  --quiet

echo ""
echo "✅ Deployed $JOB to Cloud Run ($ENV)"
echo ""
echo "To run now:"
echo "  gcloud run jobs execute $JOB --region $REGION --project $PROJECT"
