#!/bin/bash
set -e

TAXI_TYPE=$1 # "yellow"
YEAR=$2 # 2020

# Validate input arguments
if [ -z "$TAXI_TYPE" ] || [ -z "$YEAR" ]; then
  echo "Usage: $0 <taxi_type> <year>"
  echo "Example: $0 yellow 2020"
  exit 1
fi

URL_PREFIX="'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-11.parquet"

for MONTH in {1..12}; do
  FMONTH=$(printf "%02d" ${MONTH})

  URL="${URL_PREFIX}/${TAXI_TYPE}/${TAXI_TYPE}_tripdata_${YEAR}-${FMONTH}.csv.gz"

  LOCAL_PREFIX="data/raw/${TAXI_TYPE}/${YEAR}/${FMONTH}"
  LOCAL_FILE="${TAXI_TYPE}_tripdata_${YEAR}_${FMONTH}.csv.gz"
  LOCAL_PATH="${LOCAL_PREFIX}/${LOCAL_FILE}"

  echo "downloading ${URL} to ${LOCAL_PATH}"
  mkdir -p "${LOCAL_PREFIX}"
  wget "${URL}" -O "${LOCAL_PATH}" || echo "Failed to download ${URL}"

done