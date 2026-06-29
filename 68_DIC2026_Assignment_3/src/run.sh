#!/usr/bin/env bash
set -e

export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=us-east-1
export MINISTACK_ENDPOINT=http://localhost:4566

AWS="aws --endpoint-url=${MINISTACK_ENDPOINT}"
LAMBDA_TIMEOUT=60

RAW_BUCKET=assignment3-raw-reviews
PREPROCESSED_BUCKET=assignment3-preprocessed-reviews
PROFANITY_BUCKET=assignment3-profanity-checked-reviews
ANALYZED_BUCKET=assignment3-analyzed-reviews
USERS_TABLE=assignment3-users
RESULTS_TABLE=assignment3-results

create_bucket_if_missing() {
  local bucket_name="$1"
  ${AWS} s3 mb "s3://${bucket_name}" 2>/dev/null || true
}

delete_lambda_if_exists() {
  local function_name="$1"
  ${AWS} lambda delete-function --function-name "${function_name}" 2>/dev/null || true
}

create_bucket_if_missing "${RAW_BUCKET}"
create_bucket_if_missing "${PREPROCESSED_BUCKET}"
create_bucket_if_missing "${PROFANITY_BUCKET}"
create_bucket_if_missing "${ANALYZED_BUCKET}"

${AWS} ssm put-parameter --overwrite --name /assignment3/buckets/raw --type String --value "${RAW_BUCKET}"
${AWS} ssm put-parameter --overwrite --name /assignment3/buckets/preprocessed --type String --value "${PREPROCESSED_BUCKET}"
${AWS} ssm put-parameter --overwrite --name /assignment3/buckets/profanity_checked --type String --value "${PROFANITY_BUCKET}"
${AWS} ssm put-parameter --overwrite --name /assignment3/buckets/analyzed --type String --value "${ANALYZED_BUCKET}"
${AWS} ssm put-parameter --overwrite --name /assignment3/tables/users --type String --value "${USERS_TABLE}"
${AWS} ssm put-parameter --overwrite --name /assignment3/tables/results --type String --value "${RESULTS_TABLE}"

${AWS} dynamodb create-table \
  --table-name "${USERS_TABLE}" \
  --attribute-definitions AttributeName=reviewerID,AttributeType=S \
  --key-schema AttributeName=reviewerID,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST 2>/dev/null || true

${AWS} dynamodb create-table \
  --table-name "${RESULTS_TABLE}" \
  --attribute-definitions AttributeName=reviewID,AttributeType=S \
  --key-schema AttributeName=reviewID,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST 2>/dev/null || true

delete_lambda_if_exists preprocess
delete_lambda_if_exists profanity_check
delete_lambda_if_exists sentiment_analysis

(
  cd lambdas/preprocess
  rm -rf package lambda.zip
  mkdir package
  cp handler.py package/
  pip install --no-compile -r requirements.txt -t package
  find package -type d -name "__pycache__" -prune -exec rm -rf {} +
  cd package
  zip -r ../lambda.zip .
)
${AWS} lambda create-function \
  --function-name preprocess \
  --runtime python3.11 \
  --timeout "${LAMBDA_TIMEOUT}" \
  --zip-file fileb://lambdas/preprocess/lambda.zip \
  --handler handler.handler \
  --role arn:aws:iam::000000000000:role/lambda-role \
  --environment "{\"Variables\":{\"STAGE\":\"local\"}}"

(
  cd lambdas/profanity_check
  rm -rf package lambda.zip
  mkdir package
  cp handler.py package/
  pip install --no-compile -r requirements.txt -t package
  find package -type d -name "__pycache__" -prune -exec rm -rf {} +
  cd package
  zip -r ../lambda.zip .
)
${AWS} lambda create-function \
  --function-name profanity_check \
  --runtime python3.11 \
  --timeout "${LAMBDA_TIMEOUT}" \
  --zip-file fileb://lambdas/profanity_check/lambda.zip \
  --handler handler.handler \
  --role arn:aws:iam::000000000000:role/lambda-role \
  --environment "{\"Variables\":{\"STAGE\":\"local\"}}"

(cd lambdas/sentiment_analysis; rm -f lambda.zip; zip lambda.zip handler.py)
${AWS} lambda create-function \
  --function-name sentiment_analysis \
  --runtime python3.11 \
  --timeout "${LAMBDA_TIMEOUT}" \
  --zip-file fileb://lambdas/sentiment_analysis/lambda.zip \
  --handler handler.handler \
  --role arn:aws:iam::000000000000:role/lambda-role \
  --environment "{\"Variables\":{\"STAGE\":\"local\"}}"

PREPROCESS_ARN=$(${AWS} lambda get-function --function-name preprocess --query 'Configuration.FunctionArn' --output text)
PROFANITY_ARN=$(${AWS} lambda get-function --function-name profanity_check --query 'Configuration.FunctionArn' --output text)
SENTIMENT_ARN=$(${AWS} lambda get-function --function-name sentiment_analysis --query 'Configuration.FunctionArn' --output text)

${AWS} s3api put-bucket-notification-configuration \
  --bucket "${RAW_BUCKET}" \
  --notification-configuration "{\"LambdaFunctionConfigurations\":[{\"LambdaFunctionArn\":\"${PREPROCESS_ARN}\",\"Events\":[\"s3:ObjectCreated:*\"]}]}"

${AWS} s3api put-bucket-notification-configuration \
  --bucket "${PREPROCESSED_BUCKET}" \
  --notification-configuration "{\"LambdaFunctionConfigurations\":[{\"LambdaFunctionArn\":\"${PROFANITY_ARN}\",\"Events\":[\"s3:ObjectCreated:*\"]}]}"

${AWS} s3api put-bucket-notification-configuration \
  --bucket "${PROFANITY_BUCKET}" \
  --notification-configuration "{\"LambdaFunctionConfigurations\":[{\"LambdaFunctionArn\":\"${SENTIMENT_ARN}\",\"Events\":[\"s3:ObjectCreated:*\"]}]}"

echo
echo "MiniStack resources are ready."
echo "Upload one review JSON to s3://${RAW_BUCKET} to start the pipeline."
