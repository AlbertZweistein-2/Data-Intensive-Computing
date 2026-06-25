$ErrorActionPreference = "Stop"

$env:AWS_ACCESS_KEY_ID = "test"
$env:AWS_SECRET_ACCESS_KEY = "test"
$env:AWS_DEFAULT_REGION = "us-east-1"

$MiniStackEndpoint = "http://localhost:4566"

$RawBucket = "assignment3-raw-reviews"
$PreprocessedBucket = "assignment3-preprocessed-reviews"
$ProfanityBucket = "assignment3-profanity-checked-reviews"
$AnalyzedBucket = "assignment3-analyzed-reviews"
$UsersTable = "assignment3-users"
$ResultsTable = "assignment3-results"

function Invoke-Aws {
    py -c "import sys; from awscli.clidriver import main; sys.exit(main())" --endpoint-url=$MiniStackEndpoint @args
}

function Create-BucketIfMissing {
    param([string]$BucketName)
    try {
        Invoke-Aws s3 mb "s3://$BucketName"
    } catch {
        Write-Host "Bucket $BucketName already exists or could not be created again."
    }
}

function Delete-LambdaIfExists {
    param([string]$FunctionName)
    try {
        Invoke-Aws lambda delete-function --function-name $FunctionName
    } catch {
        Write-Host "Lambda $FunctionName does not exist yet."
    }
}

function Create-LambdaZip {
    param(
        [string]$LambdaFolder
    )
    $ZipPath = Join-Path $LambdaFolder "lambda.zip"
    $PackagePath = Join-Path $LambdaFolder "package"
    $RequirementsPath = Join-Path $LambdaFolder "requirements.txt"

    if (Test-Path $ZipPath) {
        Remove-Item $ZipPath
    }
    if (Test-Path $PackagePath) {
        Remove-Item -Recurse -Force $PackagePath
    }

    New-Item -ItemType Directory -Path $PackagePath | Out-Null
    Copy-Item (Join-Path $LambdaFolder "handler.py") (Join-Path $PackagePath "handler.py")

    if (Test-Path $RequirementsPath) {
        py -m pip install -r $RequirementsPath -t $PackagePath
    }

    Compress-Archive -Path (Join-Path $PackagePath "*") -DestinationPath $ZipPath
}

function Write-JsonFile {
    param(
        [string]$Path,
        [object]$Value
    )
    $Json = $Value | ConvertTo-Json -Depth 10 -Compress
    $Utf8WithoutBom = New-Object System.Text.UTF8Encoding $false
    [System.IO.File]::WriteAllText($Path, $Json, $Utf8WithoutBom)
}

Create-BucketIfMissing $RawBucket
Create-BucketIfMissing $PreprocessedBucket
Create-BucketIfMissing $ProfanityBucket
Create-BucketIfMissing $AnalyzedBucket

Invoke-Aws ssm put-parameter --overwrite --name /assignment3/buckets/raw --type String --value $RawBucket
Invoke-Aws ssm put-parameter --overwrite --name /assignment3/buckets/preprocessed --type String --value $PreprocessedBucket
Invoke-Aws ssm put-parameter --overwrite --name /assignment3/buckets/profanity_checked --type String --value $ProfanityBucket
Invoke-Aws ssm put-parameter --overwrite --name /assignment3/buckets/analyzed --type String --value $AnalyzedBucket
Invoke-Aws ssm put-parameter --overwrite --name /assignment3/tables/users --type String --value $UsersTable
Invoke-Aws ssm put-parameter --overwrite --name /assignment3/tables/results --type String --value $ResultsTable

try {
    Invoke-Aws dynamodb create-table `
        --table-name $UsersTable `
        --attribute-definitions "AttributeName=reviewerID,AttributeType=S" `
        --key-schema "AttributeName=reviewerID,KeyType=HASH" `
        --billing-mode PAY_PER_REQUEST
} catch {
    Write-Host "Table $UsersTable already exists or could not be created again."
}

try {
    Invoke-Aws dynamodb create-table `
        --table-name $ResultsTable `
        --attribute-definitions "AttributeName=reviewID,AttributeType=S" `
        --key-schema "AttributeName=reviewID,KeyType=HASH" `
        --billing-mode PAY_PER_REQUEST
} catch {
    Write-Host "Table $ResultsTable already exists or could not be created again."
}

Delete-LambdaIfExists "preprocess"
Delete-LambdaIfExists "profanity_check"
Delete-LambdaIfExists "sentiment_analysis"

Create-LambdaZip "lambdas/preprocess"
Invoke-Aws lambda create-function `
    --function-name preprocess `
    --runtime python3.11 `
    --timeout 20 `
    --zip-file fileb://lambdas/preprocess/lambda.zip `
    --handler handler.handler `
    --role arn:aws:iam::000000000000:role/lambda-role

Create-LambdaZip "lambdas/profanity_check"
Invoke-Aws lambda create-function `
    --function-name profanity_check `
    --runtime python3.11 `
    --timeout 20 `
    --zip-file fileb://lambdas/profanity_check/lambda.zip `
    --handler handler.handler `
    --role arn:aws:iam::000000000000:role/lambda-role

Create-LambdaZip "lambdas/sentiment_analysis"
Invoke-Aws lambda create-function `
    --function-name sentiment_analysis `
    --runtime python3.11 `
    --timeout 20 `
    --zip-file fileb://lambdas/sentiment_analysis/lambda.zip `
    --handler handler.handler `
    --role arn:aws:iam::000000000000:role/lambda-role

$PreprocessArn = Invoke-Aws lambda get-function --function-name preprocess --query 'Configuration.FunctionArn' --output text
$ProfanityArn = Invoke-Aws lambda get-function --function-name profanity_check --query 'Configuration.FunctionArn' --output text
$SentimentArn = Invoke-Aws lambda get-function --function-name sentiment_analysis --query 'Configuration.FunctionArn' --output text

$RawNotificationPath = Join-Path $PWD "raw-notification.json"
$PreprocessedNotificationPath = Join-Path $PWD "preprocessed-notification.json"
$ProfanityNotificationPath = Join-Path $PWD "profanity-notification.json"

Write-JsonFile $RawNotificationPath @{
    LambdaFunctionConfigurations = @(
        @{
            LambdaFunctionArn = $PreprocessArn
            Events = @("s3:ObjectCreated:*")
        }
    )
}

Write-JsonFile $PreprocessedNotificationPath @{
    LambdaFunctionConfigurations = @(
        @{
            LambdaFunctionArn = $ProfanityArn
            Events = @("s3:ObjectCreated:*")
        }
    )
}

Write-JsonFile $ProfanityNotificationPath @{
    LambdaFunctionConfigurations = @(
        @{
            LambdaFunctionArn = $SentimentArn
            Events = @("s3:ObjectCreated:*")
        }
    )
}

Invoke-Aws s3api put-bucket-notification-configuration --bucket $RawBucket --notification-configuration "file://$RawNotificationPath"
Invoke-Aws s3api put-bucket-notification-configuration --bucket $PreprocessedBucket --notification-configuration "file://$PreprocessedNotificationPath"
Invoke-Aws s3api put-bucket-notification-configuration --bucket $ProfanityBucket --notification-configuration "file://$ProfanityNotificationPath"

Write-Host ""
Write-Host "MiniStack resources are ready."
Write-Host "Upload one review JSON to s3://$RawBucket to start the pipeline."
