<#
.SYNOPSIS
  Deploy PharmaSignal to AWS: CloudFormation stack + Glue job code + reference data.
.EXAMPLE
  .\scripts\deploy.ps1 -BucketName my-pharmasignal-lake
  Credentials come from your default AWS CLI profile; nothing is written to disk.
#>
param(
    [Parameter(Mandatory = $true)][string]$BucketName,
    [string]$Region = "ap-south-1",
    [string]$StackName = "pharmasignal",
    [string]$GlueVersion = "6.0"
)
$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root

function Invoke-Aws {
    & aws @args
    if ($LASTEXITCODE -ne 0) { throw "aws $($args[0]) $($args[1]) failed (exit $LASTEXITCODE)" }
}

$account = aws sts get-caller-identity --query Account --output text
if ($LASTEXITCODE -ne 0) { throw "AWS credentials not valid. Run 'aws login' (or 'aws configure') first." }
Write-Host "Deploying stack '$StackName' to account $account in $Region (bucket: $BucketName)"

Invoke-Aws cloudformation validate-template --template-body file://infra/template.yaml --region $Region | Out-Null
Invoke-Aws cloudformation deploy `
    --stack-name $StackName `
    --template-file infra/template.yaml `
    --parameter-overrides "BucketName=$BucketName" "GlueVersion=$GlueVersion" `
    --capabilities CAPABILITY_NAMED_IAM `
    --tags Project=pharmasignal `
    --no-fail-on-empty-changeset `
    --region $Region

# Package the pure-PySpark module for --extra-py-files (forward-slash zip entries, no __pycache__)
$py = if (Test-Path ".venv\Scripts\python.exe") { ".venv\Scripts\python.exe" } else { "python" }
New-Item -ItemType Directory -Force build | Out-Null
& $py -c "import zipfile, pathlib; z = zipfile.ZipFile('build/etl.zip', 'w', zipfile.ZIP_DEFLATED); [z.write(p, p.as_posix()) for p in sorted(pathlib.Path('etl').glob('*.py'))]; z.close()"
if ($LASTEXITCODE -ne 0) { throw "packaging etl.zip failed" }

Invoke-Aws s3 cp glue/jobs/transform_faers.py "s3://$BucketName/code/transform_faers.py" --region $Region --only-show-errors
Invoke-Aws s3 cp build/etl.zip "s3://$BucketName/code/etl.zip" --region $Region --only-show-errors
Invoke-Aws s3 cp reference/tracked_drugs.csv "s3://$BucketName/reference/tracked_drugs.csv" --region $Region --only-show-errors

Write-Host "`nStack outputs:"
Invoke-Aws cloudformation describe-stacks --stack-name $StackName --region $Region `
    --query "Stacks[0].Outputs[].[OutputKey,OutputValue]" --output table
Write-Host "`nNext: python -m pipeline.run_pipeline --skip-extract"
