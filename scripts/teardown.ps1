<#
.SYNOPSIS
  Remove everything PharmaSignal created in AWS: empties the data-lake bucket, then deletes the stack
  (bucket, Glue job + bookmark, trigger, catalog database + tables, Athena workgroup, IAM role).
.EXAMPLE
  .\scripts\teardown.ps1            # asks for confirmation
  .\scripts\teardown.ps1 -Force     # no prompt
#>
param(
    [string]$Region = "ap-south-1",
    [string]$StackName = "pharmasignal",
    [switch]$Force
)
$ErrorActionPreference = "Stop"

$bucket = aws cloudformation describe-stacks --stack-name $StackName --region $Region `
    --query "Stacks[0].Outputs[?OutputKey=='BucketName'].OutputValue" --output text
if ($LASTEXITCODE -ne 0 -or -not $bucket) { throw "Stack '$StackName' not found in $Region (or credentials expired: run 'aws login')." }

if (-not $Force) {
    $answer = Read-Host "Permanently delete ALL data in s3://$bucket and stack '$StackName'? Type the bucket name to confirm"
    if ($answer -ne $bucket) { Write-Host "Aborted."; exit 1 }
}

Write-Host "Emptying s3://$bucket ..."
aws s3 rm "s3://$bucket" --recursive --only-show-errors --region $Region
if ($LASTEXITCODE -ne 0) { throw "failed to empty bucket" }

Write-Host "Deleting stack $StackName ..."
aws cloudformation delete-stack --stack-name $StackName --region $Region
aws cloudformation wait stack-delete-complete --stack-name $StackName --region $Region
if ($LASTEXITCODE -ne 0) { throw "stack deletion did not complete; check the CloudFormation console" }

Write-Host "Done. Remaining: Glue run logs in CloudWatch log groups /aws-glue/jobs/* (a few KB; delete in the console if wanted)."
