# IAM Policies and Permissions

This document outlines the required AWS IAM permissions and recommended security configurations for the Fullbay API Ingestion Lambda function.

## 🔐 Required IAM Permissions

### Lambda Execution Role

The Lambda function requires the following permissions, which are automatically created by the SAM template:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "BasicLambdaExecution",
            "Effect": "Allow",
            "Action": [
                "logs:CreateLogGroup",
                "logs:CreateLogStream",
                "logs:PutLogEvents"
            ],
            "Resource": "arn:aws:logs:*:*:*"
        },
        {
            "Sid": "VPCAccess",
            "Effect": "Allow",
            "Action": [
                "ec2:CreateNetworkInterface",
                "ec2:DescribeNetworkInterfaces",
                "ec2:DeleteNetworkInterface",
                "ec2:AttachNetworkInterface",
                "ec2:DetachNetworkInterface"
            ],
            "Resource": "*"
        },
        {
            "Sid": "SecretsManagerAccess",
            "Effect": "Allow",
            "Action": [
                "secretsmanager:GetSecretValue",
                "secretsmanager:DescribeSecret"
            ],
            "Resource": [
                "arn:aws:secretsmanager:${AWS::Region}:${AWS::AccountId}:secret:fullbay-api-credentials*"
            ]
        },
        {
            "Sid": "CloudWatchMetrics",
            "Effect": "Allow",
            "Action": [
                "cloudwatch:PutMetricData"
            ],
            "Resource": "*",
            "Condition": {
                "StringEquals": {
                    "cloudwatch:namespace": "FullbayIngestion"
                }
            }
        }
    ]
}
```

### Deployment User/Role Permissions

For deploying the application with AWS SAM, you need these permissions:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "CloudFormationAccess",
            "Effect": "Allow",
            "Action": [
                "cloudformation:CreateStack",
                "cloudformation:UpdateStack",
                "cloudformation:DeleteStack",
                "cloudformation:DescribeStacks",
                "cloudformation:DescribeStackEvents",
                "cloudformation:DescribeStackResources",
                "cloudformation:GetTemplate",
                "cloudformation:ListStacks",
                "cloudformation:ValidateTemplate"
            ],
            "Resource": [
                "arn:aws:cloudformation:*:*:stack/fullbay-api-ingestion-*/*"
            ]
        },
        {
            "Sid": "LambdaManagement",
            "Effect": "Allow",
            "Action": [
                "lambda:CreateFunction",
                "lambda:UpdateFunctionCode",
                "lambda:UpdateFunctionConfiguration",
                "lambda:DeleteFunction",
                "lambda:GetFunction",
                "lambda:ListFunctions",
                "lambda:AddPermission",
                "lambda:RemovePermission",
                "lambda:TagResource",
                "lambda:UntagResource"
            ],
            "Resource": [
                "arn:aws:lambda:*:*:function:fullbay-api-ingestion-*"
            ]
        },
        {
            "Sid": "IAMRoleManagement",
            "Effect": "Allow",
            "Action": [
                "iam:CreateRole",
                "iam:UpdateRole",
                "iam:DeleteRole",
                "iam:GetRole",
                "iam:PassRole",
                "iam:AttachRolePolicy",
                "iam:DetachRolePolicy",
                "iam:PutRolePolicy",
                "iam:DeleteRolePolicy",
                "iam:TagRole",
                "iam:UntagRole"
            ],
            "Resource": [
                "arn:aws:iam::*:role/fullbay-ingestion-role-*"
            ]
        },
        {
            "Sid": "EventBridge",
            "Effect": "Allow",
            "Action": [
                "events:PutRule",
                "events:DeleteRule",
                "events:PutTargets",
                "events:RemoveTargets",
                "events:DescribeRule"
            ],
            "Resource": [
                "arn:aws:events:*:*:rule/fullbay-*"
            ]
        },
        {
            "Sid": "CloudWatchLogs",
            "Effect": "Allow",
            "Action": [
                "logs:CreateLogGroup",
                "logs:DeleteLogGroup",
                "logs:DescribeLogGroups",
                "logs:PutRetentionPolicy",
                "logs:TagLogGroup"
            ],
            "Resource": [
                "arn:aws:logs:*:*:log-group:/aws/lambda/fullbay-api-ingestion-*"
            ]
        },
        {
            "Sid": "CloudWatchAlarms",
            "Effect": "Allow",
            "Action": [
                "cloudwatch:PutMetricAlarm",
                "cloudwatch:DeleteAlarms",
                "cloudwatch:DescribeAlarms"
            ],
            "Resource": [
                "arn:aws:cloudwatch:*:*:alarm:fullbay-ingestion-*"
            ]
        },
        {
            "Sid": "S3SAMBucket",
            "Effect": "Allow",
            "Action": [
                "s3:CreateBucket",
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::aws-sam-cli-managed-default-samclisourcebucket-*",
                "arn:aws:s3:::aws-sam-cli-managed-default-samclisourcebucket-*/*"
            ]
        }
    ]
}
```

## 🏗️ Infrastructure Permissions

### RDS Setup Permissions

To create and manage RDS resources:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "RDSManagement",
            "Effect": "Allow",
            "Action": [
                "rds:CreateDBInstance",
                "rds:DeleteDBInstance",
                "rds:DescribeDBInstances",
                "rds:ModifyDBInstance",
                "rds:CreateDBParameterGroup",
                "rds:DeleteDBParameterGroup",
                "rds:DescribeDBParameterGroups",
                "rds:CreateDBSubnetGroup",
                "rds:DeleteDBSubnetGroup",
                "rds:DescribeDBSubnetGroups",
                "rds:AddTagsToResource"
            ],
            "Resource": "*"
        }
    ]
}
```

### VPC and Networking Permissions

For VPC configuration:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "VPCConfiguration",
            "Effect": "Allow",
            "Action": [
                "ec2:CreateSecurityGroup",
                "ec2:DeleteSecurityGroup",
                "ec2:DescribeSecurityGroups",
                "ec2:AuthorizeSecurityGroupIngress",
                "ec2:AuthorizeSecurityGroupEgress",
                "ec2:RevokeSecurityGroupIngress",
                "ec2:RevokeSecurityGroupEgress",
                "ec2:DescribeVpcs",
                "ec2:DescribeSubnets",
                "ec2:CreateTags"
            ],
            "Resource": "*"
        }
    ]
}
```

### Secrets Manager Permissions

For managing secrets:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "SecretsManagement",
            "Effect": "Allow",
            "Action": [
                "secretsmanager:CreateSecret",
                "secretsmanager:UpdateSecret",
                "secretsmanager:DeleteSecret",
                "secretsmanager:DescribeSecret",
                "secretsmanager:GetSecretValue",
                "secretsmanager:PutSecretValue",
                "secretsmanager:TagResource"
            ],
            "Resource": [
                "arn:aws:secretsmanager:*:*:secret:fullbay-api-credentials*"
            ]
        }
    ]
}
```

## 🔒 Security Best Practices

### 1. Principle of Least Privilege

- Grant only the minimum permissions required
- Use resource-specific ARNs where possible
- Implement condition blocks to restrict actions

### 2. Environment Separation

Create separate roles for different environments:

```bash
# Development role
fullbay-ingestion-role-development

# Production role  
fullbay-ingestion-role-production
```

### 3. Resource Tagging

Tag all resources for better management:

```json
{
    "Environment": "production",
    "Project": "fullbay-api-ingestion",
    "Owner": "data-team",
    "CostCenter": "engineering"
}
```

### 4. Cross-Account Access

For cross-account deployments, use assume role patterns:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "AWS": "arn:aws:iam::SOURCE-ACCOUNT:role/DeploymentRole"
            },
            "Action": "sts:AssumeRole",
            "Condition": {
                "StringEquals": {
                    "sts:ExternalId": "unique-external-id"
                }
            }
        }
    ]
}
```

## 📋 Policy Templates

### Complete Deployment Policy

Combine all deployment permissions into a single policy:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        // Include all statements from above deployment sections
    ]
}
```

### Monitoring and Debugging Policy

Additional permissions for troubleshooting:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "MonitoringAccess",
            "Effect": "Allow",
            "Action": [
                "logs:DescribeLogGroups",
                "logs:DescribeLogStreams",
                "logs:GetLogEvents",
                "logs:FilterLogEvents",
                "cloudwatch:GetMetricStatistics",
                "cloudwatch:GetMetricData",
                "cloudwatch:ListMetrics",
                "lambda:GetFunction",
                "lambda:GetFunctionConfiguration",
                "lambda:InvokeFunction"
            ],
            "Resource": [
                "arn:aws:logs:*:*:log-group:/aws/lambda/fullbay-api-ingestion-*",
                "arn:aws:lambda:*:*:function:fullbay-api-ingestion-*"
            ]
        }
    ]
}
```

## 🚨 Security Considerations

### 1. Secrets Rotation

Implement regular rotation of:
- Fullbay API keys
- Database passwords
- AWS access keys (if using IAM users)

### 2. Network Security

- Deploy Lambda in private subnets
- Use NAT Gateway for outbound internet access
- Restrict RDS to private subnets only
- Configure security groups to allow only necessary traffic

### 3. Encryption

- Enable encryption at rest for RDS
- Use encrypted S3 buckets for SAM artifacts
- Enable CloudWatch logs encryption

### 4. Monitoring

- Enable CloudTrail for API call logging
- Set up CloudWatch alarms for security events
- Monitor failed authentication attempts

## 📝 Policy Validation

### Testing Permissions

Use AWS CLI to test permissions:

```bash
# Test Lambda invocation
aws lambda invoke --function-name fullbay-api-ingestion-dev test-output.json

# Test Secrets Manager access
aws secretsmanager get-secret-value --secret-id fullbay-api-credentials

# Test CloudWatch logs access
aws logs describe-log-groups --log-group-name-prefix "/aws/lambda/fullbay"
```

### IAM Policy Simulator

Use the IAM Policy Simulator to validate permissions:
1. Go to AWS Console → IAM → Policy Simulator
2. Select the role/user
3. Test specific actions and resources
4. Verify expected results

## 🔧 Troubleshooting Permissions

### Common Permission Errors

**1. AccessDeniedError during deployment**
```
Error: User: arn:aws:iam::123456789012:user/deployer is not authorized 
to perform: cloudformation:CreateStack
```
- Add CloudFormation permissions to deployment user

**2. Lambda execution failures**
```
Error: An error occurred (AccessDeniedException) when calling the 
GetSecretValue operation
```
- Verify Secrets Manager permissions in Lambda execution role

**3. VPC connectivity issues**
```
Error: Unable to connect to the database
```
- Check VPC configuration and security groups
- Ensure Lambda has VPC permissions

### Debug Steps

1. Check CloudTrail logs for denied API calls
2. Use IAM Access Analyzer for policy recommendations
3. Test permissions with AWS CLI
4. Verify resource ARNs in policies match actual resources