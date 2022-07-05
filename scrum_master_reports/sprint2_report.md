# Review of sprint 2

## Set-up Instructions

### 1. Download AWSCLIV2.msi for Windows


### 2. Configure AWS account using Command Prompt
```sh
aws configure sso
SSO start URL [None]: https://<account-name>.awsapps.com/start
SSO Region [None]: eu-west-1
```
Then complete the MFA process using an app like Authy Desktop
```
The only AWS account available to you is: 506555054152
Using the account ID 506555054152
The only role available to you is: GenerationStudentAccess
Using the role name "GenerationStudentAccess"
CLI default client Region [None]: eu-west-1
CLI default output format [None]:
CLI profile name [GenerationStudentAccess-506555054152]: learner-profile
```
To set the account profile to be default:
```sh
export AWS_DEFAULT=learner-profile
```

### 3. Create Lambda Function
Navigate to lambda in aws,go to create a function and choose the 'use a bluepint' option.
Select the s3-get-object-python
Go to configure
Enter a preffered name to create new role for the  function.
Navigate to policy templates and attach 'Amazon S3 object read-only permissions'.
## 3a Set S3 Trigger
Navigate to S3 Trigger Select bucket
Select Event Type AS (PUT)
Function templates appears below
Then create function
Before a connection to be made to the database, our lambda should be connected to the same
VPC in the redshift cluster created for us which is 'redshiftcluter-8pp4d8ut2ly.
After setting that up,we need to make sure we have the necessary polices/permissons in our Role 
So we attached the AmazonEC2FullAccess policy to our role before setting up the VPC.
Now our lambda is connected to the same redshift cluster that connects to our database,we can now devise a way to connect our labda to the data base.
### 4. 


## Completed tasks
-  Ticket :

## Potential improvements


## Tips for readability of code
- Use meaningful and descriptive names (this reduces the number of comments needed to understand the code)
- Before each function/method, write a comment describing what it does: starting scenario and intended end result
- Instead of writing a comment for each line, write a comment for what each block of code is doing


