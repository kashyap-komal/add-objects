import boto3

session=boto3.Session(
    aws_access_key_id='*************',
    aws_secret_access_key='******************************'

)

s3=session.resource('s3')

my_bucket=s3.Bucket('myboto100bucket')

for my_bucket_object in my_bucket.objects.all():
    print(my_bucket_object.key)
