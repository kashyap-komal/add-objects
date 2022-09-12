import boto3

session=boto3.Session(
    aws_access_key_id='AKIAXPJK5QCBVLX2SNOQ',
    aws_secret_access_key='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'

)

s3=session.resource('s3')

my_bucket=s3.Bucket('myboto100bucket')

for my_bucket_object in my_bucket.objects.all():
    print(my_bucket_object.key)
