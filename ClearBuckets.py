import sys
import boto3    
s3 = boto3.resource('s3')
silver_bucket = s3.Bucket('YOURPREFIX.progettoaws.silver')
gold_bucket = s3.Bucket('YOURPREFIX.progettoaws.gold')
silver_bucket.objects.all().delete()
gold_bucket.objects.all().delete()