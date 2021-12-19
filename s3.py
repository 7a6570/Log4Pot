
try:
    import boto3
except ImportError:
    print(
        "AWS python dependencies (boto3) not installed, logging to S3 storage not available.",
        file=sys.stderr
    )
    s3_import=False

from io import StringIO
from datetime import datetime
import atexit

class S3Log:

    def __init__(self, threshold=100, bucket=None, key_prefix="logs"):
        
        self.log_counter=0
        self.log_entry_threshold=threshold
        self.s3=boto3.client('s3')
        self.buffer=StringIO()

        self.bucket=bucket
        self.key_prefix=key_prefix

        atexit.register(self.write_logs_to_s3, flush=True)

    def log(self, msg):
        self.buffer.write(msg + "\n")
        self.log_counter+=1

        self.write_logs_to_s3()

    def log_payload(self, name, payload_path):

        now=datetime.now()
        key=name+"_"+now.strftime("%Y-%m-%d_%H:%M:%S")
        
        with open(payload_path) as f:
            self.s3.put_object(Body=f, Bucket=self.bucket, Key=key)

    def write_logs_to_s3(self, flush=False):

        if (self.log_counter > self.log_entry_threshold or flush) and len(self.buffer.getvalue()) > 0:

            now=datetime.now()
            key= self.key_prefix+"_"+now.strftime("%Y-%m-%d_%H:%M:%S")+".json"
            self.s3.put_object(Body=self.buffer.getvalue(), Bucket=self.bucket, Key=key)
            self.log_counter=0
            self.buffer=StringIO()


# s3=S3Log(threshold=4, bucket="7ebefe7d5292dc872e16d45d1cd19d268841e893")

# for i in range(1,30):
#     s3.log(f"test {i}")


#import ipdb; ipdb.set_trace()
#res=s3.get_object(Bucket=bucket, Key=key)
#print(res['Body'].read().decode("utf-8"))