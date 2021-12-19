
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
import hashlib

class S3Log:
    """
        write logs to S3 buckets
    """

    def __init__(self, threshold=100, bucket=None, key_prefix="logs"):
        """
            :param threshold: number of log records to keep in memory, before sending them to S3
            :param bucket: name of S3 bucket to used
            :param key_prefix: prefix of key name used to store data in the given S3 bucket

        """

        self.log_counter=0
        self.log_entry_threshold=threshold
        self.s3=boto3.client('s3')
        self.buffer=StringIO()

        self.bucket=bucket
        self.key_prefix=key_prefix

        atexit.register(self.write_logs_to_s3, flush=True)

    def log(self, msg):
        """
            :param msg: message to log
            :type msg: str
        """
        
        self.buffer.write(msg + "\n")
        self.log_counter+=1

        self.write_logs_to_s3()

    def log_payload(self, name, payload_path):
        """
            log payloads to S3

            :param name: part of the key name used to store payload
            :param payload_path: path to file containing the payload
        """
        with open(payload_path, 'rb') as f:
            data=f.read()

        m = hashlib.sha256()
        m.update(data)
        sha256hash=m.digest().hex()
        now=datetime.now()
        key=sha256hash + "_"+ name+"_"+now.strftime("%Y-%m-%d_%H-%M-%S")

        self.s3.put_object(Body=data, Bucket=self.bucket, Key=key)

    def write_logs_to_s3(self, flush=False):
        """
            write cached logs to S3

            :param flush: upload logs, even if threshold is not yet reached
        """
        
        if (self.log_counter > self.log_entry_threshold or flush) and len(self.buffer.getvalue()) > 0:

            now=datetime.now()
            key= self.key_prefix+"_"+now.strftime("%Y-%m-%d_%H:%M:%S")+".json"
            self.s3.put_object(Body=self.buffer.getvalue(), Bucket=self.bucket, Key=key)
            self.log_counter=0
            self.buffer=StringIO()