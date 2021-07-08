class connectToS3():
    def __init__(self, ):

        # Setting up connection to s3 resources from boto3.
        import boto3
        self.s3_resource = boto3.resource('s3')
        self.s3_client = boto3.client('s3')
