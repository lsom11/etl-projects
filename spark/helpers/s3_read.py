import boto3
import mpu

def s3_read(profile_name=None, bucket_name=None, file_name=None):
    """
    Read a file from an S3 source.

    Parameters
    ----------
    source : str
        Path starting with s3://, e.g. 's3://bucket-name/key/foo.bar'
    profile_name : str, optional
        AWS profile

    Returns
    -------
    content : bytes

    botocore.exceptions.NoCredentialsError
        Botocore is not able to find your credentials. Either specify
        profile_name or add the environment variables AWS_ACCESS_KEY_ID,
        AWS_SECRET_ACCESS_KEY and AWS_SESSION_TOKEN.
        See https://boto3.readthedocs.io/en/latest/guide/configuration.html
    """
    session = boto3.Session(profile_name=profile_name)
    s3 = session.client('s3')
    s3_object = s3.get_object(Bucket=bucket_name, Key=file_name)
    body = s3_object['Body']
    return body.read().decode(encoding="utf-8",errors="ignore")