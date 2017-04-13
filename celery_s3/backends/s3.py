import boto3
import logging

from celery.backends.base import KeyValueStoreBackend
from celery.exceptions import ImproperlyConfigured

logger = logging.getLogger(__name__)


def get_client(config):
    if config.get('assume_role_arn'):
        if config.get('aws_secret_access_key') and config.get('aws_access_key_id'):
            sts = boto3.client(
                'sts',
                region_name=config.pop('region', 'us-east-1'),
                aws_access_key_id=config['aws_access_key_id'],
                aws_secret_access_key=config['aws_secret_access_key'],
            )
        else:
            sts = boto3.client('sts')

        role = sts.assume_role(RoleArn=config['assume_role_arn'], RoleSesionName='S3CeleryBackend')

        return boto3.client(
            's3',
            region_name=config.pop('region', 'us-east-1'),
            aws_access_key_id=role['Credentials']['AccessKeyId'],
            aws_secret_access_key=role['Credentials']['SecretAccessKey'],
            aws_session_token=role['Credentials']['SessionToken']
        )

    if config.get('aws_secret_access_key') and config.get('aws_access_key_id'):
        return boto3.client(
            's3',
            region_name=config.pop('region', 'us-east-1'),
            aws_access_key_id=config['aws_access_key_id'],
            aws_secret_access_key=config['aws_secret_access_key'],
        )

    logger.debug('Using on-instance credentials.')
    return boto3.client('s3')


class S3Backend(KeyValueStoreBackend):
    """
    An S3 task result store.
    """
    supports_native_join = False
    implements_incr = False

    bucket_name = None
    base_path = ''

    def __init__(self, **kwargs):
        super(S3Backend, self).__init__(**kwargs)
        config = self.app.conf.get('CELERY_S3_BACKEND_SETTINGS', None)

        if not isinstance(config, dict):
            raise ImproperlyConfigured(
                'S3 backend settings should be grouped in a dict')

        self.bucket_name = config['bucket_name']
        self.base_path = config.get('base_path', self.base_path)
        self.client = get_client(config)

    def _get_key(self, key):
        if self.base_path:
            return '/'.join([self.base_path, key.decode('utf-8')])
        return key

    def get(self, key):
        logger.debug('Retrieving data from S3. Bucket: {0} Key: {1}'.format(self.bucket_name, key))
        key = self._get_key(key)
        response = self.client.get_object(self.bucket_name, key)

        if response:
            return response['Body'].read()

        return None

    def set(self, key, value):
        logger.debug('Persisting data to S3. Bucket: {0} Key: {1}'.format(self.bucket_name, key))
        key = self._get_key(key)
        response = self.client.put_object(
            Bucket=self.bucket_name,
            Key=key,
            Body=value
        )

        return value

    def delete(self, key):
        logger.debug('Removing data from S3. Bucket: {0} Key: {1}'.format(self.bucket_name, key))
        key = self._get_key(key)

        self.client.delete_object(
            Bucket=self.bucket_name,
            Key=key
        )

