# pylint: disable=C0116
#
#   Copyright 2024 getcarrier.io
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

""" S3 Bucket Operations Handler """

from datetime import datetime
from flask import Response

from pylon.core.tools import log
from tools import MinioClient

from ..responses import (
    list_buckets_response,
    create_bucket_response,
    delete_response,
    head_response,
    error_response
)


class BucketHandler:
    """Handler for S3 bucket operations"""

    def __init__(self, project, owner_id: int = None, owner_name: str = None):
        """
        Initialize the bucket handler with project context.

        Args:
            project: The project object for MinioClient
            owner_id: User ID for owner info in responses
            owner_name: User name for owner info in responses
        """
        self.project = project
        self.owner_id = owner_id or 0
        self.owner_name = owner_name or ''
        self.mc = MinioClient(project)

    def list_buckets(self) -> Response:
        """
        List all buckets for the authenticated project.

        S3 Operation: GET /
        """
        try:
            buckets = self.mc.list_bucket()

            # Build bucket list with metadata
            bucket_list = []
            for bucket_name in buckets:
                bucket_list.append({
                    'name': bucket_name,
                    'creation_date': datetime.utcnow()  # MinIO doesn't track creation date
                })

            return list_buckets_response(
                buckets=bucket_list,
                owner_id=str(self.owner_id),
                owner_display_name=self.owner_name
            )

        except Exception as e:
            log.error("ListBuckets failed: %s", e)
            return error_response(
                code='InternalError',
                message=str(e),
                status_code=500
            )

    def create_bucket(self, bucket_name: str) -> Response:
        """
        Create a new bucket.

        S3 Operation: PUT /{bucket}
        """
        try:
            # Check if bucket already exists
            existing_buckets = self.mc.list_bucket()
            if bucket_name in existing_buckets:
                return error_response(
                    code='BucketAlreadyExists',
                    message=f'Bucket {bucket_name} already exists',
                    resource=f'/{bucket_name}',
                    status_code=409
                )

            # Create the bucket
            result = self.mc.create_bucket(bucket=bucket_name, bucket_type='local')

            if isinstance(result, dict) and result.get('error'):
                return error_response(
                    code='InternalError',
                    message=result['error'],
                    resource=f'/{bucket_name}',
                    status_code=500
                )

            location = result.get('Location', f'/{self.mc.format_bucket_name(bucket_name)}')
            return create_bucket_response(location)

        except Exception as e:
            log.error("CreateBucket failed: %s", e)
            return error_response(
                code='InternalError',
                message=str(e),
                resource=f'/{bucket_name}',
                status_code=500
            )

    def delete_bucket(self, bucket_name: str) -> Response:
        """
        Delete a bucket.

        S3 Operation: DELETE /{bucket}

        Note: Bucket must be empty before deletion.
        """
        try:
            # Check if bucket exists
            existing_buckets = self.mc.list_bucket()
            if bucket_name not in existing_buckets:
                return error_response(
                    code='NoSuchBucket',
                    message=f'Bucket {bucket_name} does not exist',
                    resource=f'/{bucket_name}',
                    status_code=404
                )

            # Check if bucket is empty
            files = self.mc.list_files(bucket_name)
            if files:
                return error_response(
                    code='BucketNotEmpty',
                    message='The bucket you tried to delete is not empty',
                    resource=f'/{bucket_name}',
                    status_code=409
                )

            # Delete the bucket
            self.mc.remove_bucket(bucket_name)
            return delete_response()

        except Exception as e:
            log.error("DeleteBucket failed: %s", e)
            return error_response(
                code='InternalError',
                message=str(e),
                resource=f'/{bucket_name}',
                status_code=500
            )

    def head_bucket(self, bucket_name: str) -> Response:
        """
        Check if a bucket exists.

        S3 Operation: HEAD /{bucket}

        Returns 200 if bucket exists, 404 if not.
        """
        try:
            # Check if bucket exists
            existing_buckets = self.mc.list_bucket()
            if bucket_name not in existing_buckets:
                return error_response(
                    code='NoSuchBucket',
                    message=f'Bucket {bucket_name} does not exist',
                    resource=f'/{bucket_name}',
                    status_code=404
                )

            return head_response()

        except Exception as e:
            log.error("HeadBucket failed: %s", e)
            return error_response(
                code='InternalError',
                message=str(e),
                resource=f'/{bucket_name}',
                status_code=500
            )

    def get_bucket_location(self, bucket_name: str, region: str = 'us-east-1') -> Response:
        """
        Get bucket location (region).

        S3 Operation: GET /{bucket}?location
        """
        try:
            # Check if bucket exists
            existing_buckets = self.mc.list_bucket()
            if bucket_name not in existing_buckets:
                return error_response(
                    code='NoSuchBucket',
                    message=f'Bucket {bucket_name} does not exist',
                    resource=f'/{bucket_name}',
                    status_code=404
                )

            # Return location constraint XML
            from xml.etree.ElementTree import Element, tostring
            root = Element('LocationConstraint', xmlns='http://s3.amazonaws.com/doc/2006-03-01/')
            root.text = region

            xml_str = b'<?xml version="1.0" encoding="UTF-8"?>\n' + tostring(root, encoding='utf-8')
            return Response(xml_str, status=200, mimetype='application/xml')

        except Exception as e:
            log.error("GetBucketLocation failed: %s", e)
            return error_response(
                code='InternalError',
                message=str(e),
                resource=f'/{bucket_name}',
                status_code=500
            )
