from flask import send_file
from io import BytesIO

from ...shared.utils.restApi import RestResource
from ...shared.connectors.minio import MinioClient


class Artifact(RestResource):
    def get(self, project_id: int, bucket: str, filename: str):
        project = self.rpc.project_get_or_404(project_id=project_id)
        fobj = MinioClient(project).download_file(bucket, filename)
        return send_file(BytesIO(fobj), attachment_filename=filename)
