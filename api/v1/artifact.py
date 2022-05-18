from flask import send_file
from io import BytesIO

from flask_restful import Resource
from ....shared.tools.minio_client import MinioClient


class API(Resource):
    url_params = [
        '<int:project_id>/<string:bucket>/<string:filename>',
    ]

    def __init__(self, module):
        self.module = module

    def get(self, project_id: int, bucket: str, filename: str):
        project = self.module.context.rpc_manager.call.project_get_or_404(project_id=project_id)
        fobj = MinioClient(project).download_file(bucket, filename)
        return send_file(BytesIO(fobj), attachment_filename=filename)
