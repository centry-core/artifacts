from flask import send_file
from io import BytesIO
from hurry.filesize import size

from flask_restful import Resource
# from ....shared.tools.minio_client import MinioClient

from tools import MinioClient, MinioClientAdmin, api_tools


class ProjectAPI(api_tools.APIModeHandler):

    def get(self, project_id: int, bucket: str, filename: str):
        project = self.module.context.rpc_manager.call.project_get_or_404(project_id=project_id)
        file = MinioClient(project).download_file(bucket, filename)
        try:
            return send_file(BytesIO(file), attachment_filename=filename)
        except TypeError:  # new flask
            return send_file(BytesIO(file), download_name=filename, as_attachment=False)

    def delete(self, project_id: int, bucket: str, filename: str):
        project = self.module.context.rpc_manager.call.project_get_or_404(project_id=project_id)
        c = MinioClient(project=project)
        c.remove_file(bucket, filename)
        return {"message": "Deleted", "size": size(c.get_bucket_size(bucket))}, 200


class AdminAPI(api_tools.APIModeHandler):

    def get(self, project_id: int, bucket: str, filename: str):
        file = MinioClientAdmin().download_file(bucket, filename)
        try:
            return send_file(BytesIO(file), attachment_filename=filename)
        except TypeError:  # new flask
            return send_file(BytesIO(file), download_name=filename, as_attachment=False)

    def delete(self, project_id: int, bucket: str, filename: str):
        c = MinioClientAdmin()
        c.remove_file(bucket, filename)
        return {"message": "Deleted", "size": size(c.get_bucket_size(bucket))}, 200


class API(api_tools.APIBase):
    url_params = [
        '<string:mode>/<int:project_id>/<string:bucket>/<string:filename>',
        '<int:project_id>/<string:bucket>/<string:filename>'
    ]

    mode_handlers = {
        'default': ProjectAPI,
        'administration': AdminAPI,
    }
