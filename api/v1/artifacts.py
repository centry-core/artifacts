from flask import request

from hurry.filesize import size

from flask_restful import Resource
# from ....shared.tools.minio_client import MinioClient
# from ....shared.tools.api_tools import build_req_parser, upload_file

from tools import MinioClient, api_tools


class API(Resource):
    url_params = [
        '<int:project_id>/<string:bucket>',
    ]

    def __init__(self, module):
        self.module = module

    def get(self, project_id: int, bucket: str):
        project = self.module.context.rpc_manager.call.project_get_or_404(project_id=project_id)
        c = MinioClient(project)
        files = c.list_files(bucket)
        for each in files:
            each["size"] = size(each["size"])
        return {"total": len(files), "rows": files}

    def post(self, project_id: int, bucket: str):
        project = self.module.context.rpc_manager.call.project_get_or_404(project_id=project_id)
        c = MinioClient(project=project)
        if "file" in request.files:
            api_tools.upload_file(bucket, request.files["file"], project)
        return {"message": "Done", "size": c.get_bucket_size(bucket)}, 200

    def delete(self, project_id: int, bucket: str):
        args = request.args
        project = self.module.context.rpc_manager.call.project_get_or_404(project_id=project_id)
        c = MinioClient(project=project)
        if not args.get("fname[]"):
            c.remove_bucket(bucket)
        else:
            for fname in args.getlist("fname[]"):
                c.remove_file(bucket, fname)
        return {"message": "Deleted", "size": c.get_bucket_size(bucket)}, 200

