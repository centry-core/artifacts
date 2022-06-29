from io import BytesIO

from flask import send_file, abort

from flask_restful import Resource

from tools import MinioClient


class API(Resource):
    url_params = [
        '<int:run_id>/<string:filename>',
    ]

    def __init__(self, module):
        self.module = module

    def get(self, run_id: int, filename: str):
        test_data = self.module.context.rpc_manager.call.backend_results_or_404(run_id=run_id).to_json()
        project = self.module.context.rpc_manager.call.project_get_or_404(project_id=test_data["project_id"])
        minio_client = MinioClient(project)
        bucket_name = str(test_data["name"]).replace("_", "").replace(" ", "").lower()
        try:
            file = minio_client.download_file(bucket_name, filename)
            return send_file(BytesIO(file), attachment_filename=filename)
        except:
            abort(404)
