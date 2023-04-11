from hurry.filesize import size

from flask_restful import Resource
from pylon.core.tools import web, log  # pylint: disable=E0611,E0401
from tools import MinioClient


class API(Resource):
    url_params = [
        '<int:result_id>',
    ]

    def __init__(self, module):
        self.module = module

    def get(self, result_id: int):
        test_data = self.module.context.rpc_manager.call.backend_results_or_404(run_id=result_id).to_json()

        project = self.module.context.rpc_manager.call.project_get_or_404(project_id=test_data["project_id"])
        minio_client = MinioClient(project)
        bucket_name = str(test_data["name"]).replace("_", "").replace(" ", "").lower()
        files = minio_client.list_files(bucket_name)
        _files = []
        for each in files:
            if each["name"] == f'{test_data["build_id"]}.log':
                each["size"] = size(each["size"])
                _files.append(each)
        return _files
