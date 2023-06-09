from hurry.filesize import size
from flask import request
from flask_restful import Resource

from tools import auth

class API(Resource):
    url_params = [
        '<string:run_id>',
    ]

    def __init__(self, module):
        self.module = module

    @auth.decorators.check_api(["configuration.artifacts.artifacts.view"])
    def get(self, run_id: str):
        test_type = request.args.get('test_type')
        if test_type == "sast":
            results = self.module.context.rpc_manager.call.security_sast_results_or_404(run_id=run_id)
        elif test_type == "dependency":
            results = self.module.context.rpc_manager.call.security_dependency_results_or_404(run_id=run_id)
        else:
            results = self.module.context.rpc_manager.call.security_results_or_404(run_id=run_id)
        minio_client = results.get_minio_client()
        files = minio_client.list_files(results.bucket_name)
        for each in files:
            each["size"] = size(each["size"])
        return files
