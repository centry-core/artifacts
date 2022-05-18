from hurry.filesize import size

from flask_restful import Resource


class API(Resource):
    url_params = [
        '<string:run_id>',
    ]

    def __init__(self, module):
        self.module = module

    def get(self, run_id: str):
        results = self.module.context.rpc_manager.call.security_results_or_404(run_id=run_id)
        minio_client = results.get_minio_client()
        files = minio_client.list_files(results.bucket_name)
        for each in files:
            each["size"] = size(each["size"])
        return files
