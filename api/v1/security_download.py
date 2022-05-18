from io import BytesIO

from flask import send_file, abort

from flask_restful import Resource


class API(Resource):
    url_params = [
        '<string:run_id>/<string:filename>',
    ]

    def __init__(self, module):
        self.module = module

    def get(self, run_id: str, filename: str):
        results = self.module.context.rpc_manager.call.security_results_or_404(run_id=run_id)
        minio_client = results.get_minio_client()
        try:
            file = minio_client.download_file(results.bucket_name, filename)
            print(file)
            print(BytesIO(file).getvalue())
            return send_file(BytesIO(file), attachment_filename=filename)
        except:
            abort(404)
