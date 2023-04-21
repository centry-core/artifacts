from io import BytesIO

from flask import send_file, abort, request
from flask_restful import Resource

from tools import auth

class API(Resource):
    url_params = [
        '<string:run_id>/<string:filename>',
    ]

    def __init__(self, module):
        self.module = module

    @auth.decorators.check_api(["configuration.artifacts.artifacts.view"])
    def get(self, run_id: str, filename: str):
        test_type = request.args.get('test_type')
        if test_type == "sast":
            results = self.module.context.rpc_manager.call.security_sast_results_or_404(run_id=run_id)
        else:
            results = self.module.context.rpc_manager.call.security_results_or_404(run_id=run_id)
        minio_client = results.get_minio_client()
        try:
            file = minio_client.download_file(results.bucket_name, filename)
            try:
                return send_file(BytesIO(file), attachment_filename=filename)
            except TypeError:  # new flask
                return send_file(BytesIO(file), download_name=filename, as_attachment=True)
        except:
            abort(404)
