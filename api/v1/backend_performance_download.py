from io import BytesIO

from flask import send_file, abort

from flask_restful import Resource

from tools import MinioClient, auth


class API(Resource):
    url_params = [
        '<int:run_id>/<string:filename>',
    ]

    def __init__(self, module):
        self.module = module
        
    @auth.decorators.check_api(["configuration.artifacts.artifacts.view"])
    def get(self, run_id: int, filename: str):
        test_data = self.module.context.rpc_manager.call.backend_results_or_404(run_id=run_id).to_json()
        project = self.module.context.rpc_manager.call.project_get_or_404(project_id=test_data["project_id"])
        integration_id = test_data['test_config'].get(
            'integrations', {}).get('system', {}).get('s3_integration', {}).get('id')
        is_local = test_data['test_config'].get(
            'integrations', {}).get('system', {}).get('s3_integration', {}).get('is_local')
        minio_client = MinioClient(project, integration_id, is_local)
        bucket_name = str(test_data["name"]).replace("_", "").replace(" ", "").lower()
        try:
            file = minio_client.download_file(bucket_name, filename)
            try:
                return send_file(BytesIO(file), attachment_filename=filename)
            except TypeError:  # new flask
                return send_file(BytesIO(file), download_name=filename, as_attachment=True)
        except:
            abort(404)
