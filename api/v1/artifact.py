from flask import send_file, request
from io import BytesIO
from hurry.filesize import size
from pylon.core.tools import log
from botocore.exceptions import ClientError

from tools import MinioClient, MinioClientAdmin, api_tools, auth


class ProjectAPI(api_tools.APIModeHandler):
    @auth.decorators.check_api(["configuration.artifacts.artifacts.view"])
    def get(self, project_id: int, bucket: str, filename: str):
        project = self.module.context.rpc_manager.call.project_get_or_404(project_id=project_id)
        integration_id = request.args.get('integration_id')
        is_local = request.args.get('is_local', '').lower() == 'true'
        try:
            file = MinioClient(project, integration_id, is_local).download_file(bucket, filename)
        except:  # pylint: disable=W0702
            log.warning('File %s/%s was not found in project bucket. Looking in admin...', bucket, filename)
            file = MinioClientAdmin().download_file(bucket, filename, project_id)
        try:
            return send_file(BytesIO(file), attachment_filename=filename)
        except TypeError:  # new flask
            return send_file(BytesIO(file), download_name=filename, as_attachment=False)

    @auth.decorators.check_api(["configuration.artifacts.artifacts.delete"])
    def delete(self, project_id: int, bucket: str, filename: str):
        project = self.module.context.rpc_manager.call.project_get_or_404(project_id=project_id)
        integration_id = request.args.get('integration_id')
        is_local = request.args.get('is_local', '').lower() == 'true'
        c = MinioClient(project, integration_id, is_local)
        c.remove_file(bucket, filename)
        return {"message": "Deleted", "size": size(c.get_bucket_size(bucket))}, 200


class AdminAPI(api_tools.APIModeHandler):
    @auth.decorators.check_api(["configuration.artifacts.artifacts.view"])
    def get(self, bucket: str, filename: str, **kwargs):
        integration_id = request.args.get('integration_id')
        file = MinioClientAdmin(integration_id).download_file(bucket, filename)
        try:
            return send_file(BytesIO(file), attachment_filename=filename)
        except TypeError:  # new flask
            return send_file(BytesIO(file), download_name=filename, as_attachment=False)

    @auth.decorators.check_api(["configuration.artifacts.artifacts.delete"])
    def delete(self, bucket: str, filename: str, **kwargs):
        integration_id = request.args.get('integration_id')
        c = MinioClientAdmin(integration_id)
        c.remove_file(bucket, filename)
        return {"message": "Deleted", "size": size(c.get_bucket_size(bucket))}, 200


class API(api_tools.APIBase):
    url_params = [
        '<string:project_id>/<string:bucket>/<string:filename>',
        '<string:mode>/<string:project_id>/<string:bucket>/<string:filename>',
    ]

    mode_handlers = {
        'default': ProjectAPI,
        'administration': AdminAPI,
    }
