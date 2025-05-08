from flask import request

from hurry.filesize import size

from tools import MinioClient, api_tools, MinioClientAdmin, auth
from pylon.core.tools import log


def calculate_readable_retention_policy(days: int) -> dict:
    if days and days % 365 == 0:
        expiration_measure, expiration_value = 'years', days // 365
    elif days and days % 31 == 0:
        expiration_measure, expiration_value = 'months', days // 31
    elif days and days % 7 == 0:
        expiration_measure, expiration_value = 'weeks', days // 7
    else:
        expiration_measure, expiration_value = 'days', days
    return {
        'expiration_measure': expiration_measure,
        'expiration_value': expiration_value
    }


class ProjectAPI(api_tools.APIModeHandler):
    @auth.decorators.check_api({
        "permissions": ["configuration.artifacts.artifacts.view"],
        "recommended_roles": {
            "administration": {"admin": True, "viewer": True, "editor": True},
            "default": {"admin": True, "viewer": True, "editor": True},
            "developer": {"admin": True, "viewer": True, "editor": True},
        }})
    def get(self, project_id: int, bucket: str):
        project = self.module.context.rpc_manager.call.project_get_or_404(project_id=project_id)
        integration_id = request.args.get('integration_id')
        is_local = request.args.get('is_local', '').lower() == 'true'
        mc = MinioClient(project, integration_id, is_local)
        try:
            lifecycle = mc.get_bucket_lifecycle(bucket)
            retention_policy = calculate_readable_retention_policy(
                days=lifecycle["Rules"][0]['Expiration']['Days']
                )
        except Exception:
            retention_policy = None
        try:
            files = mc.list_files(bucket)
            for each in files:
                each["size"] = size(each["size"])
            return {"retention_policy": retention_policy, "total": len(files), "rows": files}
        except Exception as e:
            return {"error": str(e)}, 400


    @auth.decorators.check_api({
        "permissions": ["configuration.artifacts.artifacts.create"],
        "recommended_roles": {
            "administration": {"admin": True, "viewer": False, "editor": True},
            "default": {"admin": True, "viewer": False, "editor": True},
            "developer": {"admin": True, "viewer": False, "editor": True},
        }})
    def post(self, project_id: int, bucket: str):
        project = self.module.context.rpc_manager.call.project_get_or_404(project_id=project_id)
        integration_id = request.args.get('integration_id')
        is_local = request.args.get('is_local', '').lower() == 'true'
        mc = MinioClient(project, integration_id, is_local)
        if "file" in request.files:
            # log.info('Minio upload api %s ', request.files)
            api_tools.upload_file(
                bucket, request.files["file"], project, integration_id, is_local,
                create_if_not_exists=request.args.get('create_if_not_exists', True)
            )
        return {"message": "Done", "size": size(mc.get_bucket_size(bucket))}, 200

    @auth.decorators.check_api({
        "permissions": ["configuration.artifacts.artifacts.delete"],
        "recommended_roles": {
            "administration": {"admin": True, "viewer": False, "editor": True},
            "default": {"admin": True, "viewer": False, "editor": True},
            "developer": {"admin": True, "viewer": False, "editor": True},
        }})
    def delete(self, project_id: int, bucket: str):
        args = request.args
        project = self.module.context.rpc_manager.call.project_get_or_404(project_id=project_id)
        integration_id = args.get('integration_id')
        is_local = args.get('is_local', '').lower() == 'true'
        c = MinioClient(project, integration_id, is_local)
        if not args.get("fname[]"):
            c.remove_bucket(bucket)
        else:
            for fname in args.getlist("fname[]"):
                c.remove_file(bucket, fname)
        return {"message": "Deleted", "size": size(c.get_bucket_size(bucket))}, 200


class AdminAPI(api_tools.APIModeHandler):
    @auth.decorators.check_api({
        "permissions": ["configuration.artifacts.artifacts.view"],
        "recommended_roles": {
            "administration": {"admin": True, "viewer": True, "editor": True},
            "default": {"admin": True, "viewer": True, "editor": True},
            "developer": {"admin": True, "viewer": True, "editor": True},
        }})
    def get(self, bucket: str, **kwargs):
        integration_id = request.args.get('integration_id')
        mc = MinioClientAdmin(integration_id)
        try:
            lifecycle = mc.get_bucket_lifecycle(bucket)
            retention_policy = calculate_readable_retention_policy(
                days=lifecycle["Rules"][0]['Expiration']['Days']
            )
        except Exception:
            retention_policy = None
        try:
            files = mc.list_files(bucket)
            for each in files:
                each["size"] = size(each["size"])
            return {"retention_policy": retention_policy, "total": len(files), "rows": files}
        except Exception as e:
            return {"error": str(e)}, 400

    @auth.decorators.check_api({
        "permissions": ["configuration.artifacts.artifacts.create"],
        "recommended_roles": {
            "administration": {"admin": True, "viewer": False, "editor": True},
            "default": {"admin": True, "viewer": False, "editor": True},
            "developer": {"admin": True, "viewer": False, "editor": True},
        }})
    def post(self, bucket: str, **kwargs):
        integration_id = request.args.get('integration_id')
        c = MinioClientAdmin(integration_id)
        if "file" in request.files:
            api_tools.upload_file_admin(
                bucket, request.files["file"], integration_id,
                create_if_not_exists=request.args.get('create_if_not_exists', True)
            )
        return {"message": "Done", "size": size(c.get_bucket_size(bucket))}, 200

    @auth.decorators.check_api({
        "permissions": ["configuration.artifacts.artifacts.delete"],
        "recommended_roles": {
            "administration": {"admin": True, "viewer": False, "editor": True},
            "default": {"admin": True, "viewer": False, "editor": True},
            "developer": {"admin": True, "viewer": False, "editor": True},
        }})
    def delete(self, bucket: str, **kwargs):
        args = request.args
        integration_id = args.get('integration_id')
        c = MinioClientAdmin(integration_id)
        if not args.get("fname[]"):
            c.remove_bucket(bucket)
        else:
            for fname in args.getlist("fname[]"):
                c.remove_file(bucket, fname)
        return {"message": "Deleted", "size": size(c.get_bucket_size(bucket))}, 200


class API(api_tools.APIBase):
    url_params = [
        '<string:project_id>/<string:bucket>',
        '<string:mode>/<string:project_id>/<string:bucket>',
    ]

    mode_handlers = {
        'default': ProjectAPI,
        'administration': AdminAPI
    }
