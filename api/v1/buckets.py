import re
from datetime import datetime

from dateutil.relativedelta import relativedelta
from werkzeug.exceptions import Forbidden
from hurry.filesize import size

from flask import request

from pylon.core.tools import log
from tools import MinioClient, api_tools, auth


def calculate_retention_days(project, expiration_value, expiration_measure):
    data_retention_limit = project.get_data_retention_limit()
    days = data_retention_limit or None
    if expiration_value and expiration_measure:
        today_date = datetime.today().date()
        expiration_date = today_date + relativedelta(**{expiration_measure: int(expiration_value)})
        time_delta = expiration_date - today_date
        days = time_delta.days
        if data_retention_limit != -1 and days > data_retention_limit:
            raise Forbidden(description="The data retention limit allowed in the project has been exceeded")
    return days


class ProjectAPI(api_tools.APIModeHandler):
    @auth.decorators.check_api(["configuration.artifacts.artifacts.view"])
    def get(self, project_id: int):
        project = self.module.context.rpc_manager.call.project_get_or_404(project_id=project_id)
        configuration_title = request.args.get('configuration_title')
        try:
            mc = MinioClient(project, configuration_title=configuration_title)
        except AttributeError:
            return {'error': f'Error accessing s3: {configuration_title}'}, 400
        buckets = mc.list_bucket()
        rows = []
        for bucket in buckets:
            bucket_size = mc.get_bucket_size(bucket)
            response = mc.get_bucket_tags(bucket)
            tags = {tag['Key']: tag['Value'] for tag in response['TagSet']} if response else {}
            rows.append(dict(name=bucket,
                             tags=tags,
                             size=size(bucket_size),
                             # id=f"p--{project_id}.{bucket}"
                             id=mc.format_bucket_name(bucket)
                             ),
                        )
        return {"total": len(buckets), "rows": rows}, 200

    @auth.decorators.check_api(["configuration.artifacts.artifacts.create"])
    def post(self, project_id: int):
        args = request.json
        bucket = args.get("name").replace("_", "").replace(" ", "").lower()
        if not bucket:
            return {"message": "Name of bucket not provided"}, 400

        # regular expression to validate bucket name
        # ^[a-zA-Z] ensures the name starts with a letter
        # [a-zA-Z0-9-]* ensures the rest of the name contains only letters, numbers, and hyphens
        bucket_pattern = r"^[a-zA-Z][a-zA-Z0-9-]*$"

        if not re.match(bucket_pattern, bucket):
            return {
                "message": "Invalid bucket name. Bucket name must start with a "
                "letter and contain only letters, numbers, and hyphens."
            }, 400

        expiration_measure = args.get("expiration_measure")
        expiration_value = args.get("expiration_value")
        configuration_title = request.args.get('configuration_title')

        project = self.module.context.rpc_manager.call.project_get_or_404(project_id=project_id)
        try:
            mc = MinioClient(project, configuration_title=configuration_title)
        except AttributeError:
            return {'error': f'Error accessing s3: {configuration_title}'}, 400

        bucket_names = mc.list_bucket()
        if bucket in bucket_names:
            return {"message": f"Bucket with name {bucket} already exists"}, 400

        days = calculate_retention_days(project, expiration_value, expiration_measure)
        response = mc.create_bucket(bucket=bucket, bucket_type='local')
        if isinstance(response, dict) and response['Location'] and days:
            mc.configure_bucket_lifecycle(bucket=bucket, days=days)
            return {
                "message": "Created",
                "id": response['Location'].lstrip('/'),
                'name': mc.purify_bucket_name(bucket)
            }, 200
        else:
            return {"message": response}, 400

    @auth.decorators.check_api({
        "permissions": ["configuration.artifacts.artifacts.edit"],
        "recommended_roles": {
            "administration": {"admin": True, "viewer": False, "editor": True},
            "default": {"admin": True, "viewer": False, "editor": True},
            "developer": {"admin": True, "viewer": False, "editor": True},
        }})
    def put(self, project_id: int):
        args = request.json
        bucket = args.get("name").replace("_", "").replace(" ", "").lower()
        if not bucket:
            return {"message": "Name of bucket not provided"}, 400
        expiration_measure = args.get("expiration_measure")
        expiration_value = args.get("expiration_value")
        configuration_title = request.args.get('configuration_title')

        project = self.module.context.rpc_manager.call.project_get_or_404(project_id=project_id)
        try:
            mc = MinioClient(project, configuration_title=configuration_title)
        except AttributeError:
            return {'error': f'Error accessing s3: {configuration_title}'}, 400
        days = calculate_retention_days(project, expiration_value, expiration_measure)
        if days:
            try:
                mc.configure_bucket_lifecycle(bucket=bucket, days=days)
                return {"message": f"Updated", "id": f"p--{project_id}.{bucket}"}, 200
            except Exception as e:
                return {"message": str(e), "id": f"p--{project_id}.{bucket}"}, 400
        else:
            return {"message": "The data retention limit not specify or provided data is not correct"}, 400

    @auth.decorators.check_api(["configuration.artifacts.artifacts.delete"])
    def delete(self, project_id: int):
        configuration_title = request.args.get('configuration_title')
        project = self.module.context.rpc_manager.call.project_get_or_404(project_id=project_id)
        try:
            mc = MinioClient(project, configuration_title=configuration_title)
        except AttributeError:
            return {'error': f'Error accessing s3: {configuration_title}'}, 400
        mc.remove_bucket(request.args["name"])
        return {"message": "Deleted"}, 200



class API(api_tools.APIBase):
    url_params = [
        '<string:project_id>',
        '<string:mode>/<string:project_id>',
    ]

    mode_handlers = {
        'default': ProjectAPI,
        # 'administration': AdminAPI
    }
