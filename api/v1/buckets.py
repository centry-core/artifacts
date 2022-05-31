from datetime import datetime
from dateutil.relativedelta import relativedelta
from werkzeug.exceptions import Forbidden
from hurry.filesize import size

from flask import request
from flask_restful import Resource

from tools import MinioClient


class API(Resource):
    url_params = [
        '<int:project_id>',
    ]

    def __init__(self, module):
        self.module = module

    def get(self, project_id: int):
        project = self.module.context.rpc_manager.call.project_get_or_404(project_id=project_id)
        c = MinioClient(project)
        buckets = c.list_bucket()
        rows = []
        for bucket in buckets:
            rows.append(dict(name=bucket, size=size(c.get_bucket_size(bucket))))
        return {"total": len(buckets), "rows": rows}

    def post(self, project_id: int):
        args = request.json
        bucket = args["name"]
        expiration_measure = args["expiration_measure"]
        expiration_value = args["expiration_value"]

        project = self.module.context.rpc_manager.call.project_get_or_404(project_id=project_id)
        data_retention_limit = project.get_data_retention_limit()
        minio_client = MinioClient(project=project)
        days = data_retention_limit or None
        if expiration_value and expiration_measure:
            today_date = datetime.today().date()
            expiration_date = today_date + relativedelta(**{expiration_measure: int(expiration_value)})
            time_delta = expiration_date - today_date
            days = time_delta.days
            if data_retention_limit != -1 and days > data_retention_limit:
                raise Forbidden(description="The data retention limit allowed in the project has been exceeded")
        created = minio_client.create_bucket(bucket)
        if created and days:
            minio_client.configure_bucket_lifecycle(bucket=bucket, days=days)
        return {"message": "Created", "code": 200}

    def delete(self, project_id: int):
        project = self.module.context.rpc_manager.call.project_get_or_404(project_id=project_id)
        MinioClient(project=project).remove_bucket(request.args["name"])
        return {"message": "Deleted", "code": 200}
