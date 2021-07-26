from flask import request

from hurry.filesize import size

from ...shared.utils.restApi import RestResource
from ...shared.connectors.minio import MinioClient
from ...shared.utils.api_utils import build_req_parser, upload_file


class Artifacts(RestResource):
    delete_rules = (
        dict(name="fname[]", type=str, action="append", location="args"),
    )

    def __init__(self):
        super().__init__()
        self.__init_req_parsers()

    def __init_req_parsers(self):
        self._parser_delete = build_req_parser(rules=self.delete_rules)

    def get(self, project_id: int, bucket: str):
        project = self.rpc.project_get_or_404(project_id=project_id)
        c = MinioClient(project)
        files = c.list_files(bucket)
        for each in files:
            each["size"] = size(each["size"])
        return {"total": len(files), "rows": files}

    def post(self, project_id: int, bucket: str):
        project = self.rpc.project_get_or_404(project_id=project_id)
        if "file" in request.files:
            upload_file(bucket, request.files["file"], project)
        return {"message": "Done", "code": 200}

    def delete(self, project_id: int, bucket: str):
        args = self._parser_delete.parse_args(strict=False)
        project = self.rpc.project_get_or_404(project_id=project_id)
        if not args.get("fname[]"):
            MinioClient(project=project).remove_bucket(bucket)
        else:
            for filename in args.get("fname[]", ()) or ():
                MinioClient(project=project).remove_file(bucket, filename)
        return {"message": "Deleted", "code": 200}
