from io import BytesIO

from flask import send_file, abort

from hurry.filesize import size

from ...shared.utils.restApi import RestResource


class ArtifactsForSecurityResults(RestResource):
    def get(self, run_id: str):
        results = self.rpc.security_results_or_404(run_id=run_id)
        minio_client = results.get_minio_client()
        files = minio_client.list_files(results.bucket_name)
        for each in files:
            each["size"] = size(each["size"])
        return files


class ArtifactDownload(RestResource):
    def get(self, run_id: str, filename: str):
        results = self.rpc.security_results_or_404(run_id=run_id)
        minio_client = results.get_minio_client()
        try:
            file = minio_client.download_file(results.bucket_name, filename)
            print(file)
            print(BytesIO(file).getvalue())
            return send_file(BytesIO(file), attachment_filename=filename)
        except:
            abort(404)
