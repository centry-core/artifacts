from flask import request

from hurry.filesize import size

from tools import MinioClient, api_tools, auth, db
from pylon.core.tools import log
from ...models.artifact import Artifact
from ...utils.utils import create_artifact_entry, delete_artifact_entries, check_artifacts_in_use


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
        configuration_title = request.args.get('configuration_title')
        mc = MinioClient(project, configuration_title=configuration_title)
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
        configuration_title = request.args.get('configuration_title')
        try:
            mc = MinioClient(project, configuration_title=configuration_title)
        except AttributeError:
            return {'error': f'Error accessing s3: {configuration_title}'}, 400

        if "file" not in request.files:
            return {'error': 'No file provided'}, 400

        file = request.files["file"]
        filename = file.filename

        # Upload file to S3
        api_tools.upload_file_base(
            bucket=bucket,
            data=file.read(),
            file_name=filename,
            client=mc,
            create_if_not_exists=request.args.get('create_if_not_exists', True)
        )

        artifact_id = create_artifact_entry(
            project_id=project_id,
            bucket=bucket,
            filename=filename,
            source=request.form.get('source', 'manual'),
            prompt=request.form.get('prompt')
        )

        response = {
            "message": "Done",
            "ok": True,
            "bucket": bucket,
            "filename": filename,
            "size": size(mc.get_bucket_size(bucket)),
            "artifact_id": artifact_id
        }

        return response, 200

    @auth.decorators.check_api({
        "permissions": ["configuration.artifacts.artifacts.delete"],
        "recommended_roles": {
            "administration": {"admin": True, "viewer": False, "editor": True},
            "default": {"admin": True, "viewer": False, "editor": True},
            "developer": {"admin": True, "viewer": False, "editor": True},
        }})
    def delete(self, project_id: int, bucket: str):
        """
        Delete file(s) from bucket with artifact table cleanup.
        
        Query params:
        - fname[]: filename(s) to delete (existing behavior)
        - check_refs: if 'true' (default), check if artifacts are referenced in messages
        
        NEW: Also cleans up artifacts table entries for deleted files.
        NEW: Prevents deletion if artifacts are still referenced (check_refs=true).
        """
        args = request.args
        project = self.module.context.rpc_manager.call.project_get_or_404(project_id=project_id)
        configuration_title = args.get('configuration_title')
        check_refs = args.get('check_refs', 'true').lower() == 'true'
        
        try:
            mc = MinioClient(project, configuration_title=configuration_title)
        except AttributeError:
            return {'error': f'Error accessing s3: {configuration_title}'}, 400
        
        # Get filenames to check
        filenames = args.getlist("fname[]")
        
        # Check if artifacts are still referenced (if check_refs=true)
        if check_refs:
            referenced = check_artifacts_in_use(project_id, bucket, filenames if filenames else None)
            if referenced:
                # Build error message with details
                artifact_ids = list(set([ref['artifact_id'] for ref in referenced]))
                return {
                    'error': 'Cannot delete: artifacts still referenced in messages',
                    'referenced_artifacts': artifact_ids[:10],  # Limit to 10 for response size
                    'reference_count': len(referenced),
                    'hint': 'Use check_refs=false to force delete'
                }, 409
        
        # Delete from S3
        if not filenames:
            mc.remove_bucket(bucket)
            # Clean up all artifacts for this bucket
            delete_artifact_entries(project_id, bucket)
        else:
            # Delete files from S3
            for fname in filenames:
                mc.remove_file(bucket, fname)
            
            # Clean up artifacts table entries
            delete_artifact_entries(project_id, bucket, filenames)
        
        return {"message": "Deleted", "size": size(mc.get_bucket_size(bucket))}, 200




class API(api_tools.APIBase):
    url_params = [
        '<string:project_id>/<string:bucket>',
        '<string:mode>/<string:project_id>/<string:bucket>',
    ]

    mode_handlers = {
        'default': ProjectAPI,
    }
