"""
API for cross-bucket folder move operations.
"""

from flask import request
from pylon.core.tools import log
from tools import auth, api_tools

from ...utils.folder_utils import move_folder_to_bucket
from tools import MinioClient

# **REVIEW NEEDED - FEATURE INCOMPLETE**
# See artifact_move
# STATUS: The endpoint is implemented but NOT WORKING due to MinioClient s3_client initialization issue.

class ProjectAPI(api_tools.APIModeHandler):
    """API handler for cross-bucket folder move operations."""
    
    @auth.decorators.check_api({
        "permissions": ["configuration.artifacts.artifacts.create"],
        "recommended_roles": {
            "administration": {"admin": True, "viewer": False, "editor": True},
            "default": {"admin": True, "viewer": False, "editor": True},
            "developer": {"admin": True, "viewer": False, "editor": True},
        }
    })
    def post(self, project_id: int):
        """
        Move or copy a folder from one bucket to another, including all contents.
        
        Request body:
            - source_bucket: Source bucket name (required)
            - source_folder_id: Source folder ID (required)
            - destination_bucket: Destination bucket name (required)
            - destination_folder_name: New folder name in destination (optional, defaults to source name)
            - copy_mode: 'move' or 'copy' (optional, defaults to 'move')
        
        Query params:
            - configuration_title: S3 configuration to use
        
        Example:
            POST /api/v2/artifacts/folders_move/default/2?configuration_title=elitea_s3_storage
            {
                "source_bucket": "bucket1",
                "source_folder_id": 123,
                "destination_bucket": "bucket2",
                "destination_folder_name": "renamed-folder",
                "copy_mode": "move"
            }
        """
        data = request.json
        configuration_title = request.args.get('configuration_title')
        
        if not data:
            return {"error": "Request body is required"}, 400
        
        source_bucket = data.get('source_bucket')
        source_folder_id = data.get('source_folder_id')
        destination_bucket = data.get('destination_bucket')
        destination_folder_name = data.get('destination_folder_name')
        copy_mode = data.get('copy_mode', 'move')
        
        # Validate required fields
        if not source_bucket:
            return {"error": "source_bucket is required"}, 400
        if not source_folder_id:
            return {"error": "source_folder_id is required"}, 400
        if not destination_bucket:
            return {"error": "destination_bucket is required"}, 400
        if copy_mode not in ['move', 'copy']:
            return {"error": "copy_mode must be 'move' or 'copy'"}, 400
        
        # Get project and MinioClient
        project = self.module.context.rpc_manager.call.project_get_or_404(project_id=project_id)
        
        try:
            mc = MinioClient(project, configuration_title=configuration_title)
        except AttributeError:
            return {'error': f'Error accessing s3: {configuration_title}'}, 400
        
        # Validate buckets exist
        bucket_names = mc.list_bucket()
        if source_bucket not in bucket_names:
            return {"error": f"Source bucket '{source_bucket}' not found"}, 404
        if destination_bucket not in bucket_names:
            return {"error": f"Destination bucket '{destination_bucket}' not found"}, 404
        
        # Perform move/copy
        success, message, new_folder_id = move_folder_to_bucket(
            project_id=project_id,
            source_bucket=source_bucket,
            source_folder_id=source_folder_id,
            destination_bucket=destination_bucket,
            destination_folder_name=destination_folder_name,
            mc=mc,
            copy_mode=copy_mode
        )
        
        if success:
            return {
                "message": message,
                "folder_id": new_folder_id,
                "bucket": destination_bucket
            }, 200
        else:
            return {"error": message}, 400


class API(api_tools.APIBase):
    """API registration for folder move endpoint."""
    url_params = [
        '<int:project_id>',
        '<string:mode>/<int:project_id>',
    ]

    mode_handlers = {
        'default': ProjectAPI,
    }
