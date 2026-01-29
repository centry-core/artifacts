#   Copyright 2025 getcarrier.io
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

""" API for bucket folder CRUD operations """

from typing import Optional

from flask import request

from pylon.core.tools import log
from tools import MinioClient, api_tools, auth, db

from ...models.folder import ArtifactFolder
from ...models.pd.folder import FolderCreate, FolderUpdate
from ...utils.folder_utils import (
    create_folder,
    get_folder,
    list_folders,
    update_folder,
    delete_folder,
    get_folder_contents,
    move_folder_to_bucket
)


def resolve_folder_name_to_id(project_id: int, bucket: str, folder_name: str) -> Optional[int]:
    """Resolve a folder name to its ID within a bucket."""
    with db.get_session(project_id) as session:
        folder = session.query(ArtifactFolder).filter(
            ArtifactFolder.bucket == bucket,
            ArtifactFolder.name == folder_name
        ).first()
        
        return folder.id if folder else None


class ProjectAPI(api_tools.APIModeHandler):
    """API handler for bucket folder operations."""
    
    @auth.decorators.check_api({
        "permissions": ["configuration.artifacts.artifacts.view"],
        "recommended_roles": {
            "administration": {"admin": True, "viewer": True, "editor": True},
            "default": {"admin": True, "viewer": True, "editor": True},
            "developer": {"admin": True, "viewer": True, "editor": True},
        }
    })
    def get(self, project_id: int, bucket: str, folder_id: Optional[int] = None):
        """
        Get folder(s) in a bucket.
        
        If folder_id is provided, returns the folder details and its contents.
        If folder_id is not provided, lists all root folders or folders in specified parent.
        
        Query params:
            - parent_id: Filter by parent folder ID (for listing)
            - include_contents: If true, include folder contents (subfolders + artifacts)
        """
        args = request.args
        parent_id = args.get('parent_id', type=int)
        include_contents = args.get('include_contents', 'false').lower() == 'true'
        
        if folder_id is not None:
            # Get specific folder
            folder = get_folder(project_id, bucket, folder_id)
            
            if not folder:
                return {"error": f"Folder with ID {folder_id} not found"}, 404
            
            if include_contents:
                contents = get_folder_contents(project_id, bucket, folder_id)
                return contents, 200
            
            return folder.model_dump(), 200
        else:
            # List folders
            if include_contents:
                # Return folder contents for root level
                contents = get_folder_contents(project_id, bucket, parent_id)
                return contents, 200
            
            folders = list_folders(project_id, bucket, parent_id)
            return {
                "total": len(folders),
                "rows": [f.model_dump() for f in folders]
            }, 200
    
    @auth.decorators.check_api({
        "permissions": ["configuration.artifacts.artifacts.create"],
        "recommended_roles": {
            "administration": {"admin": True, "viewer": False, "editor": True},
            "default": {"admin": True, "viewer": False, "editor": True},
            "developer": {"admin": True, "viewer": False, "editor": True},
        }
    })
    def post(self, project_id: int, bucket: str):
        """
        Create a new folder in the bucket.
        
        Request body:
            - name: Folder name (required)
            - parent_id: Parent folder ID for nested folders (optional)
        """
        # Validate bucket exists
        project = self.module.context.rpc_manager.call.project_get_or_404(project_id=project_id)
        configuration_title = request.args.get('configuration_title')
        
        try:
            mc = MinioClient(project, configuration_title=configuration_title)
        except AttributeError:
            return {'error': f'Error accessing s3: {configuration_title}'}, 400
        
        bucket_names = mc.list_bucket()
        if bucket not in bucket_names:
            return {"error": f"Bucket '{bucket}' not found"}, 404
        
        # Validate request
        try:
            data = FolderCreate(**request.json)
        except Exception as e:
            return {"error": str(e)}, 400
        
        try:
            folder = create_folder(
                project_id=project_id,
                bucket=bucket,
                name=data.name,
                parent_id=data.parent_id
            )
            
            if folder:
                return {
                    "message": "Created",
                    "folder": folder.model_dump()
                }, 201
            else:
                return {"error": "Failed to create folder"}, 500
                
        except ValueError as e:
            return {"error": str(e)}, 400
        except Exception as e:
            log.error(f"Failed to create folder: {e}")
            return {"error": str(e)}, 500
    
    @auth.decorators.check_api({
        "permissions": ["configuration.artifacts.artifacts.edit"],
        "recommended_roles": {
            "administration": {"admin": True, "viewer": False, "editor": True},
            "default": {"admin": True, "viewer": False, "editor": True},
            "developer": {"admin": True, "viewer": False, "editor": True},
        }
    })
    def put(self, project_id: int, bucket: str, folder_id: Optional[int] = None):
        """
        Update folder details.
        
        Request body:
            - name: New folder name (optional)
            - parent_id: New parent folder ID (optional, for moving folders)
        """
        if folder_id is None:
            return {"error": "Folder ID is required"}, 400
        
        # Validate request
        try:
            data = FolderUpdate(**request.json)
        except Exception as e:
            return {"error": str(e)}, 400
        
        if data.name is None and data.parent_id is None:
            return {"error": "At least one of 'name' or 'parent_id' must be provided"}, 400
        
        try:
            folder = update_folder(
                project_id=project_id,
                bucket=bucket,
                folder_id=folder_id,
                name=data.name,
                parent_id=data.parent_id
            )
            
            if folder:
                return {
                    "message": "Updated",
                    "folder": folder.model_dump()
                }, 200
            else:
                return {"error": "Failed to update folder"}, 500
                
        except ValueError as e:
            return {"error": str(e)}, 400
        except Exception as e:
            log.error(f"Failed to update folder: {e}")
            return {"error": str(e)}, 500
    
    @auth.decorators.check_api({
        "permissions": ["configuration.artifacts.artifacts.delete"],
        "recommended_roles": {
            "administration": {"admin": True, "viewer": False, "editor": True},
            "default": {"admin": True, "viewer": False, "editor": True},
            "developer": {"admin": True, "viewer": False, "editor": True},
        }
    })
    def delete(self, project_id: int, bucket: str, folder_id: Optional[int] = None):
        """
        Delete a folder.
        
        Query params:
            - delete_contents: If 'true', delete all artifacts and subfolders recursively
        """
        if folder_id is None:
            return {"error": "Folder ID is required"}, 400
        
        delete_contents = request.args.get('delete_contents', 'false').lower() == 'true'
        configuration_title = request.args.get('configuration_title')
        
        # Get MinioClient for S3 operations
        project = self.module.context.rpc_manager.call.project_get_or_404(project_id=project_id)
        
        try:
            mc = MinioClient(project, configuration_title=configuration_title)
        except AttributeError:
            mc = None
        
        success, message = delete_folder(
            project_id=project_id,
            bucket=bucket,
            folder_id=folder_id,
            delete_contents=delete_contents,
            mc=mc
        )
        
        if success:
            return {"message": message}, 200
        else:
            return {"error": message}, 400


class API(api_tools.APIBase):
    url_params = [
        '<int:project_id>/<string:bucket>',
        '<int:project_id>/<string:bucket>/<int:folder_id>',
        '<string:mode>/<int:project_id>/<string:bucket>',
        '<string:mode>/<int:project_id>/<string:bucket>/<int:folder_id>',
    ]

    mode_handlers = {
        'default': ProjectAPI,
    }
