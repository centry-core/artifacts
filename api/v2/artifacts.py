from flask import request

from hurry.filesize import size
from sqlalchemy.orm import joinedload

from tools import MinioClient, api_tools, auth, db
from pylon.core.tools import log
from ...models.artifact import Artifact
from ...models.folder import ArtifactFolder
from ...utils.utils import delete_artifact_entries


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
        """
        List artifacts and folders in a bucket.
        
        Query params:
            - folder_id: Filter by folder ID (optional, None for root level)
            - configuration_title: S3 configuration to use
        """
        project = self.module.context.rpc_manager.call.project_get_or_404(project_id=project_id)
        configuration_title = request.args.get('configuration_title')
        folder_id = request.args.get('folder_id', type=int)
        
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
            
            # Fetch artifact and folder data from DB
            with db.get_session(project_id) as session:
                # Query artifacts
                artifact_query = session.query(
                    Artifact.filename,
                    Artifact.artifact_id,
                    Artifact.folder_id
                ).filter(
                    Artifact.bucket == bucket
                )
                
                # Query folders - if folder_id is provided, get subfolders; otherwise get root folders
                folder_query = session.query(ArtifactFolder).filter(
                    ArtifactFolder.bucket == bucket
                ).options(joinedload(ArtifactFolder.parent))
                
                if folder_id is not None:
                    # Show subfolders and artifacts in specific folder
                    artifact_query = artifact_query.filter(Artifact.folder_id == folder_id)
                    folder_query = folder_query.filter(ArtifactFolder.parent_id == folder_id)
                else:
                    # Show root-level folders and artifacts
                    folder_query = folder_query.filter(ArtifactFolder.parent_id.is_(None))
                    # If no folder_id specified, show root-level artifacts only
                    artifact_query = artifact_query.filter(Artifact.folder_id.is_(None))
                
                artifacts = artifact_query.all()
                folders = folder_query.all()
                
                # Create filename -> artifact data mapping
                artifact_map = {
                    artifact.filename: {
                        'artifact_id': str(artifact.artifact_id),
                        'folder_id': artifact.folder_id
                    }
                    for artifact in artifacts
                }
                
                # Extract folder data while still in session context
                folder_items = []
                for folder in folders:
                    folder_items.append({
                        "id": folder.id,
                        "folder_id": str(folder.folder_id),
                        "name": folder.name,
                        "type": "folder",
                        "prefix": folder.prefix,  # Access this property inside session
                        "created_at": folder.created_at.isoformat() if folder.created_at else None,
                        "modified": folder.updated_at.isoformat() if folder.updated_at else folder.created_at.isoformat() if folder.created_at else None
                    })
            
            # Filter files to match artifacts in current folder/root
            # S3 stores files by filename only (no folder prefix), folder structure is in DB
            filtered_files = []
            
            # Build lookup for S3 files by name
            s3_files_map = {f["name"]: f for f in files}
            
            # Add all DB artifacts, with S3 metadata if available
            for filename, artifact_data in artifact_map.items():
                s3_file = s3_files_map.get(filename)
                if s3_file:
                    # File exists in S3 - use S3 metadata
                    file_item = {
                        "name": filename,
                        "size": size(s3_file["size"]),
                        "modified": s3_file.get("modified"),
                        "artifact_id": artifact_data.get('artifact_id'),
                        "folder_id": artifact_data.get('folder_id'),
                        "type": "file"
                    }
                else:
                    # File in DB but not in S3 (orphaned) - still show it
                    file_item = {
                        "name": filename,
                        "size": "0 B",  # Unknown size
                        "modified": None,
                        "artifact_id": artifact_data.get('artifact_id'),
                        "folder_id": artifact_data.get('folder_id'),
                        "type": "file",
                        "orphaned": True  # Flag for missing S3 file
                    }
                filtered_files.append(file_item)
            
            # Combine folders and files
            all_items = folder_items + filtered_files
            
            return {
                "retention_policy": retention_policy,
                "total": len(all_items),
                "total_folders": len(folder_items),
                "total_files": len(filtered_files),
                "rows": all_items
            }
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
        """
        Upload file with artifact registration.
        
        Form data:
            - file: File to upload (required)
            - source: Source type ('manual', 'generated', 'attached')
            - prompt: Optional prompt text for generated artifacts
            - folder_id: Optional folder ID to place artifact in
        
        Query params:
            - configuration_title: S3 configuration to use
            - create_if_not_exists: Create bucket if not exists
            - overwrite: Overwrite existing file
        """
        configuration_title = request.args.get('configuration_title')
        
        if "file" not in request.files:
            return {'error': 'No file provided'}, 400

        file = request.files["file"]
        filename = file.filename
        file_data = file.read()
        
        # Get folder_id from form data
        folder_id = request.form.get('folder_id')
        
        if folder_id:
            try:
                folder_id = int(folder_id)
                # Validate folder exists
                with db.get_session(project_id) as session:
                    folder = session.query(ArtifactFolder).filter(
                        ArtifactFolder.id == folder_id,
                        ArtifactFolder.bucket == bucket
                    ).first()
                    if not folder:
                        return {'error': f'Folder with ID {folder_id} not found in bucket {bucket}'}, 404
            except ValueError:
                return {'error': 'Invalid folder_id format'}, 400
        else:
            folder_id = None
        
        try:
            # Call upload_artifact RPC directly within same pylon
            result = self.module.upload_artifact(
                project_id=project_id,
                bucket=bucket,
                filename=filename,
                file_data=file_data,
                source=request.form.get('source', 'manual'),
                prompt=request.form.get('prompt'),
                folder_id=folder_id,
                configuration_title=configuration_title,
                create_if_not_exists=request.args.get('create_if_not_exists', True),
                overwrite=request.args.get('overwrite', 'true').lower() == 'true'
            )
            
            # Build response with appropriate message
            message = "Overwritten" if result.get("was_duplicate") else "Done"
            response = {
                "message": message,
                "ok": True,
                "bucket": result["bucket"],
                "filename": result["filename"],
                "size": result["size"],
                "artifact_id": result["artifact_id"]
            }
            
            return response, 200
            
        except AttributeError as e:
            return {'error': f'Error accessing s3: {configuration_title}'}, 400
        except Exception as e:
            log.error(f"Upload failed: {e}")
            return {'error': str(e)}, 500

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
        """
        args = request.args
        project = self.module.context.rpc_manager.call.project_get_or_404(project_id=project_id)
        configuration_title = args.get('configuration_title')
        
        try:
            mc = MinioClient(project, configuration_title=configuration_title)
        except AttributeError:
            return {'error': f'Error accessing s3: {configuration_title}'}, 400
        
        # Get filenames to delete
        filenames = args.getlist("fname[]")
        
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

    @auth.decorators.check_api({
        "permissions": ["configuration.artifacts.artifacts.edit"],
        "recommended_roles": {
            "administration": {"admin": True, "viewer": False, "editor": True},
            "default": {"admin": True, "viewer": False, "editor": True},
            "developer": {"admin": True, "viewer": False, "editor": True},
        }})
    def put(self, project_id: int, bucket: str):
        """
        Update artifact(s) - currently supports moving artifacts to a different folder.
        
        Request body:
            - artifact_ids: List of artifact IDs to update
            - folder_id: Target folder ID (use null/0 to move to root)
        """
        data = request.json
        
        if not data:
            return {"error": "Request body is required"}, 400
        
        artifact_ids = data.get('artifact_ids', [])
        folder_id = data.get('folder_id')
        
        if not artifact_ids:
            return {"error": "artifact_ids is required"}, 400
        
        # Determine target folder_id
        target_folder_id = None
        
        if folder_id is not None and folder_id != 0:
            # Validate folder exists in the target bucket
            with db.get_session(project_id) as session:
                folder = session.query(ArtifactFolder).filter(
                    ArtifactFolder.id == folder_id,
                    ArtifactFolder.bucket == bucket
                ).first()
                if not folder:
                    return {"error": f"Folder with ID {folder_id} not found in bucket '{bucket}'. Cannot move artifacts to a folder in a different bucket."}, 404
                target_folder_id = folder_id
        # else: target_folder_id remains None (move to root)
        
        try:
            with db.get_session(project_id) as session:
                updated_count = session.query(Artifact).filter(
                    Artifact.bucket == bucket,
                    Artifact.artifact_id.in_(artifact_ids)
                ).update(
                    {Artifact.folder_id: target_folder_id},
                    synchronize_session=False
                )
                session.commit()
            
            return {
                "message": "Updated",
                "updated_count": updated_count
            }, 200
            
        except Exception as e:
            log.error(f"Failed to update artifacts: {e}")
            return {"error": str(e)}, 500


class API(api_tools.APIBase):
    url_params = [
        '<string:project_id>/<string:bucket>',
        '<string:mode>/<string:project_id>/<string:bucket>',
    ]

    mode_handlers = {
        'default': ProjectAPI,
    }
