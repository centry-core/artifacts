"""
API for cross-bucket artifact (single file) move/copy operations.

**REVIEW NEEDED - FEATURE INCOMPLETE**

STATUS: The endpoint is implemented but NOT WORKING due to MinioClient s3_client initialization issue.

ISSUE DESCRIPTION:
When attempting to move/copy files between buckets, the mc.s3_client attribute is None,
causing the error: "'NoneType' object has no attribute 'get_object'"

WHAT WAS ATTEMPTED (2+ hours debugging):
1. ✅ Created endpoint structure with move/copy modes
2. ✅ Added source file validation (checks if file exists and has content > 0 bytes)
3. ✅ Implemented download/upload approach (get_object → put_object) instead of copy_object
4. ✅ Added bucket name formatting with mc.format_bucket_name() for proper prefixing
5. ✅ Added destination verification after upload with rollback on failure
6. ✅ Fixed syntax errors and import issues
7. ❌ **BLOCKER**: mc.s3_client is None when accessed, even though mc.list_bucket() works

CRITICAL PROBLEM:
- MinioClient is initialized successfully: MinioClient(project, configuration_title=configuration_title)
- High-level methods work: mc.list_bucket(), mc.format_bucket_name()
- But mc.s3_client is None when we try to use it for raw boto3 operations
- This prevents us from doing get_object/put_object operations needed to physically move files in S3

QUESTIONS FOR REVIEW:
1. Why is mc.s3_client None? The parent class MinioClientABC.__init__ creates self.s3_client
2. Is there a different way to access the boto3 client from MinioClient?
3. Should we use a different approach (RPC call to a method that has proper S3 access)?
4. Are there existing methods in MinioClient for file copy operations we missed?

BACKGROUND:
- Folder move (folders_move.py) doesn't need S3 operations - folders only exist in DB
- Files must be physically moved in S3 storage when changing buckets
- Previous attempts with copy_object created orphaned files (0 B, not viewable)
- MinioClient methods all use self.s3_client internally, but direct access returns None

NEXT STEPS TO TRY:
1. Debug MinioClient initialization - check if s3_client is created under different attribute name
2. Look for existing RPC methods that handle S3 file operations
3. Consider using MinioClient.s3_client vs accessing as property
4. Check if there's a storage engine configuration issue

Please help resolve the s3_client access issue or suggest alternative approach.
"""

from flask import request
from pylon.core.tools import log
from tools import auth, api_tools, db, MinioClient

from ...models.artifact import Artifact
from ...models.folder import ArtifactFolder


class ProjectAPI(api_tools.APIModeHandler):
    """API handler for cross-bucket artifact move/copy operations."""
    
    @auth.decorators.check_api(["configuration.artifacts.artifacts.create"])
    def post(self, project_id: int):
        """
        Move or copy a single artifact from one bucket to another.
        
        Request body:
            - source_bucket: Source bucket name (required)
            - source_artifact_id: Source artifact UUID (required)
            - destination_bucket: Destination bucket name (required)
            - destination_filename: New filename in destination (optional, defaults to source filename)
            - destination_folder_id: Folder ID in destination bucket (optional)
            - copy_mode: 'move' or 'copy' (optional, defaults to 'move')
        
        Query params:
            - configuration_title: S3 configuration to use
        
        Example:
            POST /api/v2/artifacts/artifact_move/default/2?configuration_title=elitea_s3_storage
            {
                "source_bucket": "bucket1",
                "source_artifact_id": "49ea612b-7328-4ecd-befb-6027008a1a43",
                "destination_bucket": "bucket2",
                "destination_filename": "renamed_file.md",
                "destination_folder_id": 5,
                "copy_mode": "move"
            }
        """
        data = request.json
        configuration_title = request.args.get('configuration_title')
        
        if not data:
            return {"error": "Request body is required"}, 400
        
        source_bucket = data.get('source_bucket')
        source_artifact_id = data.get('source_artifact_id')
        destination_bucket = data.get('destination_bucket')
        destination_filename = data.get('destination_filename')
        destination_folder_id = data.get('destination_folder_id')
        copy_mode = data.get('copy_mode', 'move')
        
        # Validate required fields
        if not source_bucket:
            return {"error": "source_bucket is required"}, 400
        if not source_artifact_id:
            return {"error": "source_artifact_id is required"}, 400
        if not destination_bucket:
            return {"error": "destination_bucket is required"}, 400
        if copy_mode not in ['move', 'copy']:
            return {"error": "copy_mode must be 'move' or 'copy'"}, 400
        
        # Get project and MinioClient
        project = self.module.context.rpc_manager.call.project_get_or_404(project_id=project_id)
        
        try:
            mc = MinioClient(project, configuration_title=configuration_title)
            if not hasattr(mc, 's3_client') or mc.s3_client is None:
                return {'error': 'S3 client not initialized properly'}, 500
        except AttributeError:
            return {'error': f'Error accessing s3: {configuration_title}'}, 400
        
        # Validate buckets exist
        bucket_names = mc.list_bucket()
        if source_bucket not in bucket_names:
            return {"error": f"Source bucket '{source_bucket}' not found"}, 404
        if destination_bucket not in bucket_names:
            return {"error": f"Destination bucket '{destination_bucket}' not found"}, 404
        
        try:
            with db.get_session(project_id) as session:
                # Find source artifact
                source_artifact = session.query(Artifact).filter(
                    Artifact.artifact_id == source_artifact_id,
                    Artifact.bucket == source_bucket
                ).first()
                
                if not source_artifact:
                    return {"error": f"Artifact '{source_artifact_id}' not found in bucket '{source_bucket}'"}, 404
                
                # Determine destination filename
                dest_filename = destination_filename or source_artifact.filename
                
                # Check if destination filename already exists
                existing_artifact = session.query(Artifact).filter(
                    Artifact.bucket == destination_bucket,
                    Artifact.filename == dest_filename
                ).first()
                
                if existing_artifact:
                    return {"error": f"File '{dest_filename}' already exists in bucket '{destination_bucket}'"}, 409
                
                # Validate destination folder if specified
                if destination_folder_id:
                    dest_folder = session.query(ArtifactFolder).filter(
                        ArtifactFolder.id == destination_folder_id,
                        ArtifactFolder.bucket == destination_bucket
                    ).first()
                    if not dest_folder:
                        return {"error": f"Folder with ID {destination_folder_id} not found in bucket '{destination_bucket}'"}, 404
                
                # Check if source file exists and has content in S3 first
                # Format bucket names with prefix for S3 operations
                s3_source_bucket = mc.format_bucket_name(source_bucket)
                s3_dest_bucket = mc.format_bucket_name(destination_bucket)
                
                try:
                    # Get object metadata to verify it exists and has content
                    head_response = mc.s3_client.head_object(Bucket=s3_source_bucket, Key=source_artifact.filename)
                    file_size = head_response.get('ContentLength', 0)
                    
                    # Check if file has actual content
                    if file_size == 0:
                        session.rollback()
                        return {
                            "error": f"Cannot move/copy artifact '{source_artifact.filename}' - file exists but has no content (0 bytes)",
                            "suggestion": "This file is empty or corrupted. Cannot move an empty file."
                        }, 400
                except Exception as check_error:
                    session.rollback()
                    error_msg = str(check_error)
                    if 'NoSuchKey' in error_msg or 'Not Found' in error_msg or '404' in error_msg:
                        return {
                            "error": f"Cannot move/copy artifact '{source_artifact.filename}' - file does not exist in S3 storage (orphaned record)",
                            "suggestion": "This is an orphaned database record. The file was never uploaded or was deleted from storage."
                        }, 400
                    # Other errors - log but continue to attempt copy
                    log.warning(f"Could not verify source file existence: {check_error}")
                
                # Copy file in S3 - download from source and upload to destination
                try:
                    # Download file content from source bucket
                    response = mc.s3_client.get_object(Bucket=s3_source_bucket, Key=source_artifact.filename)
                    file_content = response['Body'].read()
                    
                    # Upload to destination bucket
                    mc.s3_client.put_object(
                        Bucket=s3_dest_bucket,
                        Key=dest_filename,
                        Body=file_content
                    )
                    
                    # Verify the upload succeeded by checking if destination file exists
                    try:
                        mc.s3_client.head_object(Bucket=s3_dest_bucket, Key=dest_filename)
                    except Exception as verify_error:
                        session.rollback()
                        log.error(f"S3 upload reported success but file doesn't exist in destination: {verify_error}")
                        return {
                            "error": "Failed to upload file to S3 storage - upload operation succeeded but file is not accessible in destination",
                            "suggestion": "This may indicate an S3 storage configuration issue."
                        }, 500
                except Exception as s3_error:
                    session.rollback()
                    log.error(f"Failed to download/upload S3 file: {s3_error}")
                    return {"error": f"Failed to move file in S3 storage: {str(s3_error)}"}, 500
                
                # Create new artifact entry in destination
                new_artifact = Artifact(
                    bucket=destination_bucket,
                    filename=dest_filename,
                    file_type=source_artifact.file_type,
                    source=source_artifact.source,
                    folder_id=destination_folder_id,
                    author_id=source_artifact.author_id
                )
                session.add(new_artifact)
                session.flush()  # Get the new artifact_id
                
                # If move mode, delete source
                if copy_mode == "move":
                    # Delete source file from S3
                    try:
                        mc.remove_file(source_bucket, source_artifact.filename)
                    except Exception as delete_error:
                        log.warning(f"Failed to delete source S3 file: {delete_error}")
                    
                    # Delete source artifact record
                    session.delete(source_artifact)
                
                session.commit()
                
                response = {
                    "message": f"Successfully {'moved' if copy_mode == 'move' else 'copied'} artifact to bucket '{destination_bucket}'",
                    "artifact_id": str(new_artifact.artifact_id),
                    "bucket": destination_bucket,
                    "filename": dest_filename,
                    "folder_id": destination_folder_id
                }
                
                return response, 200
                
        except Exception as e:
            log.error(f"Failed to move/copy artifact: {e}")
            return {"error": f"Failed to move/copy artifact: {str(e)}"}, 400


class API(api_tools.APIBase):
    """API registration for artifact move endpoint."""
    url_params = [
        '<int:project_id>',
        '<string:mode>/<int:project_id>',
    ]

    mode_handlers = {
        'default': ProjectAPI,
    }
