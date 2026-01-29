#   Copyright 2021 getcarrier.io
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

""" RPC methods for artifact operations """

from typing import Optional

from hurry.filesize import size
from tools import MinioClient, api_tools, db
from pylon.core.tools import log, web

from ..models.artifact import Artifact
from ..utils.utils import create_artifact_entry


class RPC:
    @web.rpc('artifacts_get_artifact_metadata_by_id', 'get_artifact_metadata_by_id')
    def get_artifact_metadata_by_id(self, artifact_id: str, project_id: int) -> Optional[dict]:
        """
        Get artifact metadata by UUID.

        Args:
            artifact_id: UUID string
            project_id: Project ID for database session

        Returns:
            dict with bucket, filename, type, source, author_id, prompt, created_at
            or None if not found
        """

        try:
            with db.get_session(project_id) as session:
                artifact = session.query(Artifact).filter_by(artifact_id=artifact_id).first()

                if artifact:
                    return {
                        "artifact_id": str(artifact.artifact_id),
                        "bucket": artifact.bucket,
                        "filename": artifact.filename,
                        "file_type": artifact.file_type,
                        "source": artifact.source,
                        "author_id": artifact.author_id,
                        "prompt": artifact.prompt,
                        "created_at": artifact.created_at.isoformat() if artifact.created_at else None
                    }
                return None
        except Exception as e:
            log.error(f"Error getting artifact {artifact_id}: {e}")
            return None

    @web.rpc('artifacts_get_artifact_with_data', 'get_artifact_with_data')
    def get_artifact_with_data(self, artifact_id: str, project_id: int, configuration_title: Optional[str] = None) -> Optional[dict]:
        """
        Get artifact metadata and file data by UUID.

        Args:
            artifact_id: UUID string
            project_id: Project ID for database session
            configuration_title: Optional S3 configuration title

        Returns:
            dict with artifact metadata (bucket, filename, file_type, etc.) and file_data (bytes)
            or None if not found

        NOTE: Do not use across different pylons
        """

        try:
            # Get artifact metadata
            with db.get_session(project_id) as session:
                artifact = session.query(Artifact).filter_by(artifact_id=artifact_id).first()

                if not artifact:
                    log.warning(f"Artifact {artifact_id} not found")
                    return None

                # Get file data from MinIO
                project = self.context.rpc_manager.timeout(3).project_get_or_404(project_id=project_id)
                mc = MinioClient(project, configuration_title=configuration_title)
                
                file_data = mc.download_file(artifact.bucket, artifact.filename)

                return {
                    "artifact_id": str(artifact.artifact_id),
                    "bucket": artifact.bucket,
                    "filename": artifact.filename,
                    "file_type": artifact.file_type,
                    "source": artifact.source,
                    "author_id": artifact.author_id,
                    "prompt": artifact.prompt,
                    "created_at": artifact.created_at.isoformat() if artifact.created_at else None,
                    "file_data": file_data
                }
        except Exception as e:
            log.error(f"Error getting artifact with data {artifact_id}: {e}")
            return None

    @web.rpc('artifacts_upload', 'upload_artifact')
    def upload_artifact(
        self,
        project_id: int,
        bucket: str,
        filename: str,
        file_data: bytes,
        source: str = "manual",
        prompt: Optional[str] = None,
        folder_id: Optional[int] = None,
        configuration_title: Optional[str] = None,
        create_if_not_exists: bool = True,
        bucket_retention_days: Optional[int] = None,
        check_duplicates: bool = True,
        overwrite: bool = False
    ) -> dict:
        """
        Upload file to MinIO and register in artifacts table.
        
        Handles all MinIO operations:
        - Bucket creation (if needed)
        - Duplicate file checking (unless overwrite=True)
        - File upload
        - Artifact registration

        Args:
            project_id: Project ID
            bucket: Bucket name
            filename: File name
            file_data: File content bytes
            source: Source type (manual, attached, generated)
            prompt: Optional user prompt context
            folder_id: Optional folder ID to place artifact in
            configuration_title: Optional S3 configuration title
            create_if_not_exists: Create bucket if it doesn't exist
            bucket_retention_days: Retention policy for bucket (if creating)
            check_duplicates: Check for duplicate files (unless overwrite=True)
            overwrite: Allow overwriting existing files

        Returns:
            dict with artifact_id, bucket, filename, file_type, size, was_duplicate, folder_id

        Raises:
            RuntimeError: If duplicate file exists and overwrite=False

        NOTE: Do not use across different pylons
        """
        try:
            project = self.context.rpc_manager.timeout(3).project_get_or_404(project_id=project_id)
            mc = MinioClient(project, configuration_title=configuration_title)
            
            was_duplicate = False

            # Check for duplicates if requested and not overwriting
            if check_duplicates and not overwrite:
                try:
                    bucket_files = mc.list_files(bucket)
                    if any(bf['name'] == filename for bf in bucket_files):
                        was_duplicate = True
                        raise RuntimeError(f"File '{filename}' already exists in bucket '{bucket}'")
                except Exception as list_error:
                    # If bucket doesn't exist, list_files will fail - that's okay
                    if "already exists" in str(list_error):
                        raise  # Re-raise duplicate error
                    # Otherwise, bucket doesn't exist yet - continue
                    pass
            
            # Create bucket if it doesn't exist
            if create_if_not_exists and bucket not in mc.list_bucket():
                mc.create_bucket(
                    bucket=bucket,
                    bucket_type='local',
                    retention_days=bucket_retention_days
                )

            # Upload file to MinIO
            api_tools.upload_file_base(
                bucket=bucket,
                data=file_data,
                file_name=filename,
                client=mc,
                create_if_not_exists=False  # Already handled above
            )

            # Register artifact in database and get details in one operation
            artifact_details = create_artifact_entry(
                project_id=project_id,
                bucket=bucket,
                filename=filename,
                source=source,
                prompt=prompt,
                folder_id=folder_id
            )

            if not artifact_details:
                raise RuntimeError(f"Failed to create artifact entry for {bucket}/{filename}")

            # Get uploaded file size in bytes (not bucket size)
            file_size_bytes = mc.get_file_size(bucket, filename) if filename else 0

            return {
                "artifact_id": artifact_details.artifact_id,
                "bucket": artifact_details.bucket,
                "filename": artifact_details.filename,
                "file_type": artifact_details.file_type,
                "size": size(file_size_bytes),  # Convert bytes to human-readable format
                "was_duplicate": was_duplicate,
                "folder_id": artifact_details.folder_id
            }
        except Exception as e:
            log.error(f"Failed to upload and register artifact: {e}")
            raise
