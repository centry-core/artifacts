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

""" Generic utility functions for artifacts plugin """

import base64
from pathlib import Path
from typing import Optional

from pylon.core.tools import log
from tools import auth, context, db

from ..models.enums.all import FileType
from ..models.artifact import Artifact


def determine_artifact_type(filename: str) -> str:
    """
    Determine artifact type from extension using index_types from elitea_core.
    
    Args:
        filename: File name with extension
        
    Returns:
        FileType.IMAGE.value | FileType.DOCUMENT.value | FileType.UNKNOWN.value
    """
    try:
        # Get index_types from elitea_core via RPC
        index_types = context.rpc_manager.timeout(3).elitea_core_get_index_types()
        
        ext = Path(filename).suffix.lower()
        
        # Check image types
        if ext in index_types.get("image_types", {}):
            return FileType.IMAGE.value
        
        # Check document types
        if ext in index_types.get("document_types", {}):
            return FileType.DOCUMENT.value
        
        return FileType.UNKNOWN.value
        
    except Exception as e:
        log.warning(f"Failed to determine artifact type for {filename}: {e}")
        return FileType.UNKNOWN.value


def create_artifact_entry(
    project_id: int,
    bucket: str,
    filename: str,
    source: str = "manual",
    prompt: Optional[str] = None
) -> Optional[str]:
    """
    Create artifact entry in database.

    Args:
        project_id: Project ID for database session
        bucket: Bucket name
        filename: File name
        source: Source type ("generated" | "attached" | "manual")
        prompt: Optional prompt text for generated artifacts

    Returns:
        artifact_id (UUID string) or None if creation failed
    """
    try:
        artifact = Artifact(
            bucket=bucket,
            filename=filename,
            file_type=determine_artifact_type(filename),
            source=source,
            author_id=auth.current_user().get("id"),
            prompt=prompt
        )

        with db.get_session(project_id) as session:
            session.add(artifact)
            session.commit()
            artifact_id = str(artifact.artifact_id)

        log.info(f"Created artifact {artifact_id} for {bucket}/{filename}")
        return artifact_id

    except Exception as e:
        log.error(f"Failed to create artifact entry: {e}")
        return None


def delete_artifact_entries(
    project_id: int,
    bucket: str,
    filenames: Optional[list] = None
) -> int:
    """
    Delete artifact entries from database.
    
    Args:
        project_id: Project ID for database session
        bucket: Bucket name
        filenames: Optional list of filenames to delete. If None, deletes all artifacts for bucket.
        
    Returns:
        Number of deleted artifact entries
    """
    try:
        with db.get_session(project_id) as session:
            if filenames is None:
                # Delete all artifacts for this bucket
                deleted_count = session.query(Artifact).filter_by(bucket=bucket).delete()
            else:
                # Delete specific artifacts
                deleted_count = session.query(Artifact).filter(
                    Artifact.bucket == bucket,
                    Artifact.filename.in_(filenames)
                ).delete(synchronize_session=False)
            
            session.commit()
            
            if deleted_count > 0:
                if filenames:
                    log.info(f"Cleaned up {deleted_count} artifact entries for {bucket} (specific files)")
                else:
                    log.info(f"Cleaned up {deleted_count} artifact entries for bucket {bucket} (all files)")
            
            return deleted_count
            
    except Exception as e:
        log.warning(f"Failed to clean up artifact entries for bucket {bucket}: {e}")
        return 0


def extract_from_message(artifact_id: str) -> Optional[tuple[bytes, str]]:
    """
    Extract file content from AttachmentMessageItem by artifact_id.
    
    This is a fallback for backward compatibility with old messages
    that don't have entries in artifacts table.
    
    Args:
        artifact_id: UUID string
        
    Returns:
        tuple of (file_content: bytes, filename: str) or None if not found
    """
    try:
        attachment_data = context.rpc_manager.timeout(3).elitea_core_get_attachment_by_artifact_id(
            artifact_id=artifact_id
        )

        if not attachment_data or not attachment_data.get('content'):
            return None

        # Extract base64 from content
        content = attachment_data['content']
        if not content:
            return None

        try:
            url = content.get('image_url', {}).get('url', '')
            if url.startswith('data:'):
                # Remove "data:image/png;base64," like prefix
                base64_data = url.split(',', 1)[1] if ',' in url else None
                if base64_data:
                    file_content = base64.b64decode(base64_data)
                    filename = attachment_data.get('filename', 'artifact')
                    return (file_content, filename)
        except Exception:
            pass

        return None

    except Exception as e:
        log.warning(f"Failed to extract from message for artifact {artifact_id}: {e}")
        return None
