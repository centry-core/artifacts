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

""" Utility functions for bucket folder operations """

from typing import Optional, List, Tuple

from pylon.core.tools import log
from tools import auth, db, MinioClient

from ..models.folder import ArtifactFolder
from ..models.artifact import Artifact
from ..models.pd.folder import FolderDetail


def create_folder(
    project_id: int,
    bucket: str,
    name: str,
    parent_id: Optional[int] = None
) -> Optional[FolderDetail]:
    """
    Create a new folder in the specified bucket.
    
    Args:
        project_id: Project ID for database session
        bucket: Bucket name
        name: Folder name
        parent_id: Optional parent folder ID for nested folders
        
    Returns:
        FolderDetail with created folder data or None if creation failed
    """
    try:
        with db.get_session(project_id) as session:
            # Check if folder with same name exists in the same parent
            existing_folder = session.query(ArtifactFolder).filter(
                ArtifactFolder.bucket == bucket,
                ArtifactFolder.name == name,
                ArtifactFolder.parent_id == parent_id
            ).first()
            
            if existing_folder:
                log.warning(f"Folder '{name}' already exists in bucket '{bucket}'")
                raise ValueError(f"Folder '{name}' already exists")
            
            # Validate parent folder exists if specified
            if parent_id:
                parent_folder = session.query(ArtifactFolder).filter(
                    ArtifactFolder.id == parent_id,
                    ArtifactFolder.bucket == bucket
                ).first()
                if not parent_folder:
                    raise ValueError(f"Parent folder with ID {parent_id} not found in bucket '{bucket}'")
            
            # Create new folder
            folder = ArtifactFolder(
                bucket=bucket,
                name=name,
                parent_id=parent_id,
                author_id=auth.current_user().get("id")
            )
            session.add(folder)
            session.commit()
            session.refresh(folder)
            
            log.info(f"Created folder '{name}' (id={folder.id}) in bucket '{bucket}'")
            
            return FolderDetail(
                id=folder.id,
                folder_id=str(folder.folder_id),
                bucket=folder.bucket,
                name=folder.name,
                parent_id=folder.parent_id,
                prefix=folder.prefix,
                author_id=folder.author_id,
                created_at=folder.created_at,
                updated_at=folder.updated_at
            )
            
    except ValueError:
        raise
    except Exception as e:
        log.error(f"Failed to create folder: {e}")
        return None


def get_folder(
    project_id: int,
    bucket: str,
    folder_id: int
) -> Optional[FolderDetail]:
    """
    Get folder details by ID.
    
    Args:
        project_id: Project ID for database session
        bucket: Bucket name
        folder_id: Folder ID
        
    Returns:
        FolderDetail or None if not found
    """
    try:
        with db.get_session(project_id) as session:
            folder = session.query(ArtifactFolder).filter(
                ArtifactFolder.id == folder_id,
                ArtifactFolder.bucket == bucket
            ).first()
            
            if not folder:
                return None
            
            return FolderDetail(
                id=folder.id,
                folder_id=str(folder.folder_id),
                bucket=folder.bucket,
                name=folder.name,
                parent_id=folder.parent_id,
                prefix=folder.prefix,
                author_id=folder.author_id,
                created_at=folder.created_at,
                updated_at=folder.updated_at
            )
            
    except Exception as e:
        log.error(f"Failed to get folder: {e}")
        return None


def get_folder_by_uuid(
    project_id: int,
    bucket: str,
    folder_uuid: str
) -> Optional[FolderDetail]:
    """
    Get folder details by UUID.
    
    Args:
        project_id: Project ID for database session
        bucket: Bucket name
        folder_uuid: Folder UUID
        
    Returns:
        FolderDetail or None if not found
    """
    try:
        with db.get_session(project_id) as session:
            folder = session.query(ArtifactFolder).filter(
                ArtifactFolder.folder_id == folder_uuid,
                ArtifactFolder.bucket == bucket
            ).first()
            
            if not folder:
                return None
            
            return FolderDetail(
                id=folder.id,
                folder_id=str(folder.folder_id),
                bucket=folder.bucket,
                name=folder.name,
                parent_id=folder.parent_id,
                prefix=folder.prefix,
                author_id=folder.author_id,
                created_at=folder.created_at,
                updated_at=folder.updated_at
            )
            
    except Exception as e:
        log.error(f"Failed to get folder by UUID: {e}")
        return None


def list_folders(
    project_id: int,
    bucket: str,
    parent_id: Optional[int] = None
) -> List[FolderDetail]:
    """
    List folders in a bucket, optionally filtered by parent folder.
    
    Args:
        project_id: Project ID for database session
        bucket: Bucket name
        parent_id: Optional parent folder ID (None for root folders)
        
    Returns:
        List of FolderDetail objects
    """
    try:
        with db.get_session(project_id) as session:
            query = session.query(ArtifactFolder).filter(
                ArtifactFolder.bucket == bucket,
                ArtifactFolder.parent_id == parent_id
            ).order_by(ArtifactFolder.name)
            
            folders = query.all()
            
            return [
                FolderDetail(
                    id=f.id,
                    folder_id=str(f.folder_id),
                    bucket=f.bucket,
                    name=f.name,
                    parent_id=f.parent_id,
                    prefix=f.prefix,
                    author_id=f.author_id,
                    created_at=f.created_at,
                    updated_at=f.updated_at
                )
                for f in folders
            ]
            
    except Exception as e:
        log.error(f"Failed to list folders: {e}")
        return []


def update_folder(
    project_id: int,
    bucket: str,
    folder_id: int,
    name: Optional[str] = None,
    parent_id: Optional[int] = None
) -> Optional[FolderDetail]:
    """
    Update folder properties.
    
    Args:
        project_id: Project ID for database session
        bucket: Bucket name
        folder_id: Folder ID to update
        name: New folder name (optional)
        parent_id: New parent folder ID (optional)
        
    Returns:
        Updated FolderDetail or None if update failed
    """
    try:
        with db.get_session(project_id) as session:
            folder = session.query(ArtifactFolder).filter(
                ArtifactFolder.id == folder_id,
                ArtifactFolder.bucket == bucket
            ).first()
            
            if not folder:
                raise ValueError(f"Folder with ID {folder_id} not found")
            
            # Update name if provided
            if name is not None:
                # Check for name conflict
                existing = session.query(ArtifactFolder).filter(
                    ArtifactFolder.bucket == bucket,
                    ArtifactFolder.name == name,
                    ArtifactFolder.parent_id == (parent_id if parent_id is not None else folder.parent_id),
                    ArtifactFolder.id != folder_id
                ).first()
                
                if existing:
                    raise ValueError(f"Folder '{name}' already exists in the target location")
                
                folder.name = name
            
            # Update parent if provided
            if parent_id is not None:
                # Prevent circular reference
                if parent_id == folder_id:
                    raise ValueError("Folder cannot be its own parent")
                
                # Validate parent exists
                if parent_id:
                    parent = session.query(ArtifactFolder).filter(
                        ArtifactFolder.id == parent_id,
                        ArtifactFolder.bucket == bucket
                    ).first()
                    if not parent:
                        raise ValueError(f"Parent folder with ID {parent_id} not found")
                    
                    # Check if parent is a descendant of this folder (would create cycle)
                    if _is_descendant(session, parent_id, folder_id):
                        raise ValueError("Cannot move folder to its own descendant")
                
                folder.parent_id = parent_id if parent_id != 0 else None
            
            session.commit()
            session.refresh(folder)
            
            log.info(f"Updated folder {folder_id} in bucket '{bucket}'")
            
            return FolderDetail(
                id=folder.id,
                folder_id=str(folder.folder_id),
                bucket=folder.bucket,
                name=folder.name,
                parent_id=folder.parent_id,
                prefix=folder.prefix,
                author_id=folder.author_id,
                created_at=folder.created_at,
                updated_at=folder.updated_at
            )
            
    except ValueError:
        raise
    except Exception as e:
        log.error(f"Failed to update folder: {e}")
        return None


def _is_descendant(session, potential_descendant_id: int, ancestor_id: int) -> bool:
    """
    Check if a folder is a descendant of another folder.
    
    Args:
        session: Database session
        potential_descendant_id: ID of folder to check
        ancestor_id: ID of potential ancestor
        
    Returns:
        True if potential_descendant is a descendant of ancestor
    """
    folder = session.query(ArtifactFolder).filter(
        ArtifactFolder.id == potential_descendant_id
    ).first()
    
    while folder and folder.parent_id:
        if folder.parent_id == ancestor_id:
            return True
        folder = session.query(ArtifactFolder).filter(
            ArtifactFolder.id == folder.parent_id
        ).first()
    
    return False


def delete_folder(
    project_id: int,
    bucket: str,
    folder_id: int,
    delete_contents: bool = False,
    mc: Optional[MinioClient] = None
) -> Tuple[bool, str]:
    """
    Delete a folder and optionally its contents.
    
    Args:
        project_id: Project ID for database session
        bucket: Bucket name
        folder_id: Folder ID to delete
        delete_contents: If True, delete all artifacts in the folder
        mc: Optional MinioClient instance
        
    Returns:
        Tuple of (success: bool, message: str)
    """
    try:
        with db.get_session(project_id) as session:
            folder = session.query(ArtifactFolder).filter(
                ArtifactFolder.id == folder_id,
                ArtifactFolder.bucket == bucket
            ).first()
            
            if not folder:
                return False, f"Folder with ID {folder_id} not found"
            
            prefix = folder.prefix
            
            # Check if folder has children
            children_count = session.query(ArtifactFolder).filter(
                ArtifactFolder.parent_id == folder_id
            ).count()
            
            if children_count > 0 and not delete_contents:
                return False, f"Folder has {children_count} subfolders. Use delete_contents=true to delete recursively."
            
            # Check if folder has artifacts
            artifacts_count = session.query(Artifact).filter(
                Artifact.bucket == bucket,
                Artifact.folder_id == folder_id
            ).count()
            
            if artifacts_count > 0 and not delete_contents:
                return False, f"Folder has {artifacts_count} artifacts. Use delete_contents=true to delete them."
            
            # Delete artifacts from S3 and database if delete_contents is True
            if delete_contents and mc:
                # Get all artifacts in this folder and subfolders
                folder_ids = _get_all_descendant_folder_ids(session, folder_id) + [folder_id]
                
                artifacts = session.query(Artifact).filter(
                    Artifact.bucket == bucket,
                    Artifact.folder_id.in_(folder_ids)
                ).all()
                
                # Delete from S3
                for artifact in artifacts:
                    try:
                        mc.remove_file(bucket, artifact.filename)
                    except Exception as e:
                        log.warning(f"Failed to delete S3 file {artifact.filename}: {e}")
                
                # Delete artifact records
                session.query(Artifact).filter(
                    Artifact.bucket == bucket,
                    Artifact.folder_id.in_(folder_ids)
                ).delete(synchronize_session=False)
            
            # Delete folder (cascades to children due to ON DELETE CASCADE)
            session.delete(folder)
            session.commit()
            
            log.info(f"Deleted folder {folder_id} (prefix: {prefix}) from bucket '{bucket}'")
            return True, f"Folder '{folder.name}' deleted successfully"
            
    except Exception as e:
        log.error(f"Failed to delete folder: {e}")
        return False, str(e)


def _get_all_descendant_folder_ids(session, folder_id: int) -> List[int]:
    """
    Get all descendant folder IDs recursively.
    
    Args:
        session: Database session
        folder_id: Parent folder ID
        
    Returns:
        List of descendant folder IDs
    """
    result = []
    children = session.query(ArtifactFolder).filter(
        ArtifactFolder.parent_id == folder_id
    ).all()
    
    for child in children:
        result.append(child.id)
        result.extend(_get_all_descendant_folder_ids(session, child.id))
    
    return result


def get_folder_contents(
    project_id: int,
    bucket: str,
    folder_id: Optional[int] = None
) -> dict:
    """
    Get contents of a folder (subfolders and artifacts).
    
    Args:
        project_id: Project ID for database session
        bucket: Bucket name
        folder_id: Folder ID (None for root level)
        
    Returns:
        Dictionary with folders and artifacts
    """
    try:
        with db.get_session(project_id) as session:
            # Get current folder details if specified
            current_folder = None
            if folder_id:
                folder = session.query(ArtifactFolder).filter(
                    ArtifactFolder.id == folder_id,
                    ArtifactFolder.bucket == bucket
                ).first()
                
                if folder:
                    current_folder = FolderDetail(
                        id=folder.id,
                        folder_id=str(folder.folder_id),
                        bucket=folder.bucket,
                        name=folder.name,
                        parent_id=folder.parent_id,
                        prefix=folder.prefix,
                        author_id=folder.author_id,
                        created_at=folder.created_at,
                        updated_at=folder.updated_at
                    )
            
            # Get subfolders
            folders = list_folders(project_id, bucket, folder_id)
            
            # Get artifacts in this folder
            artifacts = session.query(Artifact).filter(
                Artifact.bucket == bucket,
                Artifact.folder_id == folder_id
            ).all()
            
            artifact_list = [
                {
                    "id": a.id,
                    "artifact_id": str(a.artifact_id),
                    "filename": a.filename,
                    "file_type": a.file_type,
                    "source": a.source,
                    "author_id": a.author_id,
                    "created_at": a.created_at.isoformat() if a.created_at else None,
                }
                for a in artifacts
            ]
            
            return {
                "folder": current_folder,
                "folders": folders,
                "artifacts": artifact_list,
                "total_folders": len(folders),
                "total_artifacts": len(artifact_list)
            }
            
    except Exception as e:
        log.error(f"Failed to get folder contents: {e}")
        return {
            "folder": None,
            "folders": [],
            "artifacts": [],
            "total_folders": 0,
            "total_artifacts": 0
        }


def delete_folders_by_bucket(project_id: int, bucket: str) -> int:
    """
    Delete all folders for a bucket.
    
    Args:
        project_id: Project ID for database session
        bucket: Bucket name
        
    Returns:
        Number of deleted folders
    """
    try:
        with db.get_session(project_id) as session:
            deleted_count = session.query(ArtifactFolder).filter(
                ArtifactFolder.bucket == bucket
            ).delete(synchronize_session=False)
            
            session.commit()
            
            if deleted_count > 0:
                log.info(f"Deleted {deleted_count} folders from bucket '{bucket}'")
            
            return deleted_count
            
    except Exception as e:
        log.error(f"Failed to delete folders for bucket {bucket}: {e}")
        return 0


def move_folder_to_bucket(
    project_id: int,
    source_bucket: str,
    source_folder_id: int,
    destination_bucket: str,
    destination_folder_name: Optional[str] = None,
    mc: Optional[MinioClient] = None,
    copy_mode: str = "move"
) -> Tuple[bool, str, Optional[int]]:
    """
    Move or copy a folder with all its contents to another bucket.
    
    Args:
        project_id: Project ID for database session
        source_bucket: Source bucket name
        source_folder_id: Source folder ID to move
        destination_bucket: Destination bucket name
        destination_folder_name: Optional new name for folder (defaults to source name)
        mc: MinioClient instance for S3 operations
        copy_mode: 'move' (default) or 'copy' to keep source
        
    Returns:
        Tuple of (success: bool, message: str, new_folder_id: Optional[int])
    """
    try:
        with db.get_session(project_id) as session:
            # Get source folder
            source_folder = session.query(ArtifactFolder).filter(
                ArtifactFolder.id == source_folder_id,
                ArtifactFolder.bucket == source_bucket
            ).first()
            
            if not source_folder:
                return False, f"Source folder with ID {source_folder_id} not found in bucket '{source_bucket}'", None
            
            # Use source name if destination name not provided
            dest_name = destination_folder_name or source_folder.name
            
            # Check if destination folder already exists
            existing_dest = session.query(ArtifactFolder).filter(
                ArtifactFolder.bucket == destination_bucket,
                ArtifactFolder.name == dest_name,
                ArtifactFolder.parent_id.is_(None)  # Root level
            ).first()
            
            if existing_dest:
                return False, f"Folder '{dest_name}' already exists in bucket '{destination_bucket}'", None
            
            # Get all artifacts in source folder (including nested)
            def get_folder_and_subfolders(folder_id):
                """Recursively get folder and all subfolders"""
                folders = [folder_id]
                subfolders = session.query(ArtifactFolder.id).filter(
                    ArtifactFolder.bucket == source_bucket,
                    ArtifactFolder.parent_id == folder_id
                ).all()
                for (subfolder_id,) in subfolders:
                    folders.extend(get_folder_and_subfolders(subfolder_id))
                return folders
            
            all_folder_ids = get_folder_and_subfolders(source_folder_id)
            
            # Get all artifacts in these folders
            artifacts = session.query(Artifact).filter(
                Artifact.bucket == source_bucket,
                Artifact.folder_id.in_(all_folder_ids)
            ).all()
            
            # Create folder structure mapping
            folder_mapping = {}  # old_id -> new_folder
            
            def copy_folder_structure(old_folder_id, new_parent_id=None):
                """Recursively copy folder structure"""
                old_folder = session.query(ArtifactFolder).get(old_folder_id)
                if not old_folder:
                    return None
                
                # Determine new folder name
                if old_folder_id == source_folder_id:
                    new_name = dest_name
                else:
                    new_name = old_folder.name
                
                # Create new folder in destination
                new_folder = ArtifactFolder(
                    bucket=destination_bucket,
                    name=new_name,
                    parent_id=new_parent_id,
                    author_id=old_folder.author_id
                )
                session.add(new_folder)
                session.flush()  # Get the ID
                
                folder_mapping[old_folder_id] = new_folder
                
                # Copy subfolders
                subfolders = session.query(ArtifactFolder).filter(
                    ArtifactFolder.bucket == source_bucket,
                    ArtifactFolder.parent_id == old_folder_id
                ).all()
                
                for subfolder in subfolders:
                    copy_folder_structure(subfolder.id, new_folder.id)
                
                return new_folder
            
            # Copy folder structure to destination
            root_new_folder = copy_folder_structure(source_folder_id)
            
            if not root_new_folder:
                session.rollback()
                return False, "Failed to create folder structure in destination", None
            
            # Copy files in S3 and create artifact entries
            copied_files = 0
            failed_files = []
            
            for artifact in artifacts:
                try:
                    # S3 keys are just filenames (no folder prefix)
                    source_key = artifact.filename
                    
                    # Determine destination key based on folder mapping
                    new_folder = folder_mapping.get(artifact.folder_id)
                    if new_folder:
                        # Get the new filename (check for duplicates)
                        dest_key = artifact.filename  # Start with same filename
                        
                        # Check if filename already exists in destination bucket
                        # If it does, generate a unique name
                        base_name = dest_key
                        counter = 1
                        while session.query(Artifact).filter(
                            Artifact.bucket == destination_bucket,
                            Artifact.filename == dest_key
                        ).first():
                            # Generate new filename with counter
                            if '.' in base_name:
                                name_part, ext = base_name.rsplit('.', 1)
                                dest_key = f"{name_part}_{counter}.{ext}"
                            else:
                                dest_key = f"{base_name}_{counter}"
                            counter += 1
                        
                        # Copy file in S3 if mc client is available
                        # S3 keys are just filenames (flat storage)
                        if mc and hasattr(mc, 'client') and mc.client:
                            mc.client.copy_object(
                                Bucket=destination_bucket,
                                CopySource={'Bucket': source_bucket, 'Key': source_key},
                                Key=dest_key
                            )
                        
                        # Create new artifact entry in destination
                        new_artifact = Artifact(
                            bucket=destination_bucket,
                            filename=dest_key,
                            source=artifact.source,
                            folder_id=new_folder.id,
                            author_id=artifact.author_id
                        )
                        session.add(new_artifact)
                        copied_files += 1
                        
                except Exception as e:
                    log.error(f"Failed to copy artifact {artifact.filename}: {e}")
                    failed_files.append(artifact.filename)
            
            # If move mode, delete source
            if copy_mode == "move":
                # Delete source artifacts
                for artifact in artifacts:
                    try:
                        if mc:
                            mc.remove_file(source_bucket, artifact.filename)
                        session.delete(artifact)
                    except Exception as e:
                        log.error(f"Failed to delete source artifact {artifact.filename}: {e}")
                
                # Delete source folders
                for folder_id in reversed(all_folder_ids):  # Delete from leaf to root
                    folder = session.query(ArtifactFolder).get(folder_id)
                    if folder:
                        session.delete(folder)
            
            session.commit()
            
            message = f"Successfully {'moved' if copy_mode == 'move' else 'copied'} folder '{source_folder.name}' to bucket '{destination_bucket}'"
            if copied_files > 0:
                message += f" ({copied_files} files)"
            if failed_files:
                message += f". Failed files: {', '.join(failed_files[:5])}"
                if len(failed_files) > 5:
                    message += f" and {len(failed_files) - 5} more"
            
            return True, message, root_new_folder.id
            
    except Exception as e:
        log.error(f"Failed to move folder: {e}")
        return False, f"Failed to move folder: {str(e)}", None
