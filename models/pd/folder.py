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

""" Pydantic models for folder operations """

import re
from typing import Optional, List
from uuid import UUID
from datetime import datetime
from pydantic import BaseModel, ConfigDict, field_validator, field_serializer


class FolderCreate(BaseModel):
    """Request model for creating a folder."""
    
    name: str
    parent_id: Optional[int] = None
    
    @field_validator('name')
    @classmethod
    def validate_name(cls, v: str) -> str:
        """
        Validate folder name:
        - Must start with a letter
        - Can only contain letters, numbers, hyphens, and underscores
        - Cannot be empty
        - Cannot contain slashes (S3 prefix separator)
        """
        if not v or not v.strip():
            raise ValueError("Folder name cannot be empty")
        
        v = v.strip()
        
        # Folder name pattern: starts with letter, contains alphanumeric, hyphens, underscores
        folder_pattern = r"^[a-zA-Z][a-zA-Z0-9_-]*$"
        
        if not re.match(folder_pattern, v):
            raise ValueError(
                "Invalid folder name. Folder name must start with a letter "
                "and contain only letters, numbers, hyphens, and underscores."
            )
        
        if '/' in v or '\\' in v:
            raise ValueError("Folder name cannot contain slashes")
        
        return v


class FolderUpdate(BaseModel):
    """Request model for updating a folder."""
    
    name: Optional[str] = None
    parent_id: Optional[int] = None
    
    @field_validator('name')
    @classmethod
    def validate_name(cls, v: Optional[str]) -> Optional[str]:
        """Validate folder name if provided."""
        if v is None:
            return v
        
        if not v or not v.strip():
            raise ValueError("Folder name cannot be empty")
        
        v = v.strip()
        
        folder_pattern = r"^[a-zA-Z][a-zA-Z0-9_-]*$"
        
        if not re.match(folder_pattern, v):
            raise ValueError(
                "Invalid folder name. Folder name must start with a letter "
                "and contain only letters, numbers, hyphens, and underscores."
            )
        
        if '/' in v or '\\' in v:
            raise ValueError("Folder name cannot contain slashes")
        
        return v


class FolderDetail(BaseModel):
    """Response model for folder details."""
    
    model_config = ConfigDict(from_attributes=True)
    
    id: int
    folder_id: str
    bucket: str
    name: str
    parent_id: Optional[int] = None
    prefix: str
    author_id: Optional[int] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    
    @field_validator('folder_id', mode='before')
    @classmethod
    def convert_uuid_to_str(cls, v):
        """Convert UUID to string if needed."""
        if isinstance(v, UUID):
            return str(v)
        return v
    
    @field_serializer('created_at', 'updated_at')
    def serialize_datetime(self, value: Optional[datetime]) -> Optional[str]:
        """Serialize datetime to ISO format string."""
        return value.isoformat() if value else None


class FolderListResponse(BaseModel):
    """Response model for listing folders."""
    
    total: int
    rows: List[FolderDetail]


class FolderContentsResponse(BaseModel):
    """Response model for folder contents (folders + artifacts)."""
    
    folder: Optional[FolderDetail] = None
    folders: List[FolderDetail]
    artifacts: List[dict]
    total_folders: int
    total_artifacts: int
