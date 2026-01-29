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

""" Pydantic models for artifact operations """

from typing import Optional
from uuid import UUID
from pydantic import BaseModel, ConfigDict, field_validator


class ArtifactDetail(BaseModel):
    """Artifact details returned from database operations."""

    model_config = ConfigDict(from_attributes=True)

    artifact_id: str
    bucket: str
    filename: str
    file_type: str
    source: str
    author_id: Optional[int] = None
    prompt: Optional[str] = None
    folder_id: Optional[int] = None

    @field_validator('artifact_id', mode='before')
    @classmethod
    def convert_uuid_to_str(cls, v):
        """Convert UUID to string if needed."""
        if isinstance(v, UUID):
            return str(v)
        return v
