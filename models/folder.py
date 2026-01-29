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

""" ArtifactFolder model """

import uuid
from datetime import datetime
from typing import Optional

from sqlalchemy import UUID, Integer, String, DateTime, func, ForeignKey, UniqueConstraint, Index
from sqlalchemy.orm import Mapped, mapped_column, relationship

from tools import db
from tools import config as c


class ArtifactFolder(db.Base):
    """
    ArtifactFolder metadata table for organizing artifacts within S3 buckets.

    Folders are virtual constructs that help organize artifacts within buckets.
    In S3 terms, folders are represented as prefixes (e.g., "folder_name/").
    """
    __tablename__ = 'artifact_folders'
    __table_args__ = (
        UniqueConstraint('bucket', 'name', 'parent_id', name='uq_artifact_folders_bucket_name_parent'),
        Index('ix_artifact_folders_bucket', 'bucket'),
        Index('ix_artifact_folders_parent', 'parent_id'),
        {'schema': c.POSTGRES_TENANT_SCHEMA},
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    folder_id: Mapped[str] = mapped_column(UUID(as_uuid=True), unique=True, default=uuid.uuid4)
    bucket: Mapped[str] = mapped_column(String, nullable=False)
    name: Mapped[str] = mapped_column(String, nullable=False)
    parent_id: Mapped[Optional[int]] = mapped_column(
        Integer,
        ForeignKey(f'{c.POSTGRES_TENANT_SCHEMA}.artifact_folders.id', ondelete='CASCADE'),
        nullable=True
    )
    author_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=True, onupdate=func.now())

    # Self-referential relationship for nested folders
    parent = relationship(
        "ArtifactFolder",
        remote_side="ArtifactFolder.id",
        backref="children",
        lazy="joined"
    )
    
    @property
    def prefix(self) -> str:
        """
        Get the S3 prefix path for this folder.
        
        Returns:
            str: The full prefix path (e.g., "parent/child/")
        """
        if self.parent:
            return f"{self.parent.prefix}{self.name}/"
        return f"{self.name}/"

    def to_dict(self) -> dict:
        """Convert folder to dictionary representation."""
        return {
            "id": self.id,
            "folder_id": str(self.folder_id),
            "bucket": self.bucket,
            "name": self.name,
            "parent_id": self.parent_id,
            "prefix": self.prefix,
            "author_id": self.author_id,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }
