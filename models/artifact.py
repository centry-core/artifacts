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

""" Artifact model """

import uuid
from datetime import datetime
from typing import Optional

from sqlalchemy import UUID, Integer, String, Text, DateTime, func, UniqueConstraint, ForeignKey, Index
from sqlalchemy.orm import Mapped, mapped_column, relationship

from tools import db
from tools import config as c


class Artifact(db.Base):
    """
    Artifact metadata table for tracking uploaded/generated files.

    Maps artifact UUIDs to bucket+filename locations.
    Artifacts can optionally belong to a folder within a bucket.
    """
    __tablename__ = 'artifacts'
    __table_args__ = (
        UniqueConstraint('bucket', 'filename', name='uq_artifacts_bucket_filename'),
        Index('ix_artifacts_bucket_folder', 'bucket', 'folder_id'),
        {'schema': c.POSTGRES_TENANT_SCHEMA},
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    artifact_id: Mapped[str] = mapped_column(UUID(as_uuid=True), unique=True, default=uuid.uuid4)
    bucket: Mapped[str] = mapped_column(String, nullable=False)
    filename: Mapped[str] = mapped_column(String, nullable=False)
    file_type: Mapped[str] = mapped_column(String, nullable=True)
    source: Mapped[str] = mapped_column(String, nullable=False)
    author_id: Mapped[int] = mapped_column(Integer, nullable=True)
    prompt: Mapped[str] = mapped_column(Text, nullable=True)
    folder_id: Mapped[Optional[int]] = mapped_column(
        Integer,
        ForeignKey(f'{c.POSTGRES_TENANT_SCHEMA}.artifact_folders.id', ondelete='SET NULL'),
        nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=True, onupdate=func.now())
    
    # Relationship to folder
    folder = relationship("ArtifactFolder", backref="artifacts", lazy="joined")
