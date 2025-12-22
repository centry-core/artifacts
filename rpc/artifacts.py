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

from tools import MinioClient, db
from pylon.core.tools import log, web

from ..models.artifact import Artifact


class RPC:
    @web.rpc('artifacts_get_artifact_by_id', 'get_artifact_by_id')
    def get_artifact_by_id(self, artifact_id: str, project_id: int) -> Optional[dict]:
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
