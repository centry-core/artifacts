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

""" Artifact retrieval by UUID """

from flask import send_file
from io import BytesIO
from werkzeug.exceptions import NotFound

from tools import MinioClient, api_tools, auth, db, config as c
from pylon.core.tools import log, web
from ...models.artifact import Artifact


class ProjectAPI(api_tools.APIModeHandler):
    @auth.decorators.check_api(["configuration.artifacts.artifacts.view"])
    def get(self, project_id: int, artifact_id: str, **kwargs):
        project = self.module.context.rpc_manager.call.project_get_or_404(project_id=project_id)
        # Attempt 1: artifacts table
        with db.get_session(project_id) as session:
            artifact = session.query(Artifact).filter_by(artifact_id=artifact_id).first()

            if artifact:
                try:
                    mc = MinioClient(project)
                    file_content = mc.download_file(artifact.bucket, artifact.filename)

                    if file_content:
                        try:
                            return send_file(BytesIO(file_content), attachment_filename=artifact.filename)
                        except TypeError:  # new flask
                            return send_file(BytesIO(file_content), download_name=artifact.filename, as_attachment=False)
                except Exception as e:
                    log.warning(f"Failed to get file from bucket for artifact {artifact_id}: {e}")

        # Attempt 2: Fallback to message base64 (if bucket deleted but message still has content)
        try:
            result = self.module.context.rpc_manager.timeout(3).elitea_core_get_content_from_message_by_artifact_id(
                project_id=project_id,
                artifact_id=artifact_id
            )
            if result:
                file_content, filename = result
                try:
                    return send_file(BytesIO(file_content), attachment_filename=filename)
                except TypeError:  # new flask
                    return send_file(BytesIO(file_content), download_name=filename, as_attachment=False)
        except Exception as e:
            log.warning(f"Attempt 2 (message fallback) failed: {e}")

        raise NotFound(f"Artifact {artifact_id} not found")


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        '<string:project_id>',
        '<string:project_id>/<string:artifact_id>',
    ])

    mode_handlers = {
        'default': ProjectAPI,
    }
