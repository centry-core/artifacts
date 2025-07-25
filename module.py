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

""" Module """
from queue import Empty

from pylon.core.tools import log  # pylint: disable=E0611,E0401
from pylon.core.tools import module  # pylint: disable=E0611,E0401
from tools import theme

from .models.pd.configuration import configuration_record


class Module(module.ModuleModel):
    """ Task module """

    def __init__(self, context, descriptor):
        self.context = context
        self.descriptor = descriptor

    def init(self):
        """ Init module """
        log.info("Initializing module Artifacts")
        self.descriptor.init_all()

        theme.register_subsection(
            "configuration", "artifacts",
            "Artifacts",
            title="Artifacts",
            kind="slot",
            permissions={
                "permissions": ["configuration.artifacts"],
                "recommended_roles": {
                    "administration": {"admin": True, "viewer": True, "editor": True},
                    "default": {"admin": True, "viewer": True, "editor": True},
                    "developer": {"admin": True, "viewer": True, "editor": True},
                }},
            prefix="artifacts_",
            weight=5,
        )

        theme.register_mode_subsection(
            "administration", "configuration",
            "artifacts", "Artifacts",
            title="Artifacts",
            kind="slot",
            permissions={
                "permissions": ["configuration.artifacts"],
                "recommended_roles": {
                    "administration": {"admin": True, "viewer": True, "editor": True},
                    "default": {"admin": True, "viewer": True, "editor": True},
                    "developer": {"admin": True, "viewer": True, "editor": True},
                }},
            prefix="administration_artifacts_",
            # icon_class="fas fa-server fa-fw",
            # weight=2,
        )

    def deinit(self):  # pylint: disable=R0201
        """ De-init module """
        log.info("De-initializing module Artifacts")

    def ready(self):
        log.info("Artifacts ready callback")
        from tools import VaultClient, config

        from .models.pd.configuration import S3Config
        try:
            # type_name: str, section: str, model: type[BaseModel] = None, validation_func: str = None
            self.context.rpc_manager.timeout(2).configurations_register(
               **configuration_record
            )

            secrets = VaultClient().get_all_secrets()
            try:
                public_project_id = int(secrets['ai_project_id'])
                try:
                    config, was_created = self.context.rpc_manager.timeout(2).configurations_create_if_not_exists(dict(
                        project_id=public_project_id,
                        type='s3',
                        title='Elitea S3 storage',
                        shared=True,
                        data={
                            'access_key': config.MINIO_ACCESS_KEY,
                            'secret_access_key': config.MINIO_SECRET_KEY,
                            'region_name': config.MINIO_REGION,
                            'use_compatible_storage': True,
                            'storage_url': config.MINIO_URL
                        }
                    ))
                    if not was_created:
                        log.debug(f"'Configuration {config['type']}: {config['title']}' already exists")
                    log.info(f"Artifacts config created {config=}")
                except Empty:
                    log.warning('Configurations plugin unavailable')
            except (KeyError, ValueError):
                raise Exception("Public project doesn't exist")

        except Empty:
            log.warning('Configurations plugin rpc not available')




