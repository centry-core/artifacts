from pylon.core.tools import web, log  # pylint: disable=E0611,E0401
from tools import auth  # pylint: disable=E0401


class Slot:  # pylint: disable=E1101,R0903
    @web.slot('administration_artifacts_content')
    def content(self, context, slot, payload):
        log.info('slot: [%s], payload: %s', slot, payload)
        with context.app.app_context():
            return self.descriptor.render_template(
                'artifacts/content.html'
            )

    @web.slot('administration_artifacts_scripts')
    def scripts(self, context, slot, payload):
        log.info('slot: [%s], payload: %s', slot, payload)
        with context.app.app_context():
            return self.descriptor.render_template(
                'artifacts/scripts.html',
            )

    @web.slot('administration_artifacts_styles')
    def styles(self, context, slot, payload):
        log.info('slot: [%s], payload: %s', slot, payload)
        with context.app.app_context():
            return self.descriptor.render_template(
                'artifacts/styles.html',
            )
