import os
from tornado.ioloop import IOLoop
from tornado.options import parse_command_line, parse_config_file, options

from publish_handler import make_app
from publish_handler.jenkins_handler import JenkinsHandler
from publish_handler.portal_handler import PublishHandler

Handlers = [
    (r'/jenkins_handler', JenkinsHandler),
    (r'/publish_version', PublishHandler)
]

if __name__ == '__main__':
    if os.path.exists('./version_publish.config'):
        parse_config_file('./version_publish.config')
    parse_command_line()
    app  = make_app(Handlers, debug=True)
    app.listen(options.port, address=options.bind)
    try:
        print('starting')
        IOLoop.current().start()
    except KeyboardInterrupt:
        IOLoop.current().stop()