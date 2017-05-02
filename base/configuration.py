from collections import deque, namedtuple
import logging

def get_task_id_prefix():
    return 'publish_'

common_config_dict = {
    "default_ansible_parameters": {
            "is_local_inventory": True,
            "is_async": False,
            "job_name": "update"
    }
}

config_dict = {
    "env": "test",
    "zookeeper_ip": "10.99.70.73",
    "callback_url": "http://10.99.7.15:83",
    "module_handler_url": {
        'sync':'http://ansible-handler.publish.ops.eju.local/module_handler',
        'async':'http://ansible-handler.publish.ops.eju.local/module_handler',
    },
    "task_callback_url": 'http://10.99.70.73:8100/jobscallback',
    "publish_task_sequence": [
        "transfer_version",
        "update_version",
    ]
}

LOG_SETTINGS = {
    'version': 1,
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'level': 'INFO',
            'formatter': 'detailed',
            'stream': 'ext://sys.stdout',
        },
        'job_handler_file': {
            'class': 'logging.handlers.RotatingFileHandler',
            'level': 'INFO',
            'formatter': 'detailed',
            'filename': '/data/logs/version_publish/job_handler.log',
            'mode': 'a',
            'maxBytes': 10485760,
            'backupCount': 2,
        },
        'zkhandler_file': {
            'class': 'logging.handlers.RotatingFileHandler',
            'level': 'INFO',
            'formatter': 'detailed',
            'filename': '/data/logs/version_publish/zkhandler.log',
            'mode': 'a',
            'maxBytes': 10485760,
            'backupCount': 2,
        },
        'scheduler_file': {
            'class': 'logging.handlers.RotatingFileHandler',
            'level': 'INFO',
            'formatter': 'detailed',
            'filename': '/data/logs/version_publish/scheduler.log',
            'mode': 'a',
            'maxBytes': 10485760,
            'backupCount': 2,
        },
        'portal_handler_file': {
            'class': 'logging.handlers.RotatingFileHandler',
            'level': 'INFO',
            'formatter': 'detailed',
            'filename': '/data/logs/version_publish/portal_handler.log',
            'mode': 'a',
            'maxBytes': 10485760,
            'backupCount': 2,
        },
        'release_record_file': {
            'class': 'logging.handlers.RotatingFileHandler',
            'level': 'INFO',
            'formatter': 'detailed',
            'filename': '/data/logs/version_publish/release_record.log',
            'mode': 'a',
            'maxBytes': 10485760,
            'backupCount': 2,
        },
        'callback_file': {
            'class': 'logging.handlers.RotatingFileHandler',
            'level': 'INFO',
            'formatter': 'detailed',
            'filename': '/data/logs/version_publish/callback.log',
            'mode': 'a',
            'maxBytes': 10485760,
            'backupCount': 2,
        },
        'jenkins_handler_file': {
            'class': 'logging.handlers.RotatingFileHandler',
            'level': 'INFO',
            'formatter': 'detailed',
            'filename': '/data/logs/version_publish/jenkins_handler.log',
            'mode': 'a',
            'maxBytes': 10485760,
            'backupCount': 2,
        },
    },
    'formatters': {
        'detailed': {
            'format': '%(asctime)s %(module)-17s line:%(lineno)-4d ' \
            '%(levelname)-8s %(message)s',
        },
        'email': {
            'format': 'Timestamp: %(asctime)s\nModule: %(module)s\n' \
            'Line: %(lineno)d\nMessage: %(message)s',
        },
    },
    'loggers': {
        'extensive': {
            'level':'DEBUG',
            'handlers': ['console']
            },
        'job_handler': {
            'level': 'DEBUG',
            'handlers': ['console','job_handler_file']
        },
        'zkhandler': {
            'level': 'DEBUG',
            'handlers': ['console','zkhandler_file']
        },
        'scheduler': {
            'level': 'DEBUG',
            'handlers': ['console','scheduler_file']
        },
        'portal_handler': {
            'level': 'DEBUG',
            'handlers': ['console','portal_handler_file']
        },
        'release_record': {
            'level': 'DEBUG',
            'handlers': ['console','release_record_file']
        },
        'callback': {
            'level': 'DEBUG',
            'handlers': ['console','callback_file']
        },
        'jenkins_handler': {
            'level': 'DEBUG',
            'handlers': ['console','jenkins_handler_file']
        },
    }
}


