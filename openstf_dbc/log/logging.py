#!/usr/bin/env python

# SPDX-FileCopyrightText: 2021 2017-2021 Alliander N.V. <korte.termijn.prognoses@alliander.com>
#
# SPDX-License-Identifier: MPL-2.0

# -*- coding: utf-8 -*-
"""
KTP custom logging framework.

The KTP custom logging framework configures the logging functionality and enables other
modules/functions/classes to obtain a named instance. When configuring the logging
framework two arguments/options are given: the loging level and the runtime environment.
The logging level determines the lowest level of the log messages which should be displayed.
The runtime environment determines if the loggers should about a humanreadable structlog
format or a machine readable json structlog format.

In any case the logging is based on structlog (https://www.structlog.org/en/stable/).
Structlog has a different philosophy compared to more traditional logging. Struclog aims
to produce logs that are easily parsed by a machine (e.g. Kibana, Logstash etc.).

Instead of using traditional string interpolation to generate meaningfull log messages,
structlog enabled keyword arguments to given context.

traditional approach
logger.info(f"Calculate c using a = {a}, b = {b}")

structlog approach
logger.info("Calcualte c", a=a, b=b)


Example:
    # import custom logging framework
    from ktprognoses.log import logging

    # configure the logging framework
    # typically you only configure your logger once in your main/entry script
    # all other modules and classes just obtain a logger and use it
    logging.configure_logging(loglevel="INFO", runtime_env="local")

    # obtain a named logger instance
    logger = logging.get_logger("prognosis_scheduler")

    # bind a variable to the logger, from now on this variable will be added to
    # every log message, until you use `unbind`
    logger.bind(scheduler="prognosis_scheduler")

    # log something with INFO level (any number of keyword arguments may be given)
    logger.info("Start making basecase predictions", num_prediction_jobs=32)

    # prints: runtime_env == "dev"
    # 2020-08-18 20:52:35 [info     ] Start making basecase predictions [prognosis_scheduler] num_prediction_jobs=32 scheduler="prognosis_scheduler"

    # prints: runtime_env == "container"
    # {
    #   "num_prediction_jobs": 32,
    #   "event": "Start making basecase predictions",
    #   "level": "info",
    #   "logger": "prognosis_scheduler",
    #   "scheduler": "prognosis_scheduler",
    #   "timestamp": "2020-08-18 20:52:56"
    # }

    # the same principle holds for the other levels: "debug", "warning" and "error"
    # e.g. logger.debug(), logger.warning(), logger.error()

Loosely based on the work of Bas Niesink
https://github.com/Alliander/Python-API-skeleton/blob/master/app/core/initializers/logging.py
"""

import logging
import logging.config
import sys

import structlog
import structlog.threadlocal
import warnings

import openstf_dbc.log.processors as custom_processors

__configured = False
__loggers = {}


def configure_logging(loglevel="INFO", runtime_env="local"):
    """Configure the logging functionality.

        Set-up and configure the structlog based logging functionality. The log level
        sets the lowest level which should be printed while the runtime environment
        determines if the logger outputs human readable or machine (json) output.

    Args:
        loglevel (str, int): The lowest level which should be printed
        runtime_env (str, RuntimeEnv): The runtime environment

    Returns:
        None
    """
    global __configured

    if isinstance(runtime_env, str) is False:
        try:
            runtime_env = runtime_env.value
        except AttributeError:
            raise ValueError("runtime_env need to be a string or an enum")

    if runtime_env.lower() == "local":
        _configure_logging_development(loglevel)
    elif runtime_env.lower() == "container":
        _configure_logging_deployed(loglevel)
    else:
        raise ValueError(f"Unknown runtime environment {runtime_env}")

    # Some third-party loggers can be really annoying
    # for example when using tab completions while debugging
    # raise the level of of those loggers
    if loglevel in ["DEBUG", "debug", logging.DEBUG]:
        _disable_third_party_loggers()

    __configured = True


def _disable_third_party_loggers():
    annoying_loggers_level = "ERROR"
    # parse en asyncio mess up ipython tab completion,
    # cfgrid produces a lot of irrelevant logs in de the data fetcher
    annoying_loggers = [
        "parso.python.diff",
        "parso.cache",
        "asyncio",
        "urllib3.connectionpool",
        "cfgrid.dataset",
    ]
    for name in annoying_loggers:
        logging.getLogger(name).setLevel(annoying_loggers_level)


def _configure_logging_development(loglevel):

    shared_processors = [
        # threadlocal processors
        structlog.threadlocal.merge_threadlocal_context,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,  # ConsoleRendered has a fixed positions
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
        structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S"),
    ]

    processors = shared_processors + [
        structlog.stdlib.ProcessorFormatter.wrap_for_formatter
    ]

    structlog.configure(
        processors=processors,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    formatter = structlog.stdlib.ProcessorFormatter(
        processor=structlog.dev.ConsoleRenderer(),
        foreign_pre_chain=shared_processors,
    )

    root_logger = logging.getLogger()

    # look for an already StreamHandler
    for handler in root_logger.handlers:
        if type(handler) is logging.StreamHandler:
            handler.setFormatter(formatter)
            handler.setStream(stream=sys.stdout)
            break
    # no StreamHandler found -> add new one
    else:
        handler = logging.StreamHandler(stream=sys.stdout)
        handler.setFormatter(formatter)
        root_logger.addHandler(handler)

    root_logger.setLevel(loglevel)


def _configure_logging_deployed(loglevel):
    shared_processors = [
        # threadlocal processors
        structlog.threadlocal.merge_threadlocal_context,
        # stdlib processors
        structlog.stdlib.add_log_level,  # add log level to event_dict
        structlog.stdlib.add_logger_name,  # add name to event_dict
        # structlog processors
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
        structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S"),
        # custom processors
        custom_processors.add_application_metadata,  #
        custom_processors.rename_forbidden_keys,
    ]

    pre_chain = shared_processors

    # Setup JSON-logging to stdout
    logging.captureWarnings(True)
    logging.config.dictConfig(
        {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "json": {
                    "()": structlog.stdlib.ProcessorFormatter,
                    "processor": structlog.processors.JSONRenderer(),
                    "foreign_pre_chain": pre_chain,
                }
            },
            "handlers": {
                "default": {
                    "level": loglevel,
                    "class": "logging.StreamHandler",
                    "formatter": "json",
                }
            },
            "loggers": {
                "": {
                    "handlers": ["default"],
                    "level": loglevel,
                    "propagate": True,
                }
            },
        }
    )

    processors = (
        [structlog.stdlib.filter_by_level]
        + shared_processors
        + [structlog.stdlib.ProcessorFormatter.wrap_for_formatter]
    )

    structlog.configure(
        processors=processors,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )


def get_logger(name=__name__):
    global __loggers

    # if logging is not yet configures
    if __configured is False:
        # configure logging using defaults
        configure_logging()

    # always return the same instance for the same name
    if name not in __loggers:
        __loggers[name] = structlog.get_logger(name)

    return __loggers[name]


def initialize_logging(name, log_level=None):
    """Empty placeholder for backward compatibility"""
    warnings.warn(
        f"DEPRECATION WARNING: initialize_logging({name}) will be depreciated soon."
        f"please use the new get_logger function. log_level argument not used: {log_level}"
    )

    return get_logger(name)


def set_log_level(level):
    """Placeholder for backward compatibility"""
    if level is None or level == "None":
        level = "INFO"
    configure_logging(level)
