# -*- coding: utf-8 -*-
#
#   Copyright 2024 Karellen, Inc.
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
#

import argparse
import datetime
import logging
from shutil import rmtree

from .utils import get_cache_dir, StripNL, run, install_python_k8s_client

import karellen.nomw

TRACE = 5


def trace(self, msg, *args, **kwargs):
    """
    Log 'msg % args' with severity 'TRACE'.

    To pass exception information, use the keyword argument exc_info with
    a true value, e.g.

    logger.trace("Houston, we have a %s", "interesting problem", exc_info=1)
    """
    if self.isEnabledFor(TRACE):
        self._log(TRACE, msg, args, **kwargs)


logging.addLevelName(5, "TRACE")
logging.Logger.trace = trace
logger = logging.getLogger("karellen.nomw")


def define_arg_parse():
    parser = argparse.ArgumentParser(prog="nomw",
                                     description="Node-On-My-Watch",
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    g = parser.add_mutually_exclusive_group()
    g.add_argument("--version", action="version", version=karellen.nomw.__version__,
                   help="print version and exit")
    g.add_argument("--clear-k8s-cache", action="store_true",
                   help="clear Kubernetes Client cache and exit")
    g.add_argument("--pre-cache-k8s-client", action="extend", nargs="+", type=int,
                   help="download specified K8S client library major(!) version(s) and exit")
    parser.add_argument("--pre-cache-k8s-client-no-patch", action="store_true", default=None,
                        help="do not patch the k8s client being pre-cached")
    parser.add_argument("--log-format", choices=["human", "json"], default="human",
                        help="whether to log for human or machine consumption")
    parser.add_argument("--log-file", type=argparse.FileType("w"), default=None,
                        help="where to log, defaults to `stderr`")
    parser.add_argument("-v", "--verbose", choices=["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG", "TRACE"],
                        default="INFO", help="how verbose do you want Kubernator to be")
    return parser


def init_logging(verbose, output_format, output_file):
    root_log = logging.root

    handler = logging.StreamHandler(output_file)
    root_log.addHandler(handler)

    if output_format == "human":
        if handler.stream.isatty():
            import coloredlogs
            fmt_cls = coloredlogs.ColoredFormatter

        else:
            fmt_cls = logging.Formatter

        def formatTime(record, datefmt=None):
            return datetime.datetime.fromtimestamp(record.created).isoformat()

        formatter = fmt_cls("%(asctime)s %(name)s %(levelname)s %(filename)s:%(lineno)d %(message)s")
        formatter.formatTime = formatTime
    else:
        import json_log_formatter

        class JSONFormatter(json_log_formatter.JSONFormatter):
            def json_record(self, message, extra, record: logging.LogRecord):
                extra = super(JSONFormatter, self).json_record(message, extra, record)
                extra["ts"] = datetime.datetime.fromtimestamp(record.created)
                extra["name"] = record.name
                extra["level"] = record.levelname
                extra["fn"] = record.filename
                extra["ln"] = record.lineno
                del extra["time"]
                return extra

        formatter = JSONFormatter()

    handler.setFormatter(formatter)
    logger.setLevel(logging._nameToLevel[verbose])


def clear_k8s_cache():
    cache_dir = get_cache_dir("python")
    _clear_cache("Clearing Kubernetes Client cache at %s", cache_dir)


def _clear_cache(msg, cache_dir):
    logger.info(msg, cache_dir)
    if cache_dir.exists():
        rmtree(cache_dir)


def pre_cache_k8s_clients(*versions, disable_patching=False):
    proc_logger = logger.getChild("proc")
    stdout_logger = StripNL(proc_logger.info)
    stderr_logger = StripNL(proc_logger.warning)

    for v in versions:
        logger.info("Caching K8S client library ~=v%s.0%s...", v,
                    " (no patches)" if disable_patching else "")
        install_python_k8s_client(run, v, logger, stdout_logger, stderr_logger, disable_patching)


def main():
    argparser = define_arg_parse()
    args = argparser.parse_args()
    if not args.pre_cache_k8s_client and args.pre_cache_k8s_client_no_patch is not None:
        argparser.error("--pre-cache-k8s-client-no-patch can only be used with --pre-cache-k8s-client")

    init_logging(args.verbose, args.log_format, args.log_file)

    try:
        if args.clear_k8s_cache:
            clear_k8s_cache()
            return

        if args.pre_cache_k8s_client:
            pre_cache_k8s_clients(*args.pre_cache_k8s_client,
                                  disable_patching=args.pre_cache_k8s_client_no_patch)
            return

    except SystemExit as e:
        return e.code
    except Exception as e:
        logger.fatal("Kubernator terminated with an error: %s", e, exc_info=e)
        return 1
    else:
        logger.info("Kubernator terminated successfully")
    finally:
        logging.shutdown()
