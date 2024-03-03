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

import logging
import os
import sys
from collections.abc import Callable
from functools import partial
from importlib.metadata import version as pkg_version
from io import BytesIO
from io import StringIO as io_StringIO
from pathlib import Path
from subprocess import Popen, PIPE, DEVNULL, CalledProcessError, TimeoutExpired
from typing import Union, IO, BinaryIO, TextIO, AnyStr, Iterable

from gevent import spawn, Timeout
from platformdirs import user_cache_dir


class StripNL:
    def __init__(self, func):
        self._func = func

    def __call__(self, line: str):
        return self._func(line.rstrip("\r\n"))


class StringIO:
    def __init__(self, trimmed=True):
        self.write = self.write_trimmed if trimmed else self.write_untrimmed
        self._buf = io_StringIO()

    def write_untrimmed(self, line):
        self._buf.write(line)

    def write_trimmed(self, line):
        self._buf.write(f"{line}\n")

    def getvalue(self):
        return self._buf.getvalue()


logger = logging.getLogger("karellen.nomw.util")
proc_logger = logger.getChild("proc")
stdout_logger = StripNL(proc_logger.info)
stderr_logger = StripNL(proc_logger.warning)


def get_app_cache_dir():
    return Path(user_cache_dir("nomw"))


def get_cache_dir(category: str, sub_category: str = None):
    config_dir = get_app_cache_dir() / category
    if sub_category:
        config_dir = config_dir / sub_category
    if not config_dir.exists():
        config_dir.mkdir(parents=True)

    return config_dir


def install_python_k8s_client(run, package_major, logger, logger_stdout, logger_stderr):
    cache_dir = get_cache_dir("python")
    package_major_dir = cache_dir / str(package_major)
    package_major_dir_str = str(package_major_dir)

    if not package_major_dir.exists():
        package_major_dir.mkdir(parents=True, exist_ok=True)
        run([sys.executable, "-m", "pip", "install", "--no-deps", "--no-input", "--pre",
             "--root-user-action=ignore", "--break-system-packages", "--disable-pip-version-check",
             "--target", package_major_dir_str, f"kubernetes~={package_major}.0"], logger_stdout, logger_stderr).wait()

    return package_major_dir


def _get_kubernetes_client_version(self):
    return pkg_version("kubernetes").split(".")


def log_level_to_verbosity_count(level: int):
    return int(-level / 10 + 6)


class K8SClientWrapper:
    def __init__(self):
        self.server_version = None
        self.server_git_version = None

        self.client = None
        self.embedded_pkg_version = self._get_kubernetes_client_version()

    def setup_client(self):
        if not self.server_version:
            self._setup_client()

        server_minor = self.server_version[1]

        logger.info("Using Kubernetes client version =~%s.0 for server version %s",
                    server_minor, ".".join(self.server_version))
        pkg_dir = install_python_k8s_client(run, server_minor, logger, stdout_logger, stderr_logger)

        modules_to_delete = []
        for k, v in sys.modules.items():
            if k == "kubernetes" or k.startswith("kubernetes."):
                modules_to_delete.append(k)
        for k in modules_to_delete:
            del sys.modules[k]

        logger.info("Adding sys.path reference to %s", pkg_dir)
        sys.path.insert(0, str(pkg_dir))
        self.embedded_pkg_version = self._get_kubernetes_client_version()
        logger.info("Switching to Kubernetes client version %s", ".".join(self.embedded_pkg_version))
        self._setup_client()

    def _get_kubernetes_client_version(self):
        return pkg_version("kubernetes").split(".")

    def _setup_client(self):
        from kubernetes import client

        self.client = self._setup_k8s_client()
        version = client.VersionApi(self.client).get_code()
        if "-eks-" in version.git_version:
            git_version = version.git_version.split("-")[0]
        else:
            git_version = version.git_version

        self.server_version = git_version[1:].split(".")
        self.server_git_version = git_version

        logger.info("Found Kubernetes %s on %s", self.server_git_version, self.client.configuration.host)

    def _setup_k8s_client(self):
        from kubernetes import client
        from kubernetes.config import load_incluster_config, load_kube_config, ConfigException

        try:
            logger.debug("Trying K8S in-cluster configuration")
            load_incluster_config()
            logger.info("Running K8S with in-cluster configuration")
        except ConfigException as e:
            logger.trace("K8S in-cluster configuration failed", exc_info=e)
            logger.debug("Initializing K8S with kubeconfig configuration")
            load_kube_config()

        k8s_client = client.ApiClient()

        return k8s_client


def stream_writer_buf(pipe: BinaryIO, source):
    with pipe:
        if isinstance(source, Callable):
            for buf in source():
                pipe.write(buf)
        else:
            pipe.write(source)


def stream_writer_text(pipe: TextIO, source):
    with pipe:
        if isinstance(source, Callable):
            pipe.writelines(source())
        else:
            pipe.write(source)


def stream_reader_buf(pipe: BinaryIO, sink_func):
    buf = bytearray(16384)
    while read := pipe.readinto(buf):
        sink_func(memoryview(buf)[:read])


def stream_reader_line(pipe: TextIO, sink_func):
    for line in pipe:
        sink_func(line)


class ProcessRunner:
    def __init__(self, args,
                 stdout: Union[None, int, IO, Callable[[AnyStr], None]],
                 stderr: Union[None, int, IO, Callable[[AnyStr], None]],
                 stdin: Union[None, int, bytes, str, IO, Callable[[], Iterable[AnyStr]]] = DEVNULL,
                 *,
                 safe_args=None, universal_newlines=True, **kwargs):
        self._safe_args = safe_args or args
        logger.trace("Starting %r", self._safe_args)

        if "env" not in kwargs:
            kwargs["env"] = os.environ

        self._proc = Popen(args,
                           stdout=PIPE if isinstance(stdout, Callable) else (stdout if stdout is not None else DEVNULL),
                           stderr=PIPE if isinstance(stderr, Callable) else (stderr if stderr is not None else DEVNULL),
                           stdin=PIPE if isinstance(stdin, (Callable, bytes, str)) else
                           (stdin if stdin is not None else DEVNULL),
                           universal_newlines=universal_newlines,
                           **kwargs)

        self._stdin_writer = (spawn(partial(stream_writer_text if universal_newlines else stream_writer_buf,
                                            self._proc.stdin, stdin))
                              if isinstance(stdin, (Callable, bytes, str)) else None)
        self._stdout_reader = spawn(partial(stream_reader_line if universal_newlines else stream_reader_buf,
                                            self._proc.stdout, stdout)) if isinstance(stdout, Callable) else None
        self._stderr_reader = spawn(partial(stream_reader_line if universal_newlines else stream_reader_buf,
                                            self._proc.stderr, stderr)) if isinstance(stderr, Callable) else None

    @property
    def stdout(self):
        if not self._stdout_reader:
            raise RuntimeError("not available")
        return self._proc.stdout

    @property
    def stderr(self):
        if not self._stderr_reader:
            raise RuntimeError("not available")
        return self._proc.stderr

    @property
    def stdin(self):
        if not self._stdin_writer:
            raise RuntimeError("not available")
        return self._proc.stdin

    def wait(self, fail=True, timeout=None, _out_func=None):
        with Timeout(timeout, TimeoutExpired):
            retcode = self._proc.wait()
            if self._stdin_writer:
                self._stdin_writer.join()
            if self._stdout_reader:
                self._stdout_reader.join()
            if self._stderr_reader:
                self._stderr_reader.join()
        if fail and retcode:
            output = None
            if _out_func:
                output = _out_func()
            raise CalledProcessError(retcode, self._safe_args, output=output)
        return retcode

    def terminate(self):
        self._proc.terminate()

    def kill(self):
        self._proc.kill()


run = ProcessRunner


def run_capturing_out(args, stderr_logger, stdin=DEVNULL, *, safe_args=None, universal_newlines=True, **kwargs):
    out = StringIO(trimmed=False) if universal_newlines else BytesIO()
    proc = run(args, out.write, stderr_logger, stdin, safe_args=safe_args, universal_newlines=universal_newlines,
               **kwargs)
    proc.wait(_out_func=lambda: out.getvalue())
    return out.getvalue()
