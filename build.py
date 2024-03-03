# -*- coding: utf-8 -*-
#
#   Copyright 2020 Express Systems USA, Inc
#   Copyright 2021 Karellen, Inc.
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

import textwrap
from subprocess import check_call

from pybuilder.core import (use_plugin, init, Author, task)

use_plugin("pypi:karellen_pyb_plugin", ">=0.0.1")
use_plugin("python.coveralls")
use_plugin("python.vendorize")
use_plugin("filter_resources")

name = "nomw"
version = "0.0.1.dev"

summary = "Node-On-My-Watch (NOMW) is a utility for keeping K8S EC2 nodes fresh by killing them off while ensuring the running workload remains stable"
authors = [Author("Karellen, Inc.", "supervisor@karellen.co")]
maintainers = [Author("Karellen, Inc.", "supervisor@karellen.co"),
               Author("Arcadiy Ivanov", "arcadiy@karellen.co")]

url = "https://github.com/karellen/node-on-my-watch"
urls = {
    "Bug Tracker": "https://github.com/karellen/node-on-my-watch/issues",
    "Source Code": "https://github.com/karellen/node-on-my-watch/",
    "Documentation": "https://github.com/karellen/node-on-my-watch/"
}
license = "Apache License, Version 2.0"

requires_python = ">=3.9"

default_task = ["analyze", "publish"]


@init
def set_properties(project):
    project.depends_on("gevent", ">=21.1.2")
    project.depends_on("kubernetes", "~=29.0")
    project.depends_on("json-log-formatter", "~=0.3")
    project.depends_on("platformdirs", "~=4.2")
    project.depends_on("requests", "~=2.25")
    project.depends_on("boto3", "~=1.34")
    project.depends_on("coloredlogs", "~=15.0")

    project.set_property("coverage_break_build", False)
    project.set_property("cram_fail_if_no_tests", False)

    project.set_property("integrationtest_inherit_environment", True)

    project.set_property("copy_resources_target", "$dir_dist/node_on_my_watch")
    project.get_property("copy_resources_glob").append("LICENSE")
    project.set_property("filter_resources_target", "$dir_dist")
    project.get_property("filter_resources_glob").append("karellen/nomw/__init__.py")
    project.include_file("kubernator", "LICENSE")

    project.set_property("distutils_upload_sign", False)
    project.set_property("distutils_upload_sign_identity", None)
    project.set_property("distutils_upload_repository_key", None)
    project.set_property("distutils_console_scripts", ["nomw = karellen.nomw:main"])
    project.set_property("distutils_setup_keywords", ["kubernetes", "k8s", "kube", "AWS", "node"])

    project.set_property("distutils_classifiers", [
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: POSIX",
        "Operating System :: POSIX :: Linux",
        "Environment :: Console",
        "Topic :: Utilities",
        "Topic :: System :: Monitoring",
        "Topic :: System :: Distributed Computing",
        "Topic :: System :: Clustering",
        "Topic :: System :: Networking",
        "Intended Audience :: System Administrators",
        "Intended Audience :: Developers",
        "Development Status :: 4 - Beta"
    ])
    project.set_property('pybuilder_header_plugin_break_build', False)
    project.set_property("pybuilder_header_plugin_expected_header",
                         textwrap.dedent("""\
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
                         """))


@task
def publish(project):
    image = f"ghcr.io/karellen/node-on-my-watch"
    versioned_image = f"{image}:{project.dist_version}"
    project.set_property("docker_image", image)
    labels = ["-t", versioned_image]

    # Do not tag with latest if it's a development build
    if project.version == project.dist_version:
        labels += ["-t", f"{image}:latest"]

    check_call(["docker", "build"] + labels + ["."])


@task
def upload(project):
    check_call(["docker", "push", project.get_property("docker_image"), "-a"])
