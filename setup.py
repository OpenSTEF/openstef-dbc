# SPDX-FileCopyrightText: 2021 2017-2021 Alliander N.V. <korte.termijn.prognoses@alliander.com>
#
# SPDX-License-Identifier: MPL-2.0

import setuptools
import os
from setuptools import setup


pkg_dir = os.path.dirname(os.path.realpath(__file__))
# package description
with open(os.path.join(pkg_dir, "README.md")) as f:
    long_description = f.read()
with open(os.path.join(pkg_dir, "requirements.txt")) as f:
    requirements = []
    for line in f:
        line = line.strip()
        if "#" in line:
            line = line[: line.index("#")].strip()
        if len(line) == 0:
            continue
        requirements.append(line)
with open(os.path.join(pkg_dir, "PACKAGENAME")) as f:
    pkg_name = f.read().strip().strip("\n")
with open(os.path.join(pkg_dir, "VERSION")) as f:
    version = f.read().strip().strip("\n")
    if "BETA" in os.environ:
        version += f"b-{version}"
        print(f"Make beta version number: {version}")

setup(
    name=pkg_name,
    version=version,
    author="KTP",
    author_email="ktprognoses@alliander.com",
    description="Database connector package for openstf (reference)",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Alliander-opensource/openstf-db-connector",
    packages=setuptools.find_packages(),
    install_requires=requirements,
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
)
