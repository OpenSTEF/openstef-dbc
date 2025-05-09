# SPDX-FileCopyrightText: 2017-2022 Contributors to the OpenSTEF project <korte.termijn.prognoses@alliander.com> # noqa E501>
#
# SPDX-License-Identifier: MPL-2.0

from pathlib import Path

from setuptools import find_packages, setup

pkg_dir = Path(__file__).parent.absolute()


def read_requirements_from_file():
    with open(pkg_dir / "requirements.txt") as fh:
        requirements = []
        for line in fh:
            line = line.strip()
            if "#" in line:
                line = line[: line.index("#")].strip()
            if len(line) == 0:
                continue
            requirements.append(line)
        return requirements


def read_long_description_from_readme():
    with open("README.md", "r", encoding="utf-8") as fh:
        return fh.read()


setup(
    name="openstef_dbc",
    version="3.8.2",
    packages=find_packages(include=["openstef_dbc", "openstef_dbc.*"]),
    description="Database Connection for OpenSTEF",
    long_description=read_long_description_from_readme(),
    long_description_content_type="text/markdown",
    url="https://github.com/openstef/openstef-dbc",
    author="Alliander N.V",
    author_email="korte.termijn.prognoses@alliander.com",
    license="MPL-2.0",
    keywords=["database", "energy", "forecasting", "machinelearning"],
    # See https://setuptools.readthedocs.io/en/latest/userguide/datafiles.html
    # for more information
    package_data={
        # Include anything in the data directory
        "openstef_dbc": ["data/*", "*.license"]
    },
    python_requires=">=3.7.0",
    install_requires=read_requirements_from_file(),
    setup_requires=["wheel", "Cython"],
    tests_require=["pytest", "pytest-cov", "flake8"],
    classifiers=[
        r"Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        r"License :: OSI Approved :: Mozilla Public License 2.0 (MPL 2.0)",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
)
