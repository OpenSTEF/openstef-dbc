<!--
SPDX-FileCopyrightText: 2017-2022 Contributors to the OpenSTEF project <korte.termijn.prognoses@alliander.com>

SPDX-License-Identifier: MPL-2.0
-->
[![Python Build](https://github.com/openstef/openstef-dbc/actions/workflows/python-build.yaml/badge.svg?branch=master)](https://github.com/openstef/openstef-dbc/actions/workflows/python-build.yaml)
[![REUSE Compliance Check](https://github.com/openstef/openstef-dbc/actions/workflows/reuse-compliance.yml/badge.svg?branch=master)](https://github.com/openstef/openstef-dbc/actions/workflows/reuse-compliance.yml)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=OpenSTEF_openstef-dbc&metric=alert_status)](https://sonarcloud.io/dashboard?id=OpenSTEF_openstef-dbc)


# Openstef-dbc - Database connector for openstef (reference)

This repository houses the python package [openstef-dbc](https://pypi.org/project/openstef-dbc/), which provides an interface to openstef (reference) databases.

Related projects:
- [openstef-reference](https://github.com/openstef/openstef-reference)
- [openstef](https://github.com/openstef/short-term-forecasting)

## Install

1. Install by running `pip install openstef-dbc`
2. Enjoy!

## Usage

This is a package with functionality to support the openstef workflow. Most important is the DataBase class.
This class give access to the data used by openstef-reference via a convenient interface. You can use it, for example, to retrieve a prediction job by running the following lines of code:

```python
from openstef_dbc.database import DataBase

db = DataBase(config)

pj = db.get_prediction_job(307)
```
Where `config` is a `pydantic.BaseSettings` object.

## License
This project is licensed under the Mozilla Public License, version 2.0 - see LICENSE for details.

## Licenses third-party libraries
This project includes third-party libraries, which are licensed under their own respective Open-Source licenses. SPDX-License-Identifier headers are used to show which license is applicable. The concerning license files can be found in the LICENSES directory.

## Contributing
Please read [CODE_OF_CONDUCT.md](https://github.com/OpenSTEF/.github/blob/main/CODE_OF_CONDUCT.md), [CONTRIBUTING.md](https://github.com/OpenSTEF/.github/blob/main/CONTRIBUTING.md) and [PROJECT_GOVERNANACE.md](https://github.com/OpenSTEF/.github/blob/main/PROJECT_GOVERNANCE.md) for details on the process for submitting pull requests to us.

## Contact
Please read [SUPPORT.md](https://github.com/OpenSTEF/.github/blob/main/SUPPORT.md) for how to connect and get into contact with the OpenSTEF project
