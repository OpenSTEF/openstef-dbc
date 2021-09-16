<!--
SPDX-FileCopyrightText: 2021 2017-2021 Alliander N.V. <korte.termijn.prognoses@alliander.com>

SPDX-License-Identifier: MPL-2.0
-->
[![Python Build](https://github.com/alliander-opensource/openstf-dbc/actions/workflows/python-build.yaml/badge.svg?branch=master)](https://github.com/alliander-opensource/openstf-dbc/actions/workflows/python-build.yaml)
[![REUSE Compliance Check](https://github.com/alliander-opensource/openstf-dbc/actions/workflows/reuse-compliance.yml/badge.svg?branch=master)](https://github.com/alliander-opensource/openstf-dbc/actions/workflows/reuse-compliance.yml)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=alliander-opensource_openstf-db-connector&metric=alert_status)](https://sonarcloud.io/dashboard?id=alliander-opensource_openstf-db-connector)

```
# Openstf-dbc - Database connector for openstf (reference)

This repository houses the python package [openstf-dbc](https://pypi.org/project/openstf-dbc/), which provides an interface to openstf (reference) databases.

Related projects:
- [openstf-reference](https://github.com/alliander-opensource/openstf-reference)
- [openstf](https://github.com/alliander-opensource/short-term-forecasting)


## Install

1. Install by running `pip install openstf-dbc`
2. Enjoy!

## Usage

This is a package with functionality to support the openstf workflow. Most important is the DataBase class.
This class give access to the data used by openstf-reference via a convenient interface. You can use it, for example, to retrieve a prediction job by running the following lines of code:

```python
from openstf_dbc.config.config import ConfigManager
from openstf_dbc.database import DataBase

config = ConfigManager.load_project_config(PROJECT_ROOT)

db = DataBase(config)

pj = db.get_prediction_job(307)
```

## License
This project is licensed under the Mozilla Public License, version 2.0 - see LICENSE for details.

## Licenses third-party libraries
This project includes third-party libraries, which are licensed under their own respective Open-Source licenses. SPDX-License-Identifier headers are used to show which license is applicable. The concerning license files can be found in the LICENSES directory.

## Contributing

Please read [CONTRIBUTING.md](CONTRIBUTING.md) for details on our code of conduct, and the process for submitting pull requests to us.

## Contact

korte.termijn.prognoses@alliander.com
