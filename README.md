<!--
SPDX-FileCopyrightText: 2021 2017-2021 Alliander N.V. <korte.termijn.prognoses@alliander.com>

SPDX-License-Identifier: MPL-2.0
-->

# Openstf-db-connector - Database connector for OpenSTF (reference)

This repository houses the python package `openstf-dbc`, which provides an interface to OpenSTF (reference) databases. 
Related projects:
- [OpenSTF-reference](https://github.com/alliander-opensource/openstf-reference)
- [OpenSTF](https://github.com/alliander-opensource/short-term-forecasting)


## Install

1. Clone repository
2. Install by running `pip install openstf_dbc`
3. Enjoy!

## Usage

This is a package with functionality to support the OpenSTF workflow. Most important is the DataBase class.
This class give access to the data used by Openstf-reference via a convenient interface. You can use it, for example, to retrieve a prediction job by running the following lines of code:

```python
from openstf_dbc.database import DataBase

db = DataBase()

pj = db.get_prediction_jon(307)
```

## Contributing

Please read [CONTRIBUTING.md](CONTRIBUTING.md) for details on our code of conduct, and the process for submitting pull requests to us.

## Contact

korte.termijn.prognoses@alliander.com
