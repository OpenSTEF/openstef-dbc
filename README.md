<!--
SPDX-FileCopyrightText: 2021 2017-2021 Alliander N.V. <korte.termijn.prognoses@alliander.com>

SPDX-License-Identifier: MPL-2.0
-->

# icarus-base - Korte Termijn Prognoses

[Documentation (master)](https://jenkins-eba-ktp-ops.appx.cloud/job/icarus-tools/job/master/Code_20Documentation/)

Alliander heeft slimme, zelflerende algoritmes ontwikkeld die voorspellen wat de belasting op het elektriciteitsnet gaat worden voor de komende paar dagen. De toename van fluctuerende, duurzame bronnen zoals wind en zon maken de elektriciteitsbelasting steeds grilliger, terwijl de stijgende elektriciteitsvraag zorgt voor minder ruimte op het net. Om lokaal vraag en aanbod van energie op elkaar af te stemmen, helpt het om al vóóraf inzicht te hebben in wat er op het net gaat gebeuren. Meer informatie en achtergonden staan op onze [wiki](https://alliander.atlassian.net/wiki/spaces/DN/pages/1389199487/Team+Korte+Termijn+Prognoses)


## Install

1. Clone repository
2. Install by running `pip install .` in the `icarus-tools` directory
3. Collect acc-secrets.yaml and prd-secrets.yaml files from a KTP teammember or KeePass
4. Place it in the 'HOME' directory (see the config manager documentation for tips)
    * On Windows this is usually `C:\Users\<YOUR_USER_NAME>`
5. Enjoy!

## Usage

This is a package with functionality to support the Korte Termijn Prognoses workflow. Most important is the DataBase class.
This class give access to the data used by KTP via an convenient interface. You can use it, for example, to retrieve a prediciton job by running the following lines of code:

```python
from openstf_dbc.database import DataBase

db = DataBase()

pj = db.get_prediction_jon(307)
```

## Contributing

Please read [CONTRIBUTING.md](CONTRIBUTING.md) for details on our code of conduct, and the process for submitting pull requests to us.

## Contact

korte.termijn.prognoses@alliander.com
