# openstf_dbc - OpenSTF database connector

Python package for easy acces to the OpenSTF (reference) databases.

Aimed to be used in conjunction with:
- [OpenSTF](https://github.com/alliander-opensource/short-term-forecasting)
- [OpenSTF reference](https://github.com/alliander-opensource/openstf-reference)

## Install

1. Clone repository
2. Install by running `pip install .` in the root directory
3. Enjoy!

## Usage

This is a package with functionality to support the OpenSTF workflow. Most important is the DataBase class.
This class give access to the (reference) databases a convenient interface. You can use it, for example, to retrieve a prediciton job by running the following lines of code:

```python
from openstf_dbc.database import DataBase

db = DataBase()

pj = db.get_prediction_jon(307)
```

## Contributing

Please read [CONTRIBUTING.md](CONTRIBUTING.md) for details on our code of conduct, and the process for submitting pull requests to us.

## Contact

korte.termijn.prognoses@alliander.com
