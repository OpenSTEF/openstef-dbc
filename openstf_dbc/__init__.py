# SPDX-FileCopyrightText: 2021 2017-2021 Alliander N.V. <korte.termijn.prognoses@alliander.com>
#
# SPDX-License-Identifier: MPL-2.0

from importlib.metadata import version, PackageNotFoundError

try:
    __version__ = version("openstf_dbc")
except PackageNotFoundError:
    # package is not installed
    pass
