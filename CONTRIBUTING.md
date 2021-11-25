<!--
SPDX-FileCopyrightText: 2021 2017-2021 Contributors to the OpenSTF project <korte.termijn.prognoses@alliander.com>

SPDX-License-Identifier: MPL-2.0
-->

# Contributing

When contributing to this repository, please first discuss the change you wish to make via issue,
email, or any other method with the owners of this repository before making a change. 

Please note we have a code of conduct, please follow it in all your interactions with the project.

## Style guide

This project uses the PEP 8 Style Guide for Python Code. For all details about the various conventions please refer to:

[PEP 8](https://www.python.org/dev/peps/pep-0008)

Furthermore the following conventions apply:

* Maximum line length: 88 characters
* Double quotes for strings, keys etc.
    * Except when double quotes in the middle of a string are required.

## Community Guidelines

This project follows the following [Code of Conduct](https://github.com/alliander-opensource/openstf-dbc/blob/main/CODE_OF_CONDUCT.md).

## Git branching

This project uses the [GitHub flow Workflow](https://guides.github.com/introduction/flow/) and branching model. The `main` branch always contains the latest release. New feature branches are branched from `main`. When a feature is finished it is merged back into `main` via a [Pull Request](https://docs.github.com/en/github/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/about-pull-requests#:~:text=Pull%20requests%20let%20you%20tell,merged%20into%20the%20base%20branch.).

This project also uses [Jira](https://www.atlassian.com/software/jira) for its [Scrum](https://en.wikipedia.org/wiki/Scrum_software_development) planning. In order to connect git branches to Jira it is prefpreferred that the user story `id` (e.g. KTP-753) is added to the branch name.

The following convention will be used for feature branches: 'Feature [jiraticketnumber] [descripttion]' or 'Feature [name feature]' when no Jiraticketnumber is avialable.  So for example:  `Feature ktp 753 unittest all schedulers` or `Feature unittest all schedulers`.

The following convention will be used for bugfix branches: 'Bugfix [jiraticketnumber] [descripttion]' or 'Bugfix [name feature]' when no Jiraticketnumber is avialable.  So for example:  `Bugfix ktp 1425 use training days` or `Bugfix use training days`.


## Pull Request Process

1. Ensure any install or build dependencies are removed before the end of the layer when doing a 
   build.
2. Update the README.md with details of changes to the interface, this includes new environment 
   variables, exposed ports, useful file locations and container parameters.
3. Increase the version numbers in any examples files and the README.md to the new version that this
   Pull Request would represent. The versioning scheme we use is [SemVer](http://semver.org/).
4. You may merge the Pull Request in once you have the sign-off of two other developers, or if you 
   do not have permission to do that, you may request the second reviewer to merge it for you.

## Attribution

This Contributing.md is adapted from Google
available at
https://github.com/google/new-project/blob/master/docs/contributing.md



