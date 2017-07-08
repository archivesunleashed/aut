# The Archives Unleashed Toolkit
[![Build Status](https://travis-ci.org/archivesunleashed/aut.svg?branch=master)](https://travis-ci.org/archivesunleashed/aut)
[![Contribution Guidelines](http://img.shields.io/badge/CONTRIBUTING-Guidelines-blue.svg)](./CONTRIBUTING.md)
[![LICENSE](https://img.shields.io/badge/license-Apache-blue.svg?style=flat-square)](./LICENSE)
[![codecov](https://codecov.io/gh/archivesunleashed/aut/branch/master/graph/badge.svg)](https://codecov.io/gh/archivesunleashed/aut)

The Archives Unleashed Toolkit is an open-source platform for analyzing web archives. Tight integration with Hadoop provides powerful tools for analytics and data processing via Apache Spark.

## Getting Started

Clone the repo:

```
$ git clone http://github.com/archivesunleashed/aut.git
```

You can then build The Archives Unleashed Toolkit.

```
$ mvn clean install
```

For the impatient, to skip tests:

```
$ mvn clean install -DskipTests
```

The Archives Unleashed Toolkit is built against CDH 5.7.1:

+ Hadoop version: 2.6.0-cdh5.7.1
+ Spark version: 1.6.0-cdh5.7.1

The Hadoop ecosystem is evolving rapidly, so there may be incompatibilities with other versions.

# License

Licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0).

# Acknowledgments

This work is primarily supported by the [Andrew W. Mellon Foundation](https://uwaterloo.ca/arts/news/multidisciplinary-project-will-help-historians-unlock). Additional funding for the Toolkit has come from the U.S. National Science Foundation, the Natural Sciences and Engineering Research Council of Canada, the Social Sciences and Humanities Research Council of Canada, and the Ontario Ministry of Research and Innovation's Early Researcher Award program. Any opinions, findings, and conclusions or recommendations expressed are those of the researchers and do not necessarily reflect the views of the sponsors.