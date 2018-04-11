# The Archives Unleashed Toolkit
[![Build Status](https://travis-ci.org/archivesunleashed/aut.svg?branch=master)](https://travis-ci.org/archivesunleashed/aut)
[![codecov](https://codecov.io/gh/archivesunleashed/aut/branch/master/graph/badge.svg)](https://codecov.io/gh/archivesunleashed/aut)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.archivesunleashed/aut/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.archivesunleashed/aut)
[![Javadoc](https://javadoc-badge.appspot.com/io.archivesunleashed/aut.svg?label=javadoc)](http://java.docs.archivesunleashed.io/0.15.0/apidocs/index.html)
[![Scaladoc](https://javadoc-badge.appspot.com/io.archivesunleashed/aut.svg?label=scaladoc)](http://java.docs.archivesunleashed.io/0.15.0/scaladocs/index.html)
[![LICENSE](https://img.shields.io/badge/license-Apache-blue.svg?style=flat-square)](./LICENSE)
[![Contribution Guidelines](http://img.shields.io/badge/CONTRIBUTING-Guidelines-blue.svg)](./CONTRIBUTING.md)

The Archives Unleashed Toolkit is an open-source platform for analyzing web archives. Tight integration with Hadoop provides powerful tools for analytics and data processing via Apache Spark. Our full documentation can be found at <http://archivesunleashed.org/aut/>.

The Archives Unleashed Toolkit is part of the [Archives Unleashed Project](http://archivesunleashed.org/).

## Getting Started

You can download the [latest release here](https://github.com/archivesunleashed/aut/releases), build it yourself as per the instructions below, or use [Docker](https://github.com/archivesunleashed/docker-aut#use).

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

Once built or downloaded, you can follow the basic set of tutorials [here](http://archivesunleashed.org/aut/).

# License

Licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0).

# Acknowledgments

This work is primarily supported by the [Andrew W. Mellon Foundation](https://uwaterloo.ca/arts/news/multidisciplinary-project-will-help-historians-unlock). Additional funding for the Toolkit has come from the U.S. National Science Foundation, Columbia University Library's Mellon-funded Web Archiving Incentive Award, the Natural Sciences and Engineering Research Council of Canada, the Social Sciences and Humanities Research Council of Canada, and the Ontario Ministry of Research and Innovation's Early Researcher Award program. Any opinions, findings, and conclusions or recommendations expressed are those of the researchers and do not necessarily reflect the views of the sponsors.
