# The Archives Unleashed Toolkit
[![Build Status](https://travis-ci.org/archivesunleashed/aut.svg?branch=master)](https://travis-ci.org/archivesunleashed/aut)
[![codecov](https://codecov.io/gh/archivesunleashed/aut/branch/master/graph/badge.svg)](https://codecov.io/gh/archivesunleashed/aut)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.archivesunleashed/aut/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.archivesunleashed/aut)
[![Javadoc](https://javadoc-badge.appspot.com/io.archivesunleashed/aut.svg?label=javadoc)](http://java.docs.archivesunleashed.io/0.16.0/apidocs/index.html)
[![Scaladoc](https://javadoc-badge.appspot.com/io.archivesunleashed/aut.svg?label=scaladoc)](http://java.docs.archivesunleashed.io/0.16.0/scaladocs/index.html)
[![LICENSE](https://img.shields.io/badge/license-Apache-blue.svg?style=flat-square)](./LICENSE)
[![Contribution Guidelines](http://img.shields.io/badge/CONTRIBUTING-Guidelines-blue.svg)](./CONTRIBUTING.md)

The Archives Unleashed Toolkit is an open-source toolkit for analyzing web archives built around [Apache Spark](https://spark.apache.org/). This toolkit is part of the [Archives Unleashed Project](http://archivesunleashed.org/).

The toolkit grew out of a previous project called [Warcbase](https://github.com/lintool/warcbase). The following article provides a nice overview, much of which is still relevant:

+ Jimmy Lin, Ian Milligan, Jeremy Wiebe, and Alice Zhou. [Warcbase: Scalable Analytics Infrastructure for Exploring Web Archives](https://dl.acm.org/authorize.cfm?key=N46731). _ACM Journal on Computing and Cultural Heritage_, 10(4), Article 22, 2017.

## Getting Started

### Easy

If you have Apache Spark ready to go, it's as easy as:

```
$ spark-shell --packages "io.archivesunleashed:aut:0.16.0"`
```

### A little less easy

You can download the [latest release here](https://github.com/archivesunleashed/aut/releases) and include it like so:

```
$ spark-shell --jars /path/to/aut-0.16.0-fatjar.jar"`
```

### Even less easy

Build it yourself as per the instructions below:

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

### I want to use Docker!

Ok! Take a quick spin with `aut` with [Docker](https://github.com/archivesunleashed/docker-aut#use).

## Documentation! Or, how do I use this?

Once built or downloaded, you can follow the basic set of recipes and tutorials [here](http://archivesunleashed.org/aut/).

# License

Licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0).

# Acknowledgments

This work is primarily supported by the [Andrew W. Mellon Foundation](https://uwaterloo.ca/arts/news/multidisciplinary-project-will-help-historians-unlock). Additional funding for the Toolkit has come from the U.S. National Science Foundation, Columbia University Library's Mellon-funded Web Archiving Incentive Award, the Natural Sciences and Engineering Research Council of Canada, the Social Sciences and Humanities Research Council of Canada, and the Ontario Ministry of Research and Innovation's Early Researcher Award program. Any opinions, findings, and conclusions or recommendations expressed are those of the researchers and do not necessarily reflect the views of the sponsors.
