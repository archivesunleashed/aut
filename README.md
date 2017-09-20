# The Archives Unleashed Toolkit
[![Build Status](https://travis-ci.org/archivesunleashed/aut.svg?branch=master)](https://travis-ci.org/archivesunleashed/aut)
[![codecov](https://codecov.io/gh/archivesunleashed/aut/branch/master/graph/badge.svg)](https://codecov.io/gh/archivesunleashed/aut)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.archivesunleashed/aut/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.archivesunleashed/aut)
[![LICENSE](https://img.shields.io/badge/license-Apache-blue.svg?style=flat-square)](./LICENSE)
[![Contribution Guidelines](http://img.shields.io/badge/CONTRIBUTING-Guidelines-blue.svg)](./CONTRIBUTING.md)

The Archives Unleashed Toolkit is an open-source platform for analyzing web archives. Tight integration with Hadoop provides powerful tools for analytics and data processing via Apache Spark. Our full documentation can be found at <http://docs.archivesunleashed.io/>.

## Getting Started

You can download the [latest release here](https://github.com/archivesunleashed/aut/releases), or build it yourself as per the instructions below.

Clone the repo:

```
$ git clone http://github.com/archivesunleashed/aut.git
$ cd aut
```

You can then build The Archives Unleashed Toolkit.

```
$ mvn clean install
```

For the impatient, to skip tests:

```
$ mvn clean install -DskipTests
```

Once built or downloaded, [you can follow the basic set of tutorials here](http://docs.archivesunleashed.io/).

The Archives Unleashed Toolkit is built against CDH 5.7.1:

+ Hadoop version: 2.6.0-cdh5.7.1
+ Spark version: 1.6.0-cdh5.7.1

The Hadoop ecosystem is evolving rapidly, so there may be incompatibilities with other versions.

## Docker Image Build and Run

We have included a Dockerfile that contains all the necessary dependencies to build an image from the source. The image contains both `spark-shell` and `spark-notebook` in a ready-to-use state.

To build the image from source:

```
$ git clone http://github.com/archivesunleashed/aut.git
$ cd aut
$ docker image build -t aut .
```

Building the image might take a few minutes. After making any changes in the code, rerun the last command. Once ready, following command can be used to run the `spark-notebook` (which is the default command of the image):

```
$ docker container run -it --rm -p 9001:9001 aut
```

Alternatively, `spark-notebook` can be invoked explicitly with additional arguments (such as `-v` for verbose mode):

```
$ docker container run -it --rm -p 9001:9001 aut spark-notebook -v
```

On a successful run notebook panel should be accessible at http://localhost:9001/. Replace `-it --rm` with `-d` to run the notebook in detached mode.

To run the `spark-shell`, use the following command:

```
$ docker container run -it --rm aut spark-shell
```

For data persistence, two volumes `/notes` and `/data` are defined. Mount any host directory in `/notes` for notebook files and in `/data` for `WARC` and other data files. These volumes can then be used in scripts accordingly.

The image is carefully crafted to maximize build caching, minimal size, small number of layers, easier to use, and customizable. Necessary JAR file is loaded already, hence, no need to explicitly load it in each notebook or script.

# License

Licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0).

# Acknowledgments

This work is primarily supported by the [Andrew W. Mellon Foundation](https://uwaterloo.ca/arts/news/multidisciplinary-project-will-help-historians-unlock). Additional funding for the Toolkit has come from the U.S. National Science Foundation, Columbia University Library's Mellon-funded Web Archiving Incentive Award, the Natural Sciences and Engineering Research Council of Canada, the Social Sciences and Humanities Research Council of Canada, and the Ontario Ministry of Research and Innovation's Early Researcher Award program. Any opinions, findings, and conclusions or recommendations expressed are those of the researchers and do not necessarily reflect the views of the sponsors.
