# The Archives Unleashed Toolkit
[![codecov](https://codecov.io/gh/archivesunleashed/aut/branch/main/graph/badge.svg)](https://codecov.io/gh/archivesunleashed/aut)
[![Maven Central](https://img.shields.io/maven-central/v/io.archivesunleashed/aut)](https://central.sonatype.com/artifact/io.archivesunleashed/aut)
[![Scaladoc](https://img.shields.io/badge/Scaladoc-1.2.0-blue?style=flat)](https://api.docs.archivesunleashed.io/1.2.0/scaladocs/io/archivesunleashed/index.html)
[![UserDocs](https://img.shields.io/badge/UserDocs-1.2.0-blue?style=flat)](https://aut.docs.archivesunleashed.org/docs/home)
[![LICENSE](https://img.shields.io/badge/license-Apache-blue.svg?style=flat)](https://www.apache.org/licenses/LICENSE-2.0)
[![Contribution Guidelines](http://img.shields.io/badge/CONTRIBUTING-Guidelines-blue.svg)](./CONTRIBUTING.md)

The Archives Unleashed Toolkit is an open-source platform for analyzing web archives using [Apache Spark](https://spark.apache.org/), and makes use of [Sparkling](https://github.com/internetarchive/Sparkling) for parsing W/ARC records. The toolkit provides powerful tools for analytics and data processing. It is part of the [Archives Unleashed Project](https://archivesunleashed.org/).

To learn more about the Toolkit and how to use, please see our [comprehensive documentation](https://aut.docs.archivesunleashed.org/).

If you would like a more in-depth look at the project, please check out the following two articles:

+ Nick Ruest, Jimmy Lin, Ian Milligan, and Samantha Fritz. [The Archives Unleashed Project: Technology, Process, and Community to Improve Scholarly Access to Web Archives](https://hdl.handle.net/10315/37506). Proceedings of the 2020 IEEE/ACM Joint Conference on Digital Libraries (JCDL 2020), Wuhan, China.
+ Jimmy Lin, Ian Milligan, Jeremy Wiebe, and Alice Zhou. [Warcbase: Scalable Analytics Infrastructure for Exploring Web Archives](https://doi.org/10.1145/3097570). _ACM Journal on Computing and Cultural Heritage_, 10(4), Article 22, 2017.

## Dependencies

- Java 11
- Python 3.8+ (PySpark)
- Scala 2.12
- Apache Spark 3.5.x (Hadoop 3)

More information on setting up dependencies can be found [here](https://aut.docs.archivesunleashed.org/docs/next/dependencies).

## Building

Clone the repo:

```shell
git clone https://github.com/archivesunleashed/aut.git
```

You can then build The Archives Unleashed Toolkit.

```shell
mvn clean install
```

## Testing

Run the Scala unit tests:

```shell
mvn test
```

Run the unit tests plus the integration tests, which check the shaded fat jar for classpath collisions with Spark:

```shell
mvn verify
```

The Python tests live in `src/test/python/`. The pure-Python tests only need `pytest`, and the Spark-dependent tests also need the built fat jar and PySpark. See the [Python tests README](src/test/python/README.md) for details.

## Code Formatting

Pull requests are checked for formatting in CI. Scala code must pass [scalafmt](https://scalameta.org/scalafmt/) using the repository's `.scalafmt.conf`, and Python code must pass [isort](https://pycqa.github.io/isort/) and [black](https://black.readthedocs.io/):

```shell
scalafmt --list
isort -c -l 88 -m 3 -tc src/main/python/aut/* src/test/python/*.py
black --check src/main/python/aut/* src/test/python/*.py
```

## Usage

The Toolkit can be used to submit a variety of extraction jobs with `spark-submit`, as well used as a library via `spark-submit`, `pyspark`, or in your own application. More information on using the Toolkit can be found [here](https://aut.docs.archivesunleashed.org/docs/usage).


## Citing Archives Unleashed

How to cite the Archives Unleashed Toolkit or Cloud in your research:

> Nick Ruest, Jimmy Lin, Ian Milligan, and Samantha Fritz. 2020. The Archives Unleashed Project: Technology, Process, and Community to Improve Scholarly Access to Web Archives. In _Proceedings of the ACM/IEEE Joint Conference on Digital Libraries in 2020 (JCDL '20)_. Association for Computing Machinery, New York, NY, USA, 157–166. DOI:https://doi.org/10.1145/3383583.3398513

Your citations help to further the recognition of using open-source tools for scientific inquiry, assists in growing the web archiving community, and acknowledges the efforts of contributors to this project.

## License

Licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0).

## Acknowledgments

This work is primarily supported by the [Andrew W. Mellon Foundation](https://mellon.org/). Other financial and in-kind support comes from the [Social Sciences and Humanities Research Council](http://www.sshrc-crsh.gc.ca/), [Compute Canada](https://www.computecanada.ca/) (now the [Digital Research Alliance of Canada](https://alliancecan.ca/)), the [Ontario Ministry of Research, Innovation, and Science](https://www.ontario.ca/page/ministry-research-innovation-and-science), [York University Libraries](https://www.library.yorku.ca/web/), [Start Smart Labs](http://www.startsmartlabs.com/), and the [Faculty of Arts](https://uwaterloo.ca/arts/) and [David R. Cheriton School of Computer Science](https://cs.uwaterloo.ca/) at the [University of Waterloo](https://uwaterloo.ca/).

Any opinions, findings, and conclusions or recommendations expressed are those of the researchers and do not necessarily reflect the views of the sponsors.
