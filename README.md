# The Archives Unleashed Toolkit
[![Build Status](https://travis-ci.org/archivesunleashed/aut.svg?branch=master)](https://travis-ci.org/archivesunleashed/aut)
[![codecov](https://codecov.io/gh/archivesunleashed/aut/branch/master/graph/badge.svg)](https://codecov.io/gh/archivesunleashed/aut)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.archivesunleashed/aut/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.archivesunleashed/aut)
[![Javadoc](https://javadoc-badge.appspot.com/io.archivesunleashed/aut.svg?label=javadoc)](http://api.docs.archivesunleashed.io/0.50.0/apidocs/index.html)
[![Scaladoc](https://javadoc-badge.appspot.com/io.archivesunleashed/aut.svg?label=scaladoc)](http://api.docs.archivesunleashed.io/0.50.0/scaladocs/index.html)
[![LICENSE](https://img.shields.io/badge/license-Apache-blue.svg?style=flat)](https://www.apache.org/licenses/LICENSE-2.0)
[![Contribution Guidelines](http://img.shields.io/badge/CONTRIBUTING-Guidelines-blue.svg)](./CONTRIBUTING.md)

The Archives Unleashed Toolkit is an open-source platform for analyzing web archives built on [Apache Spark](http://spark.apache.org/), which provides powerful tools for analytics and data processing. This toolkit is part of the [Archives Unleashed Project](http://archivesunleashed.org/).

The toolkit grew out of a previous project called [Warcbase](https://github.com/lintool/warcbase). The following article provides a nice overview, much of which is still relevant:

+ Jimmy Lin, Ian Milligan, Jeremy Wiebe, and Alice Zhou. [Warcbase: Scalable Analytics Infrastructure for Exploring Web Archives](https://dl.acm.org/authorize.cfm?key=N46731). _ACM Journal on Computing and Cultural Heritage_, 10(4), Article 22, 2017.

## Dependencies

### Java

The Archives Unleashed Toolkit requires Java 8.

For macOS: You can find information on Java [here](https://java.com/en/download/help/mac_install.xml), or install with [homebrew](https://brew.sh) and then:

```bash
brew cask install java8
```

On Debian based system you can install Java using `apt`:

```bash
apt install openjdk-8-jdk
```

Before `spark-shell` can launch, `JAVA_HOME` must be set. If you receive an error that `JAVA_HOME` is not set, you need to point it to where Java is installed. On Linux, this might be `export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64` or on macOS it might be `export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.8.0_74.jdk/Contents/Home`.

### Python

If you would like to use the Archives Unleashed Toolkit with PySpark and Jupyter Notebooks, you'll need to have a modern version of Python installed. We recommend using the [Anaconda Distribution](https://www.anaconda.com/distribution). This _should_ install Jupyter Notebook, as well as the PySpark bindings. If it doesn't, you can install either with `conda install` or `pip install`.

### Apache Spark

Download and unzip [Apache Spark](https://spark.apache.org) to a location of your choice.

```bash
curl -L "https://archive.apache.org/dist/spark/spark-2.4.4/spark-2.4.4-bin-hadoop2.7.tgz" > spark-2.4.4-bin-hadoop2.7.tgz
tar -xvf spark-2.4.4-bin-hadoop2.7.tgz
```

## Getting Started

### Building Locally

Clone the repo:

```shell
$ git clone http://github.com/archivesunleashed/aut.git
```

You can then build The Archives Unleashed Toolkit.

```shell
$ mvn clean install
```

### Archives Unleashed Toolkit with Spark Shell

There are a two options for loading the Archives Unleashed Toolkit. The advantages and disadvantages of using either option are going to depend on your setup (single machine vs cluster):

```shell
$ spark-shell --help

  --jars JARS                 Comma-separated list of jars to include on the driver
                              and executor classpaths.
  --packages                  Comma-separated list of maven coordinates of jars to include
                              on the driver and executor classpaths. Will search the local
                              maven repo, then maven central and any additional remote
                              repositories given by --repositories. The format for the
                              coordinates should be groupId:artifactId:version.
```

#### As a package

Release version:

```shell
$ spark-shell --packages "io.archivesunleashed:aut:0.50.0"
```

HEAD (built locally):

```shell
$ spark-shell --packages "io.archivesunleashed:aut:0.18.2-SNAPSHOT"
```

#### With an UberJar

Release version:

```shell
$ spark-shell --jars /path/to/aut-0.50.0-fatjar.jar
```

HEAD (built locally):

```shell
$ spark-shell --jars /path/to/aut/target/aut-0.18.2-SNAPSHOT-fatjar.jar
```

### Archives Unleashed Toolkit with PySpark

To run PySpark with the Archives Unleashed Toolkit loaded, you will need to provide PySpark with the Java/Scala package, and the Python bindings. The Java/Scala packages can be provided with `--packages` or `--jars` as described above. The Python bindings can be [downloaded](https://github.com/archivesunleashed/aut/releases/download/aut-0.50.0/aut-0.50.0.zip), or [built locally](#building-locally) (the zip file will be found in the `target` directory.

In each of the examples below, `/path/to/python` is listed. If you are unsure where your Python is, it can be found with `which python`.

#### As a package

Release version:

```shell
$ export PYSPARK_PYTHON=/path/to/python; export PYSPARK_DRIVER_PYTHON=/path/to/python; /path/to/spark/bin/pyspark --py-files aut-0.50.0.zip --packages "io.archivesunleashed:aut:0.50.0"
```

HEAD (built locally):

```shell
$ export PYSPARK_PYTHON=/path/to/python; export PYSPARK_DRIVER_PYTHON=/path/to/python; /path/to/spark/bin/pyspark --py-files /home/nruest/Projects/au/aut/target/aut.zip --packages "io.archivesunleashed:aut:0.18.2-SNAPSHOT"
```

#### With an UberJar

Release version:

```shell
$ export PYSPARK_PYTHON=/path/to/python; export PYSPARK_DRIVER_PYTHON=/path/to/python; /path/to/spark/bin/pyspark --py-files aut-0.50.0.zip --jars /path/to/aut-0.50.0-fatjar.jar
```

HEAD (built locally):

```shell
$ export PYSPARK_PYTHON=/path/to/python; export PYSPARK_DRIVER_PYTHON=/path/to/python; /path/to/spark/bin/pyspark --py-files /home/nruest/Projects/au/aut/target/aut.zip --jars /path/to/aut-0.18.2-SNAPSHOT-fatjar.jar
```

### Archives Unleashed Toolkit with Jupyter

To run a [Jupyter Notebook](https://jupyter.org/install) with the Archives Unleashed Toolkit loaded, you will need to provide PySpark the Java/Scala package, and the Python bindings. The Java/Scala packages can be provided with `--packages` or `--jars` as described above. The Python bindings can be [downloaded](https://github.com/archivesunleashed/aut/releases/download/aut-0.50.0/aut-0.50.0.zip), or [built locally](#Introduction) (the zip file will be found in the `target` directory.

#### As a package

Release version:

```shell
$ export PYSPARK_DRIVER_PYTHON=jupyter; export PYSPARK_DRIVER_PYTHON_OPTS=notebook; /path/to/spark/bin/pyspark --py-files aut-0.50.0.zip --packages "io.archivesunleashed:aut:0.50.0"
```

HEAD (built locally):

```shell 
$ export PYSPARK_DRIVER_PYTHON=jupyter; export PYSPARK_DRIVER_PYTHON_OPTS=notebook; /path/to/spark/bin/pyspark --py-files /home/nruest/Projects/au/aut/target/aut.zip --packages "io.archivesunleashed:aut:0.18.2-SNAPSHOT"
```

#### With an UberJar

Release version:

```shell
$ export PYSPARK_DRIVER_PYTHON=jupyter; export PYSPARK_DRIVER_PYTHON_OPTS=notebook; /path/to/spark/bin/pyspark --py-files aut-0.50.0.zip --jars /path/to/aut-0.50.0-fatjar.jar
```

HEAD (built locally):

```shell
$ export PYSPARK_DRIVER_PYTHON=jupyter; export PYSPARK_DRIVER_PYTHON_OPTS=notebook; /path/to/spark/bin/pyspark --py-files /home/nruest/Projects/au/aut/target/aut.zip --jars /path/to/aut-0.18.2-SNAPSHOT-fatjar.jar
```

A Jupyter Notebook _should_ automatically load in your browser at <http://localhost:8888>. You may be asked for a token upon first launch, which just offers a bit of security. The token is available in the load screen and will look something like this:

```
[I 19:18:30.893 NotebookApp] Writing notebook server cookie secret to /run/user/1001/jupyter/notebook_cookie_secret
[I 19:18:31.111 NotebookApp] JupyterLab extension loaded from /home/nruest/bin/anaconda3/lib/python3.7/site-packages/jupyterlab
[I 19:18:31.111 NotebookApp] JupyterLab application directory is /home/nruest/bin/anaconda3/share/jupyter/lab
[I 19:18:31.112 NotebookApp] Serving notebooks from local directory: /home/nruest/Projects/au/aut
[I 19:18:31.112 NotebookApp] The Jupyter Notebook is running at:
[I 19:18:31.112 NotebookApp] http://localhost:8888/?token=87e7a47c5a015cb2b846c368722ec05c1100988fd9dcfe04
[I 19:18:31.112 NotebookApp] Use Control-C to stop this server and shut down all kernels (twice to skip confirmation).
[C 19:18:31.140 NotebookApp]

    To access the notebook, open this file in a browser:
        file:///run/user/1001/jupyter/nbserver-9702-open.html
    Or copy and paste one of these URLs:
        http://localhost:8888/?token=87e7a47c5a015cb2b846c368722ec05c1100988fd9dcfe04
```

Create a new notebook by clicking “New” (near the top right of the Jupyter homepage) and select “Python 3” from the drop down list.

The notebook will open in a new window. In the first cell enter:

```python
from aut import *

archive = WebArchive(sc, sqlContext, "src/test/resources/warc/")

webpages = archive.webpages()
webpages.printSchema()
```

Then hit <kbd>Shift</kbd>+<kbd>Enter</kbd>, or press the play button.

If you receive no errors, and see the following, you are ready to begin working with your web archives!

![](https://user-images.githubusercontent.com/218561/63203995-42684080-c061-11e9-9361-f5e6177705ff.png)

## Documentation! Or, what can I do?

Once built or downloaded, you can follow the basic set of recipes and tutorials [here](https://github.com/archivesunleashed/aut-docs/tree/master/current#the-archives-unleashed-toolkit-latest-documentation).

# License

Licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0).

# Acknowledgments

This work is primarily supported by the [Andrew W. Mellon Foundation](https://mellon.org/). Other financial and in-kind support comes from the [Social Sciences and Humanities Research Council](http://www.sshrc-crsh.gc.ca/), [Compute Canada](https://www.computecanada.ca/), the [Ontario Ministry of Research, Innovation, and Science](https://www.ontario.ca/page/ministry-research-innovation-and-science), [York University Libraries](https://www.library.yorku.ca/web/), [Start Smart Labs](http://www.startsmartlabs.com/), and the [Faculty of Arts](https://uwaterloo.ca/arts/) and [David R. Cheriton School of Computer Science](https://cs.uwaterloo.ca/) at the [University of Waterloo](https://uwaterloo.ca/).

Any opinions, findings, and conclusions or recommendations expressed are those of the researchers and do not necessarily reflect the views of the sponsors.
