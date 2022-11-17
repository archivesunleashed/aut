# Changelog

## [aut-1.2.0](https://github.com/archivesunleashed/aut/tree/aut-1.2.0) (2022-11-17)

[Full Changelog](https://github.com/archivesunleashed/aut/compare/aut-1.1.1...aut-1.2.0)

**Closed issues:**

- Include last modified date for a resource [\#546](https://github.com/archivesunleashed/aut/issues/546)

**Merged pull requests:**

- Add scalafix and remove unused imports. [\#548](https://github.com/archivesunleashed/aut/pull/548) ([ruebot](https://github.com/ruebot))
- Last modified headers [\#547](https://github.com/archivesunleashed/aut/pull/547) ([ruebot](https://github.com/ruebot))

## [aut-1.1.1](https://github.com/archivesunleashed/aut/tree/aut-1.1.1) (2022-10-31)

[Full Changelog](https://github.com/archivesunleashed/aut/compare/aut-1.1.0...aut-1.1.1)

**Fixed bugs:**

- DomainGraph should use YYYYMMDD not YYYYMMDDHHMMSS [\#544](https://github.com/archivesunleashed/aut/issues/544)

**Merged pull requests:**

- Use YYYYMMDD for crawl\_date for DomainGraphExtractor. [\#545](https://github.com/archivesunleashed/aut/pull/545) ([ruebot](https://github.com/ruebot))
- Bump jsoup from 1.14.2 to 1.15.3 [\#543](https://github.com/archivesunleashed/aut/pull/543) ([dependabot[bot]](https://github.com/apps/dependabot))

## [aut-1.1.0](https://github.com/archivesunleashed/aut/tree/aut-1.1.0) (2022-06-17)

[Full Changelog](https://github.com/archivesunleashed/aut/compare/aut-1.0.0...aut-1.1.0)

**Fixed bugs:**

- org.apache.tika.mime.MimeTypeException: Invalid media type name: application/rss+xml lang=utf-8 [\#542](https://github.com/archivesunleashed/aut/issues/542)

**Closed issues:**

- Add ARCH text files derivatives [\#540](https://github.com/archivesunleashed/aut/issues/540)

**Merged pull requests:**

- Add ARCH text files derivatives. [\#541](https://github.com/archivesunleashed/aut/pull/541) ([ruebot](https://github.com/ruebot))

## [aut-1.0.0](https://github.com/archivesunleashed/aut/tree/aut-1.0.0) (2022-06-10)

[Full Changelog](https://github.com/archivesunleashed/aut/compare/aut-0.91.0...aut-1.0.0)

**Implemented enhancements:**

- Remove http headers, and html on webpages\(\) [\#538](https://github.com/archivesunleashed/aut/issues/538)
- Add domain column to webpages\(\) [\#534](https://github.com/archivesunleashed/aut/issues/534)
- Replace Java ARC/WARC record processing library [\#494](https://github.com/archivesunleashed/aut/issues/494)
- Method to perform finer-grained selection of ARCs and WARCs [\#247](https://github.com/archivesunleashed/aut/issues/247)
- Unnecessary buffer copying [\#18](https://github.com/archivesunleashed/aut/issues/18)

**Fixed bugs:**

- Discard date RDD filter only takes a single string, not a list of strings. [\#532](https://github.com/archivesunleashed/aut/issues/532)
- Extract gzip data from transfer-encoded WARC [\#493](https://github.com/archivesunleashed/aut/issues/493)
- ARC reader string vs int error on record length [\#492](https://github.com/archivesunleashed/aut/issues/492)

**Closed issues:**

- java.lang.RuntimeException: Unsupported literal type class scala.collection.immutable.Set$Set1 Set\(liberal.ca\) [\#529](https://github.com/archivesunleashed/aut/issues/529)
- Improve CommandLineApp.scala test coverage [\#262](https://github.com/archivesunleashed/aut/issues/262)
- Improve ExtractBoilerpipeText.scala test coverage [\#261](https://github.com/archivesunleashed/aut/issues/261)
- Improve ArchiveRecord.scala test coverage [\#260](https://github.com/archivesunleashed/aut/issues/260)
- Unit testing for RecordLoader [\#182](https://github.com/archivesunleashed/aut/issues/182)
- Improve ArchiveRecordWritable.java test coverage [\#76](https://github.com/archivesunleashed/aut/issues/76)
- Improve WarcRecordUtils.java test coverage [\#74](https://github.com/archivesunleashed/aut/issues/74)
- Improve ArcRecordUtils.java test coverage [\#73](https://github.com/archivesunleashed/aut/issues/73)
- Improve ExtractDate.scala test coverage [\#64](https://github.com/archivesunleashed/aut/issues/64)
- Remove org.apache.commons.httpclient [\#23](https://github.com/archivesunleashed/aut/issues/23)

**Merged pull requests:**

- Make webpages\(\) consistent across aut and ARCH. [\#539](https://github.com/archivesunleashed/aut/pull/539) ([ruebot](https://github.com/ruebot))
- Update README [\#537](https://github.com/archivesunleashed/aut/pull/537) ([ruebot](https://github.com/ruebot))
- Fix codecov GitHub action. [\#536](https://github.com/archivesunleashed/aut/pull/536) ([ruebot](https://github.com/ruebot))
- Bump commons-compress from 1.14 to 1.21 [\#535](https://github.com/archivesunleashed/aut/pull/535) ([dependabot[bot]](https://github.com/apps/dependabot))
- Remove Java w/arc processing, and replace it with Sparkling. [\#533](https://github.com/archivesunleashed/aut/pull/533) ([ruebot](https://github.com/ruebot))
- Bump xercesImpl from 2.12.0 to 2.12.2 [\#527](https://github.com/archivesunleashed/aut/pull/527) ([dependabot[bot]](https://github.com/apps/dependabot))

## [aut-0.91.0](https://github.com/archivesunleashed/aut/tree/aut-0.91.0) (2022-01-21)

[Full Changelog](https://github.com/archivesunleashed/aut/compare/aut-0.90.4...aut-0.91.0)

**Implemented enhancements:**

- Include timestamp in crawl date [\#525](https://github.com/archivesunleashed/aut/issues/525)

**Merged pull requests:**

- Change crawl\_date format to YYYYMMDDHHMMSS, update hasDate filter. [\#526](https://github.com/archivesunleashed/aut/pull/526) ([ruebot](https://github.com/ruebot))

## [aut-0.90.4](https://github.com/archivesunleashed/aut/tree/aut-0.90.4) (2021-11-01)

[Full Changelog](https://github.com/archivesunleashed/aut/compare/aut-0.90.3...aut-0.90.4)

**Implemented enhancements:**

- Replace scala-uri library from ExtractDomain and just parse public\_suffix\_list.dat  [\#521](https://github.com/archivesunleashed/aut/issues/521)

**Fixed bugs:**

- Scaladocs haven't been created since 0.90.0 release [\#522](https://github.com/archivesunleashed/aut/issues/522)

**Merged pull requests:**

- Replace scala-uri library from ExtractDomain. [\#524](https://github.com/archivesunleashed/aut/pull/524) ([ruebot](https://github.com/ruebot))
- Issue 522 [\#523](https://github.com/archivesunleashed/aut/pull/523) ([ruebot](https://github.com/ruebot))

## [aut-0.90.3](https://github.com/archivesunleashed/aut/tree/aut-0.90.3) (2021-10-22)

[Full Changelog](https://github.com/archivesunleashed/aut/compare/aut-0.90.2...aut-0.90.3)

**Fixed bugs:**

- ExtractDomains returns non-Apex Domains [\#519](https://github.com/archivesunleashed/aut/issues/519)

**Merged pull requests:**

- Update ExtractDomain to extract apex domains. [\#520](https://github.com/archivesunleashed/aut/pull/520) ([ruebot](https://github.com/ruebot))
- Bump jsoup from 1.13.1 to 1.14.2 [\#518](https://github.com/archivesunleashed/aut/pull/518) ([dependabot[bot]](https://github.com/apps/dependabot))

## [aut-0.90.2](https://github.com/archivesunleashed/aut/tree/aut-0.90.2) (2021-05-12)

[Full Changelog](https://github.com/archivesunleashed/aut/compare/aut-0.90.1...aut-0.90.2)

**Fixed bugs:**

- ARC file name appearing in `url` list [\#516](https://github.com/archivesunleashed/aut/issues/516)
- WARC-Target-URI in Wget warc files is not parsed properly [\#514](https://github.com/archivesunleashed/aut/issues/514)

**Merged pull requests:**

- Filter or filedesc and dns records from arcs. [\#517](https://github.com/archivesunleashed/aut/pull/517) ([ruebot](https://github.com/ruebot))
- Handle wget WARC-Target-URI formatting. [\#515](https://github.com/archivesunleashed/aut/pull/515) ([ruebot](https://github.com/ruebot))

## [aut-0.90.1](https://github.com/archivesunleashed/aut/tree/aut-0.90.1) (2021-04-29)

[Full Changelog](https://github.com/archivesunleashed/aut/compare/aut-0.90.0...aut-0.90.1)

**Fixed bugs:**

- crawl\_date is not included on binary information jobs when documentation says it is [\#512](https://github.com/archivesunleashed/aut/issues/512)

**Merged pull requests:**

- Add missing crawl\_date column to binary information jobs. [\#513](https://github.com/archivesunleashed/aut/pull/513) ([ruebot](https://github.com/ruebot))
- Update jsoup to 1.13.1 [\#511](https://github.com/archivesunleashed/aut/pull/511) ([ruebot](https://github.com/ruebot))

## [aut-0.90.0](https://github.com/archivesunleashed/aut/tree/aut-0.90.0) (2021-01-27)

[Full Changelog](https://github.com/archivesunleashed/aut/compare/aut-0.80.0...aut-0.90.0)

**Fixed bugs:**

- Python implementation of .all\(\) has .keepValidPages\(\) incorrectly applied to it [\#502](https://github.com/archivesunleashed/aut/issues/502)
- Extract hyperlinks from wayback machine [\#501](https://github.com/archivesunleashed/aut/issues/501)
- Release 0.80.0 JAR produces error; built 0.80.1 fatjar built on repo works [\#495](https://github.com/archivesunleashed/aut/issues/495)

**Closed issues:**

- Migrate CI infrastructure from TravisCI to GitHub Action [\#506](https://github.com/archivesunleashed/aut/issues/506)
- Split tf into it's own repo [\#498](https://github.com/archivesunleashed/aut/issues/498)
- Change master branch to main branch [\#490](https://github.com/archivesunleashed/aut/issues/490)
- GitHub action - Run isort and black on Python code [\#488](https://github.com/archivesunleashed/aut/issues/488)
- Add scalafmt GitHub action [\#486](https://github.com/archivesunleashed/aut/issues/486)
- Add Google Java Formatter as a GitHub action [\#484](https://github.com/archivesunleashed/aut/issues/484)
- Packages build is often broken - should we support it? [\#483](https://github.com/archivesunleashed/aut/issues/483)
- Implement SaveToDisk in Python [\#478](https://github.com/archivesunleashed/aut/issues/478)
- Java 11 support [\#356](https://github.com/archivesunleashed/aut/issues/356)

**Merged pull requests:**

- ars-cloud compatibility with aut and Java 11 [\#510](https://github.com/archivesunleashed/aut/pull/510) ([ruebot](https://github.com/ruebot))
- Update to Spark 3.0.1 [\#508](https://github.com/archivesunleashed/aut/pull/508) ([ruebot](https://github.com/ruebot))
- Replace TravisCI with GitHub Actions. [\#507](https://github.com/archivesunleashed/aut/pull/507) ([ruebot](https://github.com/ruebot))
- Bump junit from 4.12 to 4.13.1 [\#505](https://github.com/archivesunleashed/aut/pull/505) ([dependabot[bot]](https://github.com/apps/dependabot))
- Fix relative links extraction [\#504](https://github.com/archivesunleashed/aut/pull/504) ([yxzhu16](https://github.com/yxzhu16))
- Remove .keepValidPages\(\) on .all\(\) Python implmentation. [\#503](https://github.com/archivesunleashed/aut/pull/503) ([ruebot](https://github.com/ruebot))
- Updates read.me to include citation section [\#500](https://github.com/archivesunleashed/aut/pull/500) ([SamFritz](https://github.com/SamFritz))
- Remove tf project; resolves \#498. [\#499](https://github.com/archivesunleashed/aut/pull/499) ([ruebot](https://github.com/ruebot))
- Add Python formatter GitHub Action. [\#489](https://github.com/archivesunleashed/aut/pull/489) ([ruebot](https://github.com/ruebot))
- Add scalafmt GitHub action and apply it to scala code. [\#487](https://github.com/archivesunleashed/aut/pull/487) ([ruebot](https://github.com/ruebot))
- Add Google Java Formatter as an action, and apply it. [\#485](https://github.com/archivesunleashed/aut/pull/485) ([ruebot](https://github.com/ruebot))
- Add Python implementation of SaveBytes. [\#482](https://github.com/archivesunleashed/aut/pull/482) ([ruebot](https://github.com/ruebot))
- Bump xercesImpl from 2.11.0 to 2.12.0 [\#481](https://github.com/archivesunleashed/aut/pull/481) ([dependabot[bot]](https://github.com/apps/dependabot))
- \[Skip Travis\] Trim README down given aut.docs.archivesunleashed.org [\#480](https://github.com/archivesunleashed/aut/pull/480) ([ruebot](https://github.com/ruebot))
- Spark 3.0.0 + Java 11 support. [\#375](https://github.com/archivesunleashed/aut/pull/375) ([ruebot](https://github.com/ruebot))

## [aut-0.80.0](https://github.com/archivesunleashed/aut/tree/aut-0.80.0) (2020-06-03)

[Full Changelog](https://github.com/archivesunleashed/aut/compare/aut-0.70.0...aut-0.80.0)

**Closed issues:**

- Broken link in documentation [\#476](https://github.com/archivesunleashed/aut/issues/476)
- Improve udfs/package.scala test coverage [\#473](https://github.com/archivesunleashed/aut/issues/473)
- Remove tabDelimit [\#471](https://github.com/archivesunleashed/aut/issues/471)
- Remove Extract Entities [\#469](https://github.com/archivesunleashed/aut/issues/469)
- PEP8 Naming  - UDFs, App method names, DataFrame names, and filters. [\#468](https://github.com/archivesunleashed/aut/issues/468)
- Python UDFs - class or not? [\#467](https://github.com/archivesunleashed/aut/issues/467)
- Remove ExtractImageDetailsDF.scala [\#464](https://github.com/archivesunleashed/aut/issues/464)
- github-stite-deploy uses password based authentication which is being deprecated by GitHub [\#461](https://github.com/archivesunleashed/aut/issues/461)
- Implement Python versions of Serializable APIs [\#410](https://github.com/archivesunleashed/aut/issues/410)
- Implement Python versions of App utilities [\#409](https://github.com/archivesunleashed/aut/issues/409)
- Implement Python versions of Matchbox utilities [\#408](https://github.com/archivesunleashed/aut/issues/408)
- Improve TupleFormatter.scala test coverage [\#59](https://github.com/archivesunleashed/aut/issues/59)
- Create tests for NERCombinedJson.scala [\#53](https://github.com/archivesunleashed/aut/issues/53)
- Create tests for NER3Classifier.scala [\#52](https://github.com/archivesunleashed/aut/issues/52)
- Create tests for ExtractEntities.scala [\#48](https://github.com/archivesunleashed/aut/issues/48)

**Merged pull requests:**

- Remove RDD suffixes on file, class, and object names. [\#479](https://github.com/archivesunleashed/aut/pull/479) ([ruebot](https://github.com/ruebot))
- PEP8 Python app method names. [\#477](https://github.com/archivesunleashed/aut/pull/477) ([ruebot](https://github.com/ruebot))
- Move Python UDF methods out of their own class. [\#475](https://github.com/archivesunleashed/aut/pull/475) ([ruebot](https://github.com/ruebot))
- Add DataFrame udf tests. [\#474](https://github.com/archivesunleashed/aut/pull/474) ([ruebot](https://github.com/ruebot))
- Remove tabDelimit. [\#472](https://github.com/archivesunleashed/aut/pull/472) ([ruebot](https://github.com/ruebot))
- Remove NER functionality. [\#470](https://github.com/archivesunleashed/aut/pull/470) ([ruebot](https://github.com/ruebot))
- Add ExtractPopularImages, WriteGEXF, and WriteGraphML to Python. [\#466](https://github.com/archivesunleashed/aut/pull/466) ([ruebot](https://github.com/ruebot))
- Remove ExtractImageDetailsDF; resolves \#464. [\#465](https://github.com/archivesunleashed/aut/pull/465) ([ruebot](https://github.com/ruebot))
- Implement Scala Matchbox UDFs in Python. [\#463](https://github.com/archivesunleashed/aut/pull/463) ([ruebot](https://github.com/ruebot))
- Import clean-up for df package. [\#462](https://github.com/archivesunleashed/aut/pull/462) ([ruebot](https://github.com/ruebot))

## [aut-0.70.0](https://github.com/archivesunleashed/aut/tree/aut-0.70.0) (2020-05-04)

[Full Changelog](https://github.com/archivesunleashed/aut/compare/aut-0.60.0...aut-0.70.0)

**Implemented enhancements:**

- Update PlainTextExtractor to just extract text [\#452](https://github.com/archivesunleashed/aut/issues/452)
- Migration of all RDD functionality over to DataFrames [\#223](https://github.com/archivesunleashed/aut/issues/223)

**Fixed bugs:**

- DomainFrequencyExtractor should remove WWW prefix [\#456](https://github.com/archivesunleashed/aut/issues/456)

**Closed issues:**

- For extractor \(spark-submit\) job, set Spark app name to be the extractor job name. [\#458](https://github.com/archivesunleashed/aut/issues/458)
- Remove RDD options from app [\#449](https://github.com/archivesunleashed/aut/issues/449)
- Add parquet as an app format option [\#448](https://github.com/archivesunleashed/aut/issues/448)
- Add datathon derivatives to app \(binary info, web pages, web graph [\#447](https://github.com/archivesunleashed/aut/issues/447)
- Update Java 8 instructions for MacOS [\#445](https://github.com/archivesunleashed/aut/issues/445)
- Add spark-submit to README [\#444](https://github.com/archivesunleashed/aut/issues/444)

**Merged pull requests:**

- \[skip travis\] README updates [\#460](https://github.com/archivesunleashed/aut/pull/460) ([ruebot](https://github.com/ruebot))
- Set spark-submit app name to be "aut - extractorName". [\#459](https://github.com/archivesunleashed/aut/pull/459) ([ruebot](https://github.com/ruebot))
- Add RemovePrefixWWWDF to DomainFrequencyExtractor. [\#457](https://github.com/archivesunleashed/aut/pull/457) ([ruebot](https://github.com/ruebot))
- Updating Java install instructions for MacOS, resolves \#445 [\#455](https://github.com/archivesunleashed/aut/pull/455) ([ianmilligan1](https://github.com/ianmilligan1))
-  Add option to save to Parquet for app. [\#454](https://github.com/archivesunleashed/aut/pull/454) ([ruebot](https://github.com/ruebot))
- Update PlainTextExtractor to output a single column; text. [\#453](https://github.com/archivesunleashed/aut/pull/453) ([ruebot](https://github.com/ruebot))
- Add a number of additional app extractors. [\#451](https://github.com/archivesunleashed/aut/pull/451) ([ruebot](https://github.com/ruebot))
- Remove RDD option in app; DataFrame only now. [\#450](https://github.com/archivesunleashed/aut/pull/450) ([ruebot](https://github.com/ruebot))
- \[skip-travis\] Add spark-submit option to README; resolves \#444. [\#446](https://github.com/archivesunleashed/aut/pull/446) ([ruebot](https://github.com/ruebot))

## [aut-0.60.0](https://github.com/archivesunleashed/aut/tree/aut-0.60.0) (2020-04-15)

[Full Changelog](https://github.com/archivesunleashed/aut/compare/aut-0.50.0...aut-0.60.0)

**Implemented enhancements:**

- Discussion: Restyle UDFs in the context of DataFrames [\#425](https://github.com/archivesunleashed/aut/issues/425)
- Add alt text column to imageGraph \(imageLinks\) [\#420](https://github.com/archivesunleashed/aut/issues/420)
- UDFs that filter on url should also filter on src [\#418](https://github.com/archivesunleashed/aut/issues/418)

**Fixed bugs:**

- CommandLineApp DomainGraphExtractor Uses Different Node IDs than WriteGraph [\#439](https://github.com/archivesunleashed/aut/issues/439)
- DomainGraphExtractor produces different output in RDD vs DF [\#436](https://github.com/archivesunleashed/aut/issues/436)
- Command line app fails because of missing log4j configuration [\#433](https://github.com/archivesunleashed/aut/issues/433)

**Closed issues:**

- Remove GraphXML and ExtractGraphX [\#442](https://github.com/archivesunleashed/aut/issues/442)
- Use Monochromatic Ids instead of hash to produce network identifiers. [\#440](https://github.com/archivesunleashed/aut/issues/440)
- Add graphml output to DomainGraphExtractor [\#435](https://github.com/archivesunleashed/aut/issues/435)
- Add webgraph, imagegraph, webpages, etc. to command line app [\#431](https://github.com/archivesunleashed/aut/issues/431)
- Rename imageLinks to imageGraph [\#419](https://github.com/archivesunleashed/aut/issues/419)

**Merged pull requests:**

- Remove GraphX support; resolves \#442. [\#443](https://github.com/archivesunleashed/aut/pull/443) ([ruebot](https://github.com/ruebot))
- Remove WriteGraph; resolves \#439. [\#441](https://github.com/archivesunleashed/aut/pull/441) ([ruebot](https://github.com/ruebot))
-  Add graphml output to CommandLineApp and DomainGraphExtractor. [\#438](https://github.com/archivesunleashed/aut/pull/438) ([ruebot](https://github.com/ruebot))
- Align RDD and DF output for DomainGraphExtractor. [\#437](https://github.com/archivesunleashed/aut/pull/437) ([ruebot](https://github.com/ruebot))
- Update log4j configuration to resolve \#433. [\#434](https://github.com/archivesunleashed/aut/pull/434) ([ruebot](https://github.com/ruebot))
- Add imagegraph, and webgraph to command line app. [\#432](https://github.com/archivesunleashed/aut/pull/432) ([ruebot](https://github.com/ruebot))
- Tweak hasDate to handle Seq. [\#430](https://github.com/archivesunleashed/aut/pull/430) ([ruebot](https://github.com/ruebot))
- Restyle keep/discard filter UDFs in the context of DataFrames [\#429](https://github.com/archivesunleashed/aut/pull/429) ([ruebot](https://github.com/ruebot))
- Update Spark and Hadoop versions. [\#426](https://github.com/archivesunleashed/aut/pull/426) ([ruebot](https://github.com/ruebot))
- update for 'src' column [\#424](https://github.com/archivesunleashed/aut/pull/424) ([SinghGursimran](https://github.com/SinghGursimran))
- \[skip travis\] Add pre-print link to README. [\#423](https://github.com/archivesunleashed/aut/pull/423) ([ruebot](https://github.com/ruebot))
- Add img alt text to imagegraph\(\); resolves \#420. [\#422](https://github.com/archivesunleashed/aut/pull/422) ([ruebot](https://github.com/ruebot))
- Rename imageLinks to imageGraph; resolves \#419 [\#421](https://github.com/archivesunleashed/aut/pull/421) ([ruebot](https://github.com/ruebot))
- Need --repositories flag with --packages. [\#417](https://github.com/archivesunleashed/aut/pull/417) ([ruebot](https://github.com/ruebot))

## [aut-0.50.0](https://github.com/archivesunleashed/aut/tree/aut-0.50.0) (2020-02-05)

[Full Changelog](https://github.com/archivesunleashed/aut/compare/aut-0.18.1...aut-0.50.0)

**Implemented enhancements:**

- Add crawl\_date to binary DataFrames and imageLinks [\#413](https://github.com/archivesunleashed/aut/issues/413)

**Fixed bugs:**

- 0.18.0 with --packages is broken [\#407](https://github.com/archivesunleashed/aut/issues/407)

**Closed issues:**

- .webpages\(\) additional tokenized columns? [\#402](https://github.com/archivesunleashed/aut/issues/402)
- Test and documentation inventory [\#372](https://github.com/archivesunleashed/aut/issues/372)

**Merged pull requests:**

- Clean up test descriptions, addresses \#372. [\#416](https://github.com/archivesunleashed/aut/pull/416) ([ruebot](https://github.com/ruebot))
- Remaining Matchbox implementations for Scala [\#415](https://github.com/archivesunleashed/aut/pull/415) ([SinghGursimran](https://github.com/SinghGursimran))
- Add crawl\_date to binary DataFrames and imageLinks. [\#414](https://github.com/archivesunleashed/aut/pull/414) ([ruebot](https://github.com/ruebot))
- Various DataFrame implementation updates for documentation clean-up; Addresses \#372. [\#406](https://github.com/archivesunleashed/aut/pull/406) ([ruebot](https://github.com/ruebot))
- Use https for maven repo. [\#405](https://github.com/archivesunleashed/aut/pull/405) ([ruebot](https://github.com/ruebot))
- Test clean-up. [\#404](https://github.com/archivesunleashed/aut/pull/404) ([ruebot](https://github.com/ruebot))
- Add language detection column to webpages. [\#403](https://github.com/archivesunleashed/aut/pull/403) ([ruebot](https://github.com/ruebot))
- DataFrame Implementation - Serializable APIs [\#401](https://github.com/archivesunleashed/aut/pull/401) ([SinghGursimran](https://github.com/SinghGursimran))
- Filter blank src/dest out of webgraph. [\#400](https://github.com/archivesunleashed/aut/pull/400) ([ruebot](https://github.com/ruebot))
- More df implementations [\#399](https://github.com/archivesunleashed/aut/pull/399) ([SinghGursimran](https://github.com/SinghGursimran))
- Scala imports cleanup. [\#398](https://github.com/archivesunleashed/aut/pull/398) ([ruebot](https://github.com/ruebot))
- More Serializable APIs for DataFrames [\#396](https://github.com/archivesunleashed/aut/pull/396) ([SinghGursimran](https://github.com/SinghGursimran))
- Update ExtractDateRDD test [\#395](https://github.com/archivesunleashed/aut/pull/395) ([ruebot](https://github.com/ruebot))
- Add doc comments for webpages and webgraph; resolves \#392. [\#394](https://github.com/archivesunleashed/aut/pull/394) ([ruebot](https://github.com/ruebot))
- Add additional filters for fextFiles; resolves \#362. [\#393](https://github.com/archivesunleashed/aut/pull/393) ([ruebot](https://github.com/ruebot))
- API implementations for DataFrame [\#391](https://github.com/archivesunleashed/aut/pull/391) ([SinghGursimran](https://github.com/SinghGursimran))
- Setup for Serializable APIs on DataFrames [\#389](https://github.com/archivesunleashed/aut/pull/389) ([SinghGursimran](https://github.com/SinghGursimran))
- Add and update tests, resolve textFiles bug.  [\#388](https://github.com/archivesunleashed/aut/pull/388) ([ruebot](https://github.com/ruebot))
- Dataframe matchbox Implementations [\#387](https://github.com/archivesunleashed/aut/pull/387) ([SinghGursimran](https://github.com/SinghGursimran))
- Clean-up underscore import, and scalastyle warnings. [\#386](https://github.com/archivesunleashed/aut/pull/386) ([ruebot](https://github.com/ruebot))
- Rename pages\(\) to webpages\(\). [\#384](https://github.com/archivesunleashed/aut/pull/384) ([ruebot](https://github.com/ruebot))
- More Data Frame Implementations + Code Refactoring [\#383](https://github.com/archivesunleashed/aut/pull/383) ([SinghGursimran](https://github.com/SinghGursimran))
- Extract popular images - Data Frame implementation [\#382](https://github.com/archivesunleashed/aut/pull/382) ([SinghGursimran](https://github.com/SinghGursimran))
- Append UDF with RDD or RF. [\#381](https://github.com/archivesunleashed/aut/pull/381) ([ruebot](https://github.com/ruebot))
- Matchbox utilities to DataFrames [\#380](https://github.com/archivesunleashed/aut/pull/380) ([SinghGursimran](https://github.com/SinghGursimran))
- Rename DF functions to be consistent with Python DF functions. [\#379](https://github.com/archivesunleashed/aut/pull/379) ([ruebot](https://github.com/ruebot))
- Converting output of NER Classifier to WANE Format [\#378](https://github.com/archivesunleashed/aut/pull/378) ([SinghGursimran](https://github.com/SinghGursimran))
- Finding Hyperlinks within Collection on Pages with Certain Keyword [\#377](https://github.com/archivesunleashed/aut/pull/377) ([SinghGursimran](https://github.com/SinghGursimran))
- Update README.md [\#376](https://github.com/archivesunleashed/aut/pull/376) ([lintool](https://github.com/lintool))
- Fix for Issue-368 [\#374](https://github.com/archivesunleashed/aut/pull/374) ([SinghGursimran](https://github.com/SinghGursimran))
- \[skip travis\] update description. see https://github.com/archivesunle… [\#373](https://github.com/archivesunleashed/aut/pull/373) ([ruebot](https://github.com/ruebot))
- Various UDF implementation and cleanup for DF [\#370](https://github.com/archivesunleashed/aut/pull/370) ([lintool](https://github.com/lintool))
- Update commons-compress to 1.19; CVE-2019-12402 [\#365](https://github.com/archivesunleashed/aut/pull/365) ([ruebot](https://github.com/ruebot))
- Add ComputeSHA1 method; resolves \#363. [\#364](https://github.com/archivesunleashed/aut/pull/364) ([ruebot](https://github.com/ruebot))
- Align NER output to WANE format [\#361](https://github.com/archivesunleashed/aut/pull/361) ([ruebot](https://github.com/ruebot))
- Update keepValidPages to include a filter on 200 OK. [\#360](https://github.com/archivesunleashed/aut/pull/360) ([ruebot](https://github.com/ruebot))
- Update to Spark 2.4.4 [\#358](https://github.com/archivesunleashed/aut/pull/358) ([ruebot](https://github.com/ruebot))
- \[skip travis\] Update links [\#357](https://github.com/archivesunleashed/aut/pull/357) ([ruebot](https://github.com/ruebot))
- Improve test coverage. [\#354](https://github.com/archivesunleashed/aut/pull/354) ([ruebot](https://github.com/ruebot))
- Add discardLanguage filter to RecordLoader. [\#353](https://github.com/archivesunleashed/aut/pull/353) ([ruebot](https://github.com/ruebot))

## [aut-0.18.1](https://github.com/archivesunleashed/aut/tree/aut-0.18.1) (2020-01-17)

[Full Changelog](https://github.com/archivesunleashed/aut/compare/aut-0.18.0...aut-0.18.1)

**Implemented enhancements:**

- Enhance keepValidPages  [\#359](https://github.com/archivesunleashed/aut/issues/359)
- Add discardLanguage filter [\#352](https://github.com/archivesunleashed/aut/issues/352)

**Fixed bugs:**

- textFiles does not filter properly [\#390](https://github.com/archivesunleashed/aut/issues/390)
- DataFrame error with text files: java.net.MalformedURLException: unknown protocol: filedesc [\#362](https://github.com/archivesunleashed/aut/issues/362)

**Closed issues:**

- Missing doc comments [\#392](https://github.com/archivesunleashed/aut/issues/392)
- Bug in ArcTest? Why run RemoveHTML? [\#369](https://github.com/archivesunleashed/aut/issues/369)
- UDF CaMeL cASe consistency issues [\#368](https://github.com/archivesunleashed/aut/issues/368)
- ExtractDomain or ExtractBaseDomain? [\#367](https://github.com/archivesunleashed/aut/issues/367)
- Align DataFrame boilerplate in Python and Scala [\#366](https://github.com/archivesunleashed/aut/issues/366)
- Create a ComputeSHA1 method [\#363](https://github.com/archivesunleashed/aut/issues/363)
- Discussion: Should we align our Named Entity Recognition output with WANE format? [\#297](https://github.com/archivesunleashed/aut/issues/297)
- DataFrame discussion: open thread [\#190](https://github.com/archivesunleashed/aut/issues/190)

## [aut-0.18.0](https://github.com/archivesunleashed/aut/tree/aut-0.18.0) (2019-08-21)

[Full Changelog](https://github.com/archivesunleashed/aut/compare/aut-0.17.0...aut-0.18.0)

**Implemented enhancements:**

- Add method for unknown extensions in binary extractions [\#343](https://github.com/archivesunleashed/aut/issues/343)
- Use Tika's detected MIME type instead of ArchiveRecord getMimeType? [\#342](https://github.com/archivesunleashed/aut/issues/342)
- Add filter/keep by http status to RecordLoader class [\#315](https://github.com/archivesunleashed/aut/issues/315)
- Audio binary object extraction [\#307](https://github.com/archivesunleashed/aut/issues/307)
- Video binary object extraction [\#306](https://github.com/archivesunleashed/aut/issues/306)
- Powerpoint binary object extraction [\#305](https://github.com/archivesunleashed/aut/issues/305)
- Doc binary object extraction [\#304](https://github.com/archivesunleashed/aut/issues/304)
- Spreadsheet binary object extraction [\#303](https://github.com/archivesunleashed/aut/issues/303)
- PDF binary object extraction [\#302](https://github.com/archivesunleashed/aut/issues/302)
- Test aut with Apache Spark 2.4.0 [\#295](https://github.com/archivesunleashed/aut/issues/295)
- Replace hashing of unique ids with .zipWithUniqueId\(\) [\#243](https://github.com/archivesunleashed/aut/issues/243)
- Integration of neural network models for image analysis [\#240](https://github.com/archivesunleashed/aut/issues/240)
- More complete Twitter Ingestion [\#194](https://github.com/archivesunleashed/aut/issues/194)
- Image Search Functionality [\#165](https://github.com/archivesunleashed/aut/issues/165)
- feature request: log when loadArchives opens and closes warc files in a dir [\#156](https://github.com/archivesunleashed/aut/issues/156)

**Fixed bugs:**

- DataFrame commands throwing java.lang.NullPointerException on example data [\#320](https://github.com/archivesunleashed/aut/issues/320)
- Class issues when using aut-0.17.0-fatjar.jar [\#313](https://github.com/archivesunleashed/aut/issues/313)
- Image extraction does not scale with number of WARCs [\#298](https://github.com/archivesunleashed/aut/issues/298)
- ExtractDomain mistakenly checks source first then url [\#277](https://github.com/archivesunleashed/aut/issues/277)
- Improve ExtractDomain to Better Isolate Domains [\#269](https://github.com/archivesunleashed/aut/issues/269)

**Security fixes:**

- CVE-2017-7525 -- com.fasterxml.jackson.core:jackson-databind [\#279](https://github.com/archivesunleashed/aut/issues/279)

**Closed issues:**

- Inconsistency in ArchiveRecord.getContentBytes [\#334](https://github.com/archivesunleashed/aut/issues/334)
- Rationalize computeHash and ComputeMD5 [\#333](https://github.com/archivesunleashed/aut/issues/333)
- Test additional Java versions with TravisCI [\#324](https://github.com/archivesunleashed/aut/issues/324)
- Remove Twitter/tweet analysis [\#322](https://github.com/archivesunleashed/aut/issues/322)
- Trouble testing s3 connectivity [\#319](https://github.com/archivesunleashed/aut/issues/319)
- Depfu Error: No dependency files found [\#309](https://github.com/archivesunleashed/aut/issues/309)
- Strategy to deal with conflict between application and Spark distribution dependencies [\#308](https://github.com/archivesunleashed/aut/issues/308)
- SaveImageTest.scala should delete saved image file [\#299](https://github.com/archivesunleashed/aut/issues/299)
- Remove Deprecated ExtractGraph.scala file for next release. [\#291](https://github.com/archivesunleashed/aut/issues/291)
- DetectLanguage.scala: class LanguageIdentifier in package language is deprecated [\#286](https://github.com/archivesunleashed/aut/issues/286)
- Maven build warning during release [\#273](https://github.com/archivesunleashed/aut/issues/273)
- Improve DataFrameLoader.scala test coverage [\#265](https://github.com/archivesunleashed/aut/issues/265)
- Improve package.scala test coverage [\#263](https://github.com/archivesunleashed/aut/issues/263)
- Discussion: Idiom for loading DataFrames [\#231](https://github.com/archivesunleashed/aut/issues/231)
- DataFrame field names: open thread [\#229](https://github.com/archivesunleashed/aut/issues/229)
- DataFrame performance comparison: Scala vs. Python [\#215](https://github.com/archivesunleashed/aut/issues/215)
- TweetUtilsTest.scala doesn't test Spark, only underlying json4s library [\#206](https://github.com/archivesunleashed/aut/issues/206)
- feature request: ArchiveRecord.archiveFile [\#164](https://github.com/archivesunleashed/aut/issues/164)
- feature request: possibility to query about the progress [\#162](https://github.com/archivesunleashed/aut/issues/162)
- Update to Apache Tika 1.19.1; security vulnerabilities in 1.12 [\#131](https://github.com/archivesunleashed/aut/issues/131)
- Create tests for ExtractGraph.scala [\#49](https://github.com/archivesunleashed/aut/issues/49)
- Setup Victims [\#5](https://github.com/archivesunleashed/aut/issues/5)

**Merged pull requests:**

- Update LICENSE and license headers. [\#351](https://github.com/archivesunleashed/aut/pull/351) ([ruebot](https://github.com/ruebot))
- Add binary extraction DataFrames to PySpark. [\#350](https://github.com/archivesunleashed/aut/pull/350) ([ruebot](https://github.com/ruebot))
- Add method for determining binary file extension [\#349](https://github.com/archivesunleashed/aut/pull/349) ([jrwiebe](https://github.com/jrwiebe))
- Add keep and discard by http status. [\#347](https://github.com/archivesunleashed/aut/pull/347) ([ruebot](https://github.com/ruebot))
- Add office document binary extraction. [\#346](https://github.com/archivesunleashed/aut/pull/346) ([ruebot](https://github.com/ruebot))
- Use version of tika-parsers without a classifier [\#345](https://github.com/archivesunleashed/aut/pull/345) ([jrwiebe](https://github.com/jrwiebe))
- Use Tika's detected MIME type instead of ArchiveRecord getMimeType. [\#344](https://github.com/archivesunleashed/aut/pull/344) ([ruebot](https://github.com/ruebot))
- Add Audio & Video binary extraction [\#341](https://github.com/archivesunleashed/aut/pull/341) ([ruebot](https://github.com/ruebot))
- Extract PDF [\#340](https://github.com/archivesunleashed/aut/pull/340) ([jrwiebe](https://github.com/jrwiebe))
- More scalastyle work; addresses \#196. [\#339](https://github.com/archivesunleashed/aut/pull/339) ([ruebot](https://github.com/ruebot))
- Replace computeHash with ComputeMD5; resolves \#333. [\#338](https://github.com/archivesunleashed/aut/pull/338) ([ruebot](https://github.com/ruebot))
- Update Tika to 1.22; address security alerts. [\#337](https://github.com/archivesunleashed/aut/pull/337) ([ruebot](https://github.com/ruebot))
- Tests [\#336](https://github.com/archivesunleashed/aut/pull/336) ([ruebot](https://github.com/ruebot))
- Make ArchiveRecord.getContentBytes consistent, Resolve \#334 [\#335](https://github.com/archivesunleashed/aut/pull/335) ([ianmilligan1](https://github.com/ianmilligan1))
- Enable S3 access [\#332](https://github.com/archivesunleashed/aut/pull/332) ([jrwiebe](https://github.com/jrwiebe))
- Updates to pom following 0e701b271e04e60c6fa89f39299dae7142d700b8 [\#328](https://github.com/archivesunleashed/aut/pull/328) ([ruebot](https://github.com/ruebot))
- Move data frame fields names to snake\_case. [\#327](https://github.com/archivesunleashed/aut/pull/327) ([ruebot](https://github.com/ruebot))
- Python formatting, and gitignore additions. [\#326](https://github.com/archivesunleashed/aut/pull/326) ([ruebot](https://github.com/ruebot))
- Test Java 8 & 11, and remove OracleJDK; resolves \#324. [\#325](https://github.com/archivesunleashed/aut/pull/325) ([ruebot](https://github.com/ruebot))
- Remove Tweet utils. [\#323](https://github.com/archivesunleashed/aut/pull/323) ([ruebot](https://github.com/ruebot))
- Update to Spark 2.4.3 and update Tika to 1.20. [\#321](https://github.com/archivesunleashed/aut/pull/321) ([ruebot](https://github.com/ruebot))
- add image analysis w/ tensorflow [\#318](https://github.com/archivesunleashed/aut/pull/318) ([h324yang](https://github.com/h324yang))
- Makes ArchiveRecordImpl serializable [\#316](https://github.com/archivesunleashed/aut/pull/316) ([jrwiebe](https://github.com/jrwiebe))
- Resolve cobertura-maven-plugin class issue; resolves \#313. [\#314](https://github.com/archivesunleashed/aut/pull/314) ([ruebot](https://github.com/ruebot))
- Update spark-core\_2.11 to 2.3.1. [\#312](https://github.com/archivesunleashed/aut/pull/312) ([ruebot](https://github.com/ruebot))
- Log closing of ARC and WARC files, per \#156 [\#301](https://github.com/archivesunleashed/aut/pull/301) ([jrwiebe](https://github.com/jrwiebe))
- Delete saved image file; resolves \#299 [\#300](https://github.com/archivesunleashed/aut/pull/300) ([jrwiebe](https://github.com/jrwiebe))
- Remove Deprecated ExtractGraph app [\#293](https://github.com/archivesunleashed/aut/pull/293) ([greebie](https://github.com/greebie))
- Add .getHttpStatus and .getFilename to ArchiveRecordImpl class \#198 & \#164 [\#292](https://github.com/archivesunleashed/aut/pull/292) ([greebie](https://github.com/greebie))
- Update license headers for \#208. [\#290](https://github.com/archivesunleashed/aut/pull/290) ([ruebot](https://github.com/ruebot))
- Change Id generation for graphs from using hashes for urls to using .zipWithUniqueIds\(\) [\#289](https://github.com/archivesunleashed/aut/pull/289) ([greebie](https://github.com/greebie))
- CVE-2018-11771 update [\#288](https://github.com/archivesunleashed/aut/pull/288) ([ruebot](https://github.com/ruebot))
- CVE-2017-17485 update; follow-on to \#281. [\#287](https://github.com/archivesunleashed/aut/pull/287) ([ruebot](https://github.com/ruebot))
- Update Apache Tika - security vulnerabilities; resolves \#131. [\#285](https://github.com/archivesunleashed/aut/pull/285) ([ruebot](https://github.com/ruebot))
- \[skip travis\] Update README [\#284](https://github.com/archivesunleashed/aut/pull/284) ([ruebot](https://github.com/ruebot))
- Only trigger TravisCI on master. [\#283](https://github.com/archivesunleashed/aut/pull/283) ([ruebot](https://github.com/ruebot))
- Missed something for \#208. [\#282](https://github.com/archivesunleashed/aut/pull/282) ([ruebot](https://github.com/ruebot))
- CVE-2018-7489 fix. [\#281](https://github.com/archivesunleashed/aut/pull/281) ([ruebot](https://github.com/ruebot))
- Update jackson-databind version; resolves \#279. [\#280](https://github.com/archivesunleashed/aut/pull/280) ([ruebot](https://github.com/ruebot))
- Patch for \#277: Fix bug and unit test for ExtractDomain [\#278](https://github.com/archivesunleashed/aut/pull/278) ([borislin](https://github.com/borislin))
- Patch for \#269: Replace backslash with forward slash in URL [\#276](https://github.com/archivesunleashed/aut/pull/276) ([borislin](https://github.com/borislin))
- Clean-up pom.xml to remove plugin warnings; resolves \#273. [\#274](https://github.com/archivesunleashed/aut/pull/274) ([ruebot](https://github.com/ruebot))

## [aut-0.17.0](https://github.com/archivesunleashed/aut/tree/aut-0.17.0) (2018-10-04)

[Full Changelog](https://github.com/archivesunleashed/aut/compare/aut-0.16.0...aut-0.17.0)

**Implemented enhancements:**

- Add EscapeHTML Function for ExtractLinks [\#266](https://github.com/archivesunleashed/aut/issues/266)
- PySpark support [\#12](https://github.com/archivesunleashed/aut/issues/12)

**Fixed bugs:**

-  AUT exits/dies on java.util.zip.ZipException: too many length or distance symbols [\#271](https://github.com/archivesunleashed/aut/issues/271)
- AUT exits/dies on java.util.zip.ZipException: invalid distance too far back [\#246](https://github.com/archivesunleashed/aut/issues/246)
- Improve ExtractDomain Normalization [\#239](https://github.com/archivesunleashed/aut/issues/239)
- Twitter analysis is broken; see also: https://github.com/json4s/json4s/issues/496 [\#197](https://github.com/archivesunleashed/aut/issues/197)
- Prevent encoding errors in PySpark [\#122](https://github.com/archivesunleashed/aut/issues/122)

**Closed issues:**

- Cannot skip bad record while reading warc file [\#267](https://github.com/archivesunleashed/aut/issues/267)
- Why did Scalastyle not reject `null` values in TweetUtilTest [\#255](https://github.com/archivesunleashed/aut/issues/255)
- Create UDF to combine basic text filtering features [\#253](https://github.com/archivesunleashed/aut/issues/253)
- spark-shell --packages "io.archivesunleashed:aut:0.16.0" fails with not\_found dependencies [\#242](https://github.com/archivesunleashed/aut/issues/242)
- CommandLineAppRunner.scala produces output per WARC instead of combined result. [\#235](https://github.com/archivesunleashed/aut/issues/235)
- Extract images out of images DataFrame and store to disk [\#232](https://github.com/archivesunleashed/aut/issues/232)
- Before the next release, make sure docker-aut builds on master... or make sure --packages works [\#227](https://github.com/archivesunleashed/aut/issues/227)
- DataFrames for image analysis [\#220](https://github.com/archivesunleashed/aut/issues/220)
- The attempt to upgrade Spark version to 2.3.0 is not successful [\#218](https://github.com/archivesunleashed/aut/issues/218)
- Convert nulls to Option\(T\) [\#212](https://github.com/archivesunleashed/aut/issues/212)
- Bringing Scala DataFrames into PySpark [\#209](https://github.com/archivesunleashed/aut/issues/209)
- What is AUT? [\#208](https://github.com/archivesunleashed/aut/issues/208)
- Refactor ExtractGraph and assess value of GraphX for producing network graphs [\#203](https://github.com/archivesunleashed/aut/issues/203)
- Codify creation of standard derivatives into apps [\#195](https://github.com/archivesunleashed/aut/issues/195)
- TweetUtils - support fulltext [\#192](https://github.com/archivesunleashed/aut/issues/192)
- Combine UDFs into appropriate objects [\#187](https://github.com/archivesunleashed/aut/issues/187)
- Register Scala functions for use in Pyspark [\#148](https://github.com/archivesunleashed/aut/issues/148)
- PySpark performance bottlenecks: counting values [\#130](https://github.com/archivesunleashed/aut/issues/130)
- Redesign of PySpark DataFrame interface for filtering [\#120](https://github.com/archivesunleashed/aut/issues/120)
- Improve RecordLoader.scala test coverage [\#60](https://github.com/archivesunleashed/aut/issues/60)

**Merged pull requests:**

- Patch for \#246 & \#271: Fix exception error when processing corrupted ARC files [\#272](https://github.com/archivesunleashed/aut/pull/272) ([borislin](https://github.com/borislin))
- Update Bug report template. [\#268](https://github.com/archivesunleashed/aut/pull/268) ([ruebot](https://github.com/ruebot))
- ExtractBoilerpipeText to remove headers as well. \#253 [\#256](https://github.com/archivesunleashed/aut/pull/256) ([greebie](https://github.com/greebie))
- Add additional tweet fields to TweetUtils; partially address \#194. [\#254](https://github.com/archivesunleashed/aut/pull/254) ([ruebot](https://github.com/ruebot))
- Add support for full\_text in tweets; resolve \#192. [\#252](https://github.com/archivesunleashed/aut/pull/252) ([ruebot](https://github.com/ruebot))
- Get rid of 'filesystem-root relative reference' warning. [\#251](https://github.com/archivesunleashed/aut/pull/251) ([ruebot](https://github.com/ruebot))
- Remove stray characters from example commands. [\#250](https://github.com/archivesunleashed/aut/pull/250) ([ruebot](https://github.com/ruebot))
- Deal with final scalastyle assessments: Issue 212 [\#249](https://github.com/archivesunleashed/aut/pull/249) ([greebie](https://github.com/greebie))
- Address main scalastyle errors - \#196 [\#248](https://github.com/archivesunleashed/aut/pull/248) ([greebie](https://github.com/greebie))
- Add ExtractGraphX including algorithms for PageRank and Components. Issue 203 [\#245](https://github.com/archivesunleashed/aut/pull/245) ([greebie](https://github.com/greebie))
- Travis build fixes [\#244](https://github.com/archivesunleashed/aut/pull/244) ([ruebot](https://github.com/ruebot))
- Data frame implementation of extractors. Also added cmd arguments to resolve \#235 [\#236](https://github.com/archivesunleashed/aut/pull/236) ([TitusAn](https://github.com/TitusAn))
- Save images from dataframe to disk [\#234](https://github.com/archivesunleashed/aut/pull/234) ([jwli229](https://github.com/jwli229))
- Add missing dependencies in; addresses \#227. [\#233](https://github.com/archivesunleashed/aut/pull/233) ([ruebot](https://github.com/ruebot))
- Code cleanup: ArchiveRecord + impl moved into same Scala file [\#230](https://github.com/archivesunleashed/aut/pull/230) ([lintool](https://github.com/lintool))
- Add Extract Image Details API [\#226](https://github.com/archivesunleashed/aut/pull/226) ([jwli229](https://github.com/jwli229))
- Implement DomainFrequency, DomainGraph and PlainText extractor that can be run from command line [\#225](https://github.com/archivesunleashed/aut/pull/225) ([TitusAn](https://github.com/TitusAn))
- Remove duplicate call of keepValidPages [\#224](https://github.com/archivesunleashed/aut/pull/224) ([jwli229](https://github.com/jwli229))
- Extract Image Links DF API + Test [\#221](https://github.com/archivesunleashed/aut/pull/221) ([jwli229](https://github.com/jwli229))
- Update Apache Spark to 2.3.0; resolves \#218 [\#219](https://github.com/archivesunleashed/aut/pull/219) ([ruebot](https://github.com/ruebot))
- Resolve https://github.com/archivesunleashed/docker-aut/issues/17 [\#217](https://github.com/archivesunleashed/aut/pull/217) ([ruebot](https://github.com/ruebot))
- Create issue templates [\#216](https://github.com/archivesunleashed/aut/pull/216) ([ruebot](https://github.com/ruebot))
- Exposing Scala DataFrames in PySpark [\#214](https://github.com/archivesunleashed/aut/pull/214) ([lintool](https://github.com/lintool))
- Update project description; resolves \#208. [\#211](https://github.com/archivesunleashed/aut/pull/211) ([ruebot](https://github.com/ruebot))
- Initial DataFrames merge [\#210](https://github.com/archivesunleashed/aut/pull/210) ([lintool](https://github.com/lintool))
- Add more instructions on how to use things to the README. [\#207](https://github.com/archivesunleashed/aut/pull/207) ([ruebot](https://github.com/ruebot))

## [aut-0.16.0](https://github.com/archivesunleashed/aut/tree/aut-0.16.0) (2018-04-26)

[Full Changelog](https://github.com/archivesunleashed/aut/compare/aut-0.15.0...aut-0.16.0)

**Implemented enhancements:**

- Revisit approach to .keepValidPages\(\) [\#177](https://github.com/archivesunleashed/aut/issues/177)

**Closed issues:**

- keepValidPages incorrectly filters out pages with mime-type text/html followed by charset [\#199](https://github.com/archivesunleashed/aut/issues/199)

**Merged pull requests:**

- Unbork'ing tweet analysis \(Fixes Issue 197\) - take2 [\#205](https://github.com/archivesunleashed/aut/pull/205) ([lintool](https://github.com/lintool))
- Update README.md [\#202](https://github.com/archivesunleashed/aut/pull/202) ([lintool](https://github.com/lintool))
- Code reformatting [\#201](https://github.com/archivesunleashed/aut/pull/201) ([lintool](https://github.com/lintool))
- fix \#199: mime-type was incorrectly parsed from content-type when cha… [\#200](https://github.com/archivesunleashed/aut/pull/200) ([dportabella](https://github.com/dportabella))

## [aut-0.15.0](https://github.com/archivesunleashed/aut/tree/aut-0.15.0) (2018-04-11)

[Full Changelog](https://github.com/archivesunleashed/aut/compare/aut-0.14.0...aut-0.15.0)

**Implemented enhancements:**

- Clean-up scaladoc comments [\#184](https://github.com/archivesunleashed/aut/issues/184)

**Closed issues:**

- Rename package io.archivesunleashed.io [\#188](https://github.com/archivesunleashed/aut/issues/188)
- Major Refactoring: RecordRDD [\#180](https://github.com/archivesunleashed/aut/issues/180)
- Major refactoring: matchbox cleanup [\#179](https://github.com/archivesunleashed/aut/issues/179)
- Major refactoring: io.archivesunleashed.spark -\> io.archivesunleashed [\#178](https://github.com/archivesunleashed/aut/issues/178)

**Merged pull requests:**

- Improve and clean-up Scaladocs; resolves \#184 [\#193](https://github.com/archivesunleashed/aut/pull/193) ([ruebot](https://github.com/ruebot))
- Major refactoring of package structure [\#189](https://github.com/archivesunleashed/aut/pull/189) ([lintool](https://github.com/lintool))
- make ArchiveRecord a trait [\#186](https://github.com/archivesunleashed/aut/pull/186) ([helgeho](https://github.com/helgeho))

## [aut-0.14.0](https://github.com/archivesunleashed/aut/tree/aut-0.14.0) (2018-03-20)

[Full Changelog](https://github.com/archivesunleashed/aut/compare/aut-0.13.0...aut-0.14.0)

**Closed issues:**

- Incorporate Scala UDFs into Auto-documentation [\#176](https://github.com/archivesunleashed/aut/issues/176)

**Merged pull requests:**

- Resolve \#176; setup scaladocs. [\#183](https://github.com/archivesunleashed/aut/pull/183) ([ruebot](https://github.com/ruebot))
- Revert "make ArchiveRecord a trait \(\#175\)" [\#181](https://github.com/archivesunleashed/aut/pull/181) ([ruebot](https://github.com/ruebot))

## [aut-0.13.0](https://github.com/archivesunleashed/aut/tree/aut-0.13.0) (2018-03-07)

[Full Changelog](https://github.com/archivesunleashed/aut/compare/aut-0.12.2...aut-0.13.0)

**Merged pull requests:**

- make ArchiveRecord a trait [\#175](https://github.com/archivesunleashed/aut/pull/175) ([helgeho](https://github.com/helgeho))

## [aut-0.12.2](https://github.com/archivesunleashed/aut/tree/aut-0.12.2) (2018-02-28)

[Full Changelog](https://github.com/archivesunleashed/aut/compare/aut-0.12.1...aut-0.12.2)

**Implemented enhancements:**

- ArchiveRecord.warcFile [\#171](https://github.com/archivesunleashed/aut/issues/171)
- Better approach to ids in WriteGraphML & WriteGEXF [\#168](https://github.com/archivesunleashed/aut/issues/168)
- Build pre-filtered networks [\#109](https://github.com/archivesunleashed/aut/issues/109)
- KeepDate UDF should support date range [\#108](https://github.com/archivesunleashed/aut/issues/108)
- Changing keepDate to allow multiple dates, would close \#108 [\#161](https://github.com/archivesunleashed/aut/pull/161) ([ianmilligan1](https://github.com/ianmilligan1))

**Fixed bugs:**

- Broken GEXF Files Due to \< and \> characters in node id fields [\#172](https://github.com/archivesunleashed/aut/issues/172)
- There is insufficient memory for the Java Runtime Environment to continue [\#159](https://github.com/archivesunleashed/aut/issues/159)
- AUT Fails on Extracting Text from WARCs [\#158](https://github.com/archivesunleashed/aut/issues/158)

**Closed issues:**

- RecordLoader.loadArchives fails with nested dirs [\#169](https://github.com/archivesunleashed/aut/issues/169)
- Unparseable date error [\#163](https://github.com/archivesunleashed/aut/issues/163)
- remove angle brackets from ArchiveRecord.getUrl [\#157](https://github.com/archivesunleashed/aut/issues/157)
- Benchmarking Scala vs Python [\#121](https://github.com/archivesunleashed/aut/issues/121)
- Improve WacArcInputFormat.java test coverage [\#80](https://github.com/archivesunleashed/aut/issues/80)
- Improve WacWarcInputFormat.java test coverage [\#78](https://github.com/archivesunleashed/aut/issues/78)
- Improve WarcRecordWritable.java test coverage [\#77](https://github.com/archivesunleashed/aut/issues/77)
- Improve ArcRecordWritable.java test coverage [\#75](https://github.com/archivesunleashed/aut/issues/75)
- Improve ArcRecord.scala test coverage [\#69](https://github.com/archivesunleashed/aut/issues/69)
- Improve RemoveHttpHeader.scala test coverage [\#57](https://github.com/archivesunleashed/aut/issues/57)
- Investigate Jupyter notebooks on Altiscale [\#37](https://github.com/archivesunleashed/aut/issues/37)

**Merged pull requests:**

- Gexf Fixes & StringUtil Functions \#172 [\#173](https://github.com/archivesunleashed/aut/pull/173) ([greebie](https://github.com/greebie))
- Graphml Improvements [\#170](https://github.com/archivesunleashed/aut/pull/170) ([greebie](https://github.com/greebie))
- Graphml [\#167](https://github.com/archivesunleashed/aut/pull/167) ([greebie](https://github.com/greebie))
- Fix bug -- label type should be "string" not "label". [\#166](https://github.com/archivesunleashed/aut/pull/166) ([greebie](https://github.com/greebie))
- Add link to docker-aut. [\#160](https://github.com/archivesunleashed/aut/pull/160) ([ruebot](https://github.com/ruebot))
- Remove references to Arc and WarcRecord libraries \(covered by Archive… [\#146](https://github.com/archivesunleashed/aut/pull/146) ([greebie](https://github.com/greebie))

## [aut-0.12.1](https://github.com/archivesunleashed/aut/tree/aut-0.12.1) (2017-12-15)

[Full Changelog](https://github.com/archivesunleashed/aut/compare/aut-0.12.0...aut-0.12.1)

**Fixed bugs:**

- ARC Handling Bug in 0.12.0 when Extracting Links [\#154](https://github.com/archivesunleashed/aut/issues/154)
- Changes jsoup version in pom.xml \(\#154\) [\#155](https://github.com/archivesunleashed/aut/pull/155) ([ianmilligan1](https://github.com/ianmilligan1))

## [aut-0.12.0](https://github.com/archivesunleashed/aut/tree/aut-0.12.0) (2017-12-11)

[Full Changelog](https://github.com/archivesunleashed/aut/compare/aut-0.11.0...aut-0.12.0)

**Implemented enhancements:**

- Add GraphML UDF [\#142](https://github.com/archivesunleashed/aut/issues/142)
- GEXF Output [\#103](https://github.com/archivesunleashed/aut/issues/103)
- Native notebook support [\#14](https://github.com/archivesunleashed/aut/issues/14)
- DataFrames support [\#13](https://github.com/archivesunleashed/aut/issues/13)

**Fixed bugs:**

- NullPointerException error during build [\#124](https://github.com/archivesunleashed/aut/issues/124)
- Resolves Issue \#128: Uses new getOrigins method [\#136](https://github.com/archivesunleashed/aut/pull/136) ([ianmilligan1](https://github.com/ianmilligan1))

**Closed issues:**

- Create tests for WriteGEXF.scala [\#138](https://github.com/archivesunleashed/aut/issues/138)
- ERROR ArcRecordUtils - Read 1224 bytes but expected 1300 bytes [\#128](https://github.com/archivesunleashed/aut/issues/128)
- WarcRecordUtils.java uses or overrides a deprecated API [\#127](https://github.com/archivesunleashed/aut/issues/127)
- class LanguageIdentifier in package language is deprecated [\#126](https://github.com/archivesunleashed/aut/issues/126)
- multiple versions of scala [\#125](https://github.com/archivesunleashed/aut/issues/125)
- ExtractLinks running slowly [\#123](https://github.com/archivesunleashed/aut/issues/123)
- com.cloudera.cdh:hadoop-ant:pom:0.20.2-cdh3u4 -- errors [\#118](https://github.com/archivesunleashed/aut/issues/118)

**Merged pull requests:**

- Too many JUNITs [\#152](https://github.com/archivesunleashed/aut/pull/152) ([ruebot](https://github.com/ruebot))
- Add more packages and exclusions for \#113 [\#150](https://github.com/archivesunleashed/aut/pull/150) ([ruebot](https://github.com/ruebot))
- - Add tests for RecordLoader [\#149](https://github.com/archivesunleashed/aut/pull/149) ([greebie](https://github.com/greebie))
- Tuple Formatter Test Improvement [\#145](https://github.com/archivesunleashed/aut/pull/145) ([greebie](https://github.com/greebie))
- Check to replace partial coverage for ExtractDate. [\#144](https://github.com/archivesunleashed/aut/pull/144) ([greebie](https://github.com/greebie))
- Add GraphML UDF [\#143](https://github.com/archivesunleashed/aut/pull/143) ([greebie](https://github.com/greebie))
- Remove stackTrace output on caught error. [\#141](https://github.com/archivesunleashed/aut/pull/141) ([greebie](https://github.com/greebie))
- Add deprecation warnings to outmoded Arc and Warc formats. [\#140](https://github.com/archivesunleashed/aut/pull/140) ([greebie](https://github.com/greebie))
- Tests for WriteGEXF Issue \#138 [\#139](https://github.com/archivesunleashed/aut/pull/139) ([greebie](https://github.com/greebie))
- Include script to write to GEXF. \(\#103\) [\#137](https://github.com/archivesunleashed/aut/pull/137) ([greebie](https://github.com/greebie))
- Use correct import for WARCConstants; Resolves \#127. [\#133](https://github.com/archivesunleashed/aut/pull/133) ([ruebot](https://github.com/ruebot))
- Downgrade Tika to 1.12. Resolves \#126. [\#132](https://github.com/archivesunleashed/aut/pull/132) ([ruebot](https://github.com/ruebot))
- Pin everything to Scala 2.11.8; Resolves \#125. [\#129](https://github.com/archivesunleashed/aut/pull/129) ([ruebot](https://github.com/ruebot))
- Exclude old version of Hadoop. Resolves \#118. [\#119](https://github.com/archivesunleashed/aut/pull/119) ([ruebot](https://github.com/ruebot))

## [aut-0.11.0](https://github.com/archivesunleashed/aut/tree/aut-0.11.0) (2017-11-22)

[Full Changelog](https://github.com/archivesunleashed/aut/compare/aut-0.10.0...aut-0.11.0)

**Implemented enhancements:**

- GetCrawlYear to accompany GetCrawlMonth [\#104](https://github.com/archivesunleashed/aut/issues/104)
- Refactor RecordLoader classes [\#102](https://github.com/archivesunleashed/aut/issues/102)
- Adding getCrawlYear in ArchiveRecords, resolves \#104 [\#105](https://github.com/archivesunleashed/aut/pull/105) ([ianmilligan1](https://github.com/ianmilligan1))

**Closed issues:**

- spark-shell --packages "io.archivesunleashed:aut:0.10.0"` fails with not\_found dependencies [\#113](https://github.com/archivesunleashed/aut/issues/113)
- update the version of the dependencies not available on the central maven repository [\#111](https://github.com/archivesunleashed/aut/issues/111)
- Bake keepValidPages\(\) into RecordLoader [\#101](https://github.com/archivesunleashed/aut/issues/101)
- Create tests for JsonUtil.scala [\#66](https://github.com/archivesunleashed/aut/issues/66)
- Improve ExtractDomain.scala test coverage [\#63](https://github.com/archivesunleashed/aut/issues/63)
- Improve ExtractImageLinks.scala test coverage [\#62](https://github.com/archivesunleashed/aut/issues/62)
- Improve ExtractLinks.scala test coverage [\#61](https://github.com/archivesunleashed/aut/issues/61)
- Improve StringUtils.scala test coverage [\#58](https://github.com/archivesunleashed/aut/issues/58)
- Improve RemoveHTML.scala test coverage [\#56](https://github.com/archivesunleashed/aut/issues/56)
- Create tests for TweetUtils.scala [\#54](https://github.com/archivesunleashed/aut/issues/54)
- Create tests for ExtractTextFromPDFs.scala [\#51](https://github.com/archivesunleashed/aut/issues/51)
- Create tests for ExtractPopularImages.scala [\#50](https://github.com/archivesunleashed/aut/issues/50)
- Create tests for ExtractBoilerpipeText.scala [\#47](https://github.com/archivesunleashed/aut/issues/47)
- Create tests for ComputeMD5.scala [\#46](https://github.com/archivesunleashed/aut/issues/46)
- Create tests for ComputeImageSize.scala [\#45](https://github.com/archivesunleashed/aut/issues/45)

**Merged pull requests:**

- This needs to hold steady. [\#117](https://github.com/archivesunleashed/aut/pull/117) ([ruebot](https://github.com/ruebot))
- Update all dependencies, and add missing dependencies to resolve \#113.  [\#116](https://github.com/archivesunleashed/aut/pull/116) ([ruebot](https://github.com/ruebot))
- Updated documentation links; link to project page [\#115](https://github.com/archivesunleashed/aut/pull/115) ([ianmilligan1](https://github.com/ianmilligan1))
- Remove pom.xml cruft; Partially resolves \#111. [\#112](https://github.com/archivesunleashed/aut/pull/112) ([ruebot](https://github.com/ruebot))
- Created Code of Conduct file [\#110](https://github.com/archivesunleashed/aut/pull/110) ([SamFritz](https://github.com/SamFritz))
- Refactor ArchiveRecord classes; addresses \#101 and \#102 [\#107](https://github.com/archivesunleashed/aut/pull/107) ([MapleOx](https://github.com/MapleOx))
- Improve coverage for issue-67 \(RecordRDD.scala\) [\#99](https://github.com/archivesunleashed/aut/pull/99) ([greebie](https://github.com/greebie))
- Minor fix to improve coverage. \#55 [\#98](https://github.com/archivesunleashed/aut/pull/98) ([greebie](https://github.com/greebie))
- Test ExtractTextFromPDFs.  \#51 [\#97](https://github.com/archivesunleashed/aut/pull/97) ([greebie](https://github.com/greebie))
- Remove example scripts. Resolves \#95, \#70, \#71, \#72. [\#96](https://github.com/archivesunleashed/aut/pull/96) ([ruebot](https://github.com/ruebot))
- Setup cobertura better so we have local html reports. [\#94](https://github.com/archivesunleashed/aut/pull/94) ([ruebot](https://github.com/ruebot))
- Create unit tests for Issue \#50 \(ExtractPopularImages\) [\#93](https://github.com/archivesunleashed/aut/pull/93) ([greebie](https://github.com/greebie))
- Add ExtractGraphTest; lint fixes on RemoveHttpHeaderTest. [\#92](https://github.com/archivesunleashed/aut/pull/92) ([greebie](https://github.com/greebie))
- Improve coverage for Issue \#80 [\#91](https://github.com/archivesunleashed/aut/pull/91) ([greebie](https://github.com/greebie))
- Improve coverage for TweetUtils [\#90](https://github.com/archivesunleashed/aut/pull/90) ([greebie](https://github.com/greebie))
- Increase coverage for ComputeImageSize. \#45 [\#89](https://github.com/archivesunleashed/aut/pull/89) ([greebie](https://github.com/greebie))
- Complete coverage for \#66 [\#88](https://github.com/archivesunleashed/aut/pull/88) ([greebie](https://github.com/greebie))
- Improve Test Coverage for \#55, \#56, \#57, \#58, \#59, \#60, \#61, \#62, \#63, \#64 & \#66 [\#87](https://github.com/archivesunleashed/aut/pull/87) ([greebie](https://github.com/greebie))
- Add PR template. [\#85](https://github.com/archivesunleashed/aut/pull/85) ([ruebot](https://github.com/ruebot))
- First round of unit tests [\#84](https://github.com/archivesunleashed/aut/pull/84) ([greebie](https://github.com/greebie))
- Use Scala 2.11.8; Align further with Altiscale. [\#83](https://github.com/archivesunleashed/aut/pull/83) ([ruebot](https://github.com/ruebot))

## [aut-0.10.0](https://github.com/archivesunleashed/aut/tree/aut-0.10.0) (2017-10-02)

[Full Changelog](https://github.com/archivesunleashed/aut/compare/aut-0.9.0...aut-0.10.0)

**Fixed bugs:**

- NER breaks for WARC files? [\#41](https://github.com/archivesunleashed/aut/issues/41)

**Closed issues:**

- Do we need pythonconverters/ArcRecordConverter.scala? If so, tests. If not, delete it. [\#65](https://github.com/archivesunleashed/aut/issues/65)
- Upgrade to Spark 2 on Altiscale [\#43](https://github.com/archivesunleashed/aut/issues/43)
- Investigate our test coverage according to codecov.io [\#36](https://github.com/archivesunleashed/aut/issues/36)
- Update Scala version [\#35](https://github.com/archivesunleashed/aut/issues/35)
- Update to use Java 8 [\#32](https://github.com/archivesunleashed/aut/issues/32)
- Migrate warcbase-resources to aut-resources [\#30](https://github.com/archivesunleashed/aut/issues/30)
- mvn site-deploy -DskipTests is still failing [\#27](https://github.com/archivesunleashed/aut/issues/27)
- Retarget Hadoop [\#9](https://github.com/archivesunleashed/aut/issues/9)

**Merged pull requests:**

- Update to Apache Spark 2.1.1; resolves \#43. [\#82](https://github.com/archivesunleashed/aut/pull/82) ([ruebot](https://github.com/ruebot))
- Remove unused file; resolves \#65. [\#81](https://github.com/archivesunleashed/aut/pull/81) ([ruebot](https://github.com/ruebot))
- Removed inaccurate information from README.md [\#44](https://github.com/archivesunleashed/aut/pull/44) ([lintool](https://github.com/lintool))
- Add WARC support for ExtractEntities; Resolve \#41. [\#42](https://github.com/archivesunleashed/aut/pull/42) ([ruebot](https://github.com/ruebot))
- Add OpenJDK8 and remove OracleJDK7 so we can use trusty. [\#39](https://github.com/archivesunleashed/aut/pull/39) ([ruebot](https://github.com/ruebot))
- Link to aut-docs in README [\#38](https://github.com/archivesunleashed/aut/pull/38) ([ianmilligan1](https://github.com/ianmilligan1))
- Resolve \#32; Update to Java 8 [\#34](https://github.com/archivesunleashed/aut/pull/34) ([ruebot](https://github.com/ruebot))
- Resolve \#9; Update Hadoop and Spark versions. [\#33](https://github.com/archivesunleashed/aut/pull/33) ([ruebot](https://github.com/ruebot))
- Added reference to the releases [\#31](https://github.com/archivesunleashed/aut/pull/31) ([ianmilligan1](https://github.com/ianmilligan1))
- Resolve \#27 - Deploy javadocs to gh-pages [\#29](https://github.com/archivesunleashed/aut/pull/29) ([ruebot](https://github.com/ruebot))
- Add Maven Central badge. [\#28](https://github.com/archivesunleashed/aut/pull/28) ([ruebot](https://github.com/ruebot))

## [aut-0.9.0](https://github.com/archivesunleashed/aut/tree/aut-0.9.0) (2017-08-24)

[Full Changelog](https://github.com/archivesunleashed/aut/compare/2d61f4fc1c886cc4c03ea31cc060021d5b1f9616...aut-0.9.0)

**Closed issues:**

- More work needs to be done on the pom.xml to get us to a release.  [\#25](https://github.com/archivesunleashed/aut/issues/25)
- Is src/main/java/io/archivesunleashed/demo required? [\#17](https://github.com/archivesunleashed/aut/issues/17)
- Visualization Repo \(aut-viz\) [\#16](https://github.com/archivesunleashed/aut/issues/16)
- Remove `src/main/python` [\#10](https://github.com/archivesunleashed/aut/issues/10)
- What do we do with all the documentation at docs.warcbase.org? [\#8](https://github.com/archivesunleashed/aut/issues/8)
- Setup to publish javadocs on ghpages [\#7](https://github.com/archivesunleashed/aut/issues/7)
- Get a project setup on sonatype [\#6](https://github.com/archivesunleashed/aut/issues/6)
- Setup license headers and mycila [\#4](https://github.com/archivesunleashed/aut/issues/4)
- Setup checkstyle [\#3](https://github.com/archivesunleashed/aut/issues/3)
- Setup codecov.io [\#1](https://github.com/archivesunleashed/aut/issues/1)

**Merged pull requests:**

- Resolve \#25 update pom.xml to do a release [\#26](https://github.com/archivesunleashed/aut/pull/26) ([ruebot](https://github.com/ruebot))
- Resolve \#7 [\#24](https://github.com/archivesunleashed/aut/pull/24) ([ruebot](https://github.com/ruebot))
- Add Slack integration for TravisCI [\#21](https://github.com/archivesunleashed/aut/pull/21) ([ruebot](https://github.com/ruebot))
- Setup mycila plugin, and normalize all license headers; Resolves \#4. [\#20](https://github.com/archivesunleashed/aut/pull/20) ([ruebot](https://github.com/ruebot))
- Add checkstyle plugin, and remove demo; resolves \#3 \#17. [\#19](https://github.com/archivesunleashed/aut/pull/19) ([ruebot](https://github.com/ruebot))
- Updating README [\#15](https://github.com/archivesunleashed/aut/pull/15) ([ianmilligan1](https://github.com/ianmilligan1))
- Remove dir; resolves \#10 [\#11](https://github.com/archivesunleashed/aut/pull/11) ([ruebot](https://github.com/ruebot))
- Setup codecov.io integration; resolves \#1 [\#2](https://github.com/archivesunleashed/aut/pull/2) ([ruebot](https://github.com/ruebot))



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
