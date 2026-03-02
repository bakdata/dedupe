# Changelog

All notable changes to this project will be documented in this file.

## [2.1.2](https://github.com/bakdata/dedupe/tree/2.1.2) - 2026-03-02
### What's changed

* Migrate azure pipeline to GH-action by @yannick-roeder in [#27](https://github.com/bakdata/dedupe/pull/27)

* Upgrade to Gradle 8.4 by @philipp94831 in [#28](https://github.com/bakdata/dedupe/pull/28)

* Upgrade to Gradle 9.3 by @philipp94831 in [#29](https://github.com/bakdata/dedupe/pull/29)

* Configure Dependabot by @philipp94831 in [#30](https://github.com/bakdata/dedupe/pull/30)

* Bump org.apache.commons:commons-text from 1.11.0 to 1.15.0 by @dependabot[bot] in [#34](https://github.com/bakdata/dedupe/pull/34)

* Bump org.apache.commons:commons-csv from 1.10.0 to 1.14.1 by @dependabot[bot] in [#35](https://github.com/bakdata/dedupe/pull/35)

* Bump commons-codec:commons-codec from 1.16.0 to 1.21.0 by @dependabot[bot] in [#32](https://github.com/bakdata/dedupe/pull/32)

* Bump com.google.guava:guava from 33.0.0-jre to 33.5.0-jre by @dependabot[bot] in [#33](https://github.com/bakdata/dedupe/pull/33)


### New Contributors
* @dependabot[bot] made their first contribution in [#33](https://github.com/bakdata/dedupe/pull/33)
* @yannick-roeder made their first contribution in [#27](https://github.com/bakdata/dedupe/pull/27)

**Full Changelog**: https://github.com/bakdata/dedupe/compare/2.1.1...2.1.2

## [2.1.1](https://github.com/bakdata/dedupe/tree/2.1.1) - 2024-01-29
### What's changed

* Update dependencies by @philipp94831 in [#26](https://github.com/bakdata/dedupe/pull/26)


**Full Changelog**: https://github.com/bakdata/dedupe/compare/2.1.0...2.1.1

## [2.1.0](https://github.com/bakdata/dedupe/tree/2.1.0) - 2021-09-28
### What's changed

* Make cluster id generator only use ids by @philipp94831 in [#24](https://github.com/bakdata/dedupe/pull/24)


**Full Changelog**: https://github.com/bakdata/dedupe/compare/2.0.3...2.1.0

## [2.0.3](https://github.com/bakdata/dedupe/tree/2.0.3) - 2019-11-06
### What's changed

* Handle clusters with more than 128 elements by @SvenLehmann in [#23](https://github.com/bakdata/dedupe/pull/23)


**Full Changelog**: https://github.com/bakdata/dedupe/compare/2.0.2...2.0.3

## [2.0.2](https://github.com/bakdata/dedupe/tree/2.0.2) - 2019-10-07
### What's changed

* Fix right candidate selection for large cluster refinement in [#20](https://github.com/bakdata/dedupe/pull/20)

* Fix heuristic cluster refinement by @SvenLehmann in [#21](https://github.com/bakdata/dedupe/pull/21)

* Add RefineCluster interface

* Revert "Add RefineCluster interface"

* Add RefineCluster interface by @SvenLehmann in [#22](https://github.com/bakdata/dedupe/pull/22)


### New Contributors
* @ made their first contribution

**Full Changelog**: https://github.com/bakdata/dedupe/compare/2.0.1...2.0.2

## [2.0.1](https://github.com/bakdata/dedupe/tree/2.0.1) - 2019-09-11
### What's changed

* Fixed coverage report for multi module setup by @AHeise

* Fix infinite recursion in SimilarityMeasure by @SvenLehmann in [#19](https://github.com/bakdata/dedupe/pull/19)


### New Contributors
* @SvenLehmann made their first contribution in [#19](https://github.com/bakdata/dedupe/pull/19)

**Full Changelog**: https://github.com/bakdata/dedupe/compare/2.0.0...2.0.1

## [2.0.0](https://github.com/bakdata/dedupe/tree/2.0.0) - 2019-03-21
### What's changed

* General cleanup by @AHeise in [#7](https://github.com/bakdata/dedupe/pull/7)

* Moving from Travis to Azure pipelines by @AHeise in [#8](https://github.com/bakdata/dedupe/pull/8)

* Added azure build badge and added maven/gradle snippets by @AHeise

* Preparing 2.0.0 release by @AHeise

* Refactored API and added javadocs by @AHeise in [#10](https://github.com/bakdata/dedupe/pull/10)

* Refactor common by @AHeise in [#16](https://github.com/bakdata/dedupe/pull/16)

* Added javadoc to readme and revised readme by @AHeise


**Full Changelog**: https://github.com/bakdata/dedupe/compare/1.1.0...2.0.0

## [1.1.0](https://github.com/bakdata/dedupe/tree/1.1.0) - 2019-02-12
### What's changed

* Initial commit by @AHeise

* Restructure modules by @philipp94831

* Moving conflict resolutions completely to common by @AHeise

* Adding id to cluster and extending clustering implementation to generate such ids by @AHeise

* Added ID parameter to cluster by @AHeise

* Added readme by @AHeise

* Made cluster id comparable by @AHeise

* Cleanup by @AHeise

* Adding sonar and travis config + code cleanup by @AHeise

* Add license 1 (#1) by @AHeise

* Setting up travis and sonar (#2) by @AHeise

* Adding nexus deployment and releasing through travis by @AHeise

* Using bakdata OSS plugins and switched to Kotlin DSL by @AHeise

* Bumped bakdata plugin version to 1.0.1 by @AHeise

* General cleanup with bakdata code style. by @AHeise

* Applied bakdata inspection by @AHeise

* Fully use OnlineDuplicateDetection to avoid code duplication by @AHeise

* Adding descriptions for sonatype releases. by @AHeise

* Setting up github release and changelog generation by @AHeise


### New Contributors
* @bakdata-bot made their first contribution
* @AHeise made their first contribution
* @philipp94831 made their first contribution

**Full Changelog**: https://github.com/bakdata/dedupe/compare/1.0.1...1.1.0

<!-- generated by git-cliff -->
