[![Build Status](https://dev.azure.com/bakdata/public/_apis/build/status/bakdata.dedupe?branchName=master)](https://dev.azure.com/bakdata/public/_build/latest?definitionId=4&branchName=master)
[![Sonarcloud status](https://sonarcloud.io/api/project_badges/measure?project=com.bakdata.dedupe%3Adedupe&metric=alert_status)](https://sonarcloud.io/dashboard?id=com.bakdata.dedupe%3Adedupe)
[![Code coverage](https://sonarcloud.io/api/project_badges/measure?project=com.bakdata.dedupe%3Adedupe&metric=coverage)](https://sonarcloud.io/dashboard?id=com.bakdata.dedupe%3Adedupe)
[![Maven](https://img.shields.io/maven-central/v/com.bakdata.dedupe/core.svg)](https://search.maven.org/search?q=g:com.bakdata.dedupe&core=gav)

Java DSL for (online) deduplication
===================================

Deduplication is a notorious configuration-heavy process with many competing algorithms and very domain-dependent details. 
This project aims to provide a concise Java DSL that allows developers to focus on these details without having to switch between languages and frameworks.

In particular, this project provides the following:
- A set of interfaces and base classes to model typical stages and (intermediate) outputs of a deduplication process.
- A type-safe configuration DSL that relies on builder patterns to provide an easy and intuitive way to plug the different parts together.
- A pure java implementation such that each part can be extended or fully customized with arbitrary complex user code.
- Implementations for common algorithms for each stage (to be expanded).
- Wrapper for or reimplementation of common similarity measures.
- A focus on online algorithms that continuously deduplicate a stream of input records.
- Examples for different domains (to be expanded).

## Getting Started

You can add dedupe via Maven Central.

#### Gradle
```gradle
compile group: 'com.bakdata.dedupe', name: 'common', version: '1.1.0'
```

#### Maven
```xml
<dependency>
    <groupId>com.bakdata.dedupe</groupId>
    <artifactId>common</artifactId>
    <version>1.1.0</version>
</dependency>
```

For other build tools or versions, refer to the [latest version in MvnRepository](https://mvnrepository.com/artifact/com.bakdata.dedupe/common/latest).

# Using the framework #

For a reference, please refer to the [full javadoc](https://bakdata.github.io/dedupe/javadoc/). 

In this section, we cover the different configuration stages. Ultimately, we want to receive a *deduplication* instance where we can feed in the records and receive the results. In this section, we configure an online, pair-based deduplication. The runnable code can be found in [examples](examples).

A pair-base deduplication consists of a [*duplicate detection*](#Duplicate-detection) that finds duplicate clusters and a [*fusion*](#Fusion) that provides a consisted representation of the found clusters.
```java
// create deduplication, parts are explained later
OnlineDeduplication<Person> deduplication = 
        FusingOnlineDuplicateDetection.<Long, Person>builder()
                .duplicateDetection(new PersonDuplicateDetection())
                .fusion(new PersonFusion())
                .build();

// apply it to a list of customers
List<Person> customers = ...;
for(Person customer: customers) {
    final Person fusedPerson = deduplication.deduplicate(customer);    
    // store fused person
}
```

## Duplicate detection ##

The pair-base duplicate detection in turn has a [*candidate selection*](#Candidate-selection) that chooses promising pairs to limit search space, a [*classifier*](#Candidate-classification) that labels pairs as duplicate or non-duplicate, and [*clustering*](#Clustering) that consolidates all pairs into plausible clusters.

```java
OnlineDuplicateDetection<Long, Person> duplicateDetection = 
        OnlinePairBasedDuplicateDetection.<Long, Person>builder()
                .classifier(new PersonClassifier())
                .candidateSelection(new PersonCandidateSelection())
                .clustering(new PersonClustering())
                .build();
```

## Candidate selection ##
The different parts are configured in the following. First, we define the candidate selection for a person.

```java
// configure candidate selection with 3 passes (= 3 sorting keys)
OnlineCandidateSelection<Person> candidateSelection = OnlineSortedNeighborhoodMethod.<Person>builder()
    .defaultWindowSize(WINDOW_SIZE)
    .sortingKey(new SortingKey<>("First name+Last name",
            person -> CompositeValue.of(normalizeName(person.getFirstName()), normalizeName(person.getLastName()))))
    .sortingKey(new SortingKey<>("Last name+First name",
            person -> CompositeValue.of(normalizeName(person.getLastName()), normalizeName(person.getFirstName()))))
    .sortingKey(new SortingKey<>("Bday+Last name",
            person -> CompositeValue.of(person.getBirthDate(), normalizeName(person.getLastName()))))
    .build();

private static String normalizeName(String value) {
    // split umlauts into canonicals
    return java.text.Normalizer.normalize(value.toLowerCase(), java.text.Normalizer.Form.NFD)
            // remove everything in braces
            .replaceAll("\\(.*?\\)", "")
            // remove all non-alphanumericals
            .replaceAll("[^\\p{Alnum}]", "");
}
```

The chosen sorted neighborhood uses sorting keys to sort the input and compare all records within a given window. In this case, we perform 3 passes with different sorting keys. Each pass uses the same window size but they can also be configured individually.

The sorting keys can be of arbitrary, comparable data type. The framework provides *CompositeValue* when the sorting key consists of several parts (which is recommended to resolve ties in the first part of the key). The normalizeName function is a custom UDF specific for this domain.

## Candidate classification ##

The output of the candidate selection is a list of candidate pairs, which is fed into the candidate classificationResult.

```java
import static com.bakdata.dedupe.similarity.CommonSimilarityMeasures.*;
Classifier<Person> personClassifier = RuleBasedClassifier.<Person>builder()
    .negativeRule("Different social security number", inequality().of(Person::getSSN))
    .positiveRule("Default", CommonSimilarityMeasures.<Person>weightedAverage()
        .add(10, Person::getSSN, equality())
        .add(2, Person::getFirstName, max(levenshtein().cutoff(.5f), jaroWinkler()))
        .add(2, Person::getLastName, max(equality().of(beiderMorse()), jaroWinkler()))
        .build()
        .scaleWithThreshold(.9f))
    .build();
```

The classificationResult DSL heavy relies on static factory functions in *CommonSimilarityMeasures* and can be easily extended by custom base similarity measures.

The used rule-based classificationResult applies a list of rules until a rule triggers and thus determines the classificationResult.

In this case, a negative rule first checks if the two persons of the candidate pair have a different social security number (SSN) if present. 

The next rule performs a weighted average of 3 different feature similarities. If SSN is given, it is highly weighted and almost suffices for a positive or negative classificationResult. Additionally, first and last name contribute to the classificationResult.

## Clustering ##

To consolidate a list of duplicate pairs into a consistent cluster, an additional clustering needs to be performed. Often times, only transitive closure is applied, which is also available in this framework. However, for high precision use cases, transitive closure is not enough.

```java
RefineCluster<Long, Person> refineCluster = RefineCluster.<Long, Person>builder()
        .classifier(new PersonClassifier())
        .clusterIdGenerator(ClusterIdGenerators.longGenerator())
        .build();

Clustering<Long, Person> refinedTransitiveClosure = RefinedTransitiveClosure.<Long, Person, String>builder()
        .refineCluster(this.refineCluster)
        .idExtractor(Person::getId)
        .build();

@Delegate
Clustering<Long, Person> clustering = ConsistentClustering.<Long, Person, String>builder()
        .clustering(this.refinedTransitiveClosure)
        .idExtractor(Person::getId)
        .build();
```

Here, we configure a *refining transitive closure* strategy that in particular checks for explicit negative classifications inside the transitive closure such that duplicate clusters are split into more plausible subclusters.

Finally, the wrapping *consistent clustering* ensure that IDs remain stable in the long run of an online deduplication.

## Fusion ##

Ultimately, we want to receive a duplicate-free dataset. Hence, we need to configure the fusion.

```java
ConflictResolution<Person, Person> personMerge = ConflictResolutions.merge(Person::new)
    .field(Person::getId, Person::setId).with(min())
    .field(Person::getFirstName, Person::setFirstName).with(longest()).then(vote())
    .field(Person::getLastName, Person::setLastName).correspondingToPrevious()
    .field(Person::getSSN, Person::setSSN).with(assumeEqualValue())
    .build();
Fusion<Person> personFusion = ConflictResolutionFusion.<Person>builder()
    .sourceExtractor(Person::getSource)
    .lastModifiedExtractor(Person::getLastModified)
    .rootResolution(personMerge)
    .build();
```

The shown conflict resolution approach, reconciles each field in a recursive manner by choosing the respective value according to a list of conflict resolution functions. Each function reduces the list of candidate values until hopefully only one value remains. If not, the fusion fails and has to be manually resolved.

The conflict resolution may also use source preferences, source confidence scores, and timestamps to find the best value.

# Maintenance #

## Structure ##

The project consists of three parts
- **core**: Provides the basic interfaces and data structures of the various deduplication stages.
- **common**: Implements common similarities and algorithms.
- **examples**: Showcases different domains.

## Building & executing tests ##

This project requires Java 11. To build and execute all tests, please run
```bash
./gradlew build
```

For IDEs, import the project (or the build.gradle.ktsgit a file) as a gradle project. The project makes heavily use of [Lombok](http://projectlombok.org/), so make sure you have the appropriate IDE plugin and enabled annotation preprocessing. 
