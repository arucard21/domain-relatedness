# Domain Similarity

This contains the code implementing and evaluating the techniques proposed in my thesis:  
[_Domain-focused dataset discovery for tabular datasets, using easily-available information about the domain_](http://resolver.tudelft.nl/uuid:012f7697-16be-4c93-965b-b4f8ebd391b3)

## Dataset domain terms
The dataset domain terms can be generated using the `DatasetDomainTerms` class.
You need to construct an instance of this class and provide it with two paths. 
The first is the directory containing the tables of all domain-representative datasets, as (optionally gzipped) TSV files.
The second is the directory where the results should be output, as well as intermediate outcomes.

You can then set the configuration options on this instance and use the `generate()` command to generate the dataset domain terms.
This returns the path to the directory containing the dataset domain terms.

```java
Path datasetDomainTermsPath = new DatasetDomainTerms(domainRepresentativeDatasetsDirectory, outputDirectory)
				.similarityAlgorithm(D4Config.EQSIM_JI)
				.pruningStrategy(D4Config.TRIMMER_CONSERVATIVE)
				.columnExpansionDisabled(false)
				.allTermsFromColumnDomainsIncluded(false)
				.generate();
```

## Dataset discovery
Dataset discovery can be done in a similar way.
First, create an instance of the `DatasetDiscovery` class.
The first argument for its constructor is the path to the directory containing the dataset domain terms.
This should be the same as the path returned from technique for generate those dataset domain terms.
The second argument is the path where the result should be output, as well as intermediate outcomes.
The third argument is a list of paths, one for each directory that contains a dataset that should be used as input for this dataset discovery technique.
The only configuration here is whether matched terms should be logged.
This is a resource-intensive task so it should only be enabled when neccessary.

You can then use the `discoverDatasetsFocusedOnSameDomain()` method to run the dataset discovery technique.
This returns the set of datasets that are considered to focus on the domain represented by the dataset domain terms that were provided.
After this method is used, you can also retrieve the similarity scores that were calculated for each datasets.

```java
DatasetDiscovery datasetDiscoveryForDomain = new DatasetDiscovery(datasetDomainTermsDirectory, outputDirectory, inputDatasets);
Set<String> domainFocusedDatasets = datasetDiscoveryForDomain.discoverDatasetsFocusedOnSameDomain();
List<DatasetSimilarity> datasetSimilarityScores = datasetDiscoveryForDomain.getDatasetSimilarityScores();
```
## Reproducing the evaluation
### Preparing input datasets
In order to reproduce the evaluation, you need to download the input datasets.
They should be extracted to a directory named `input` at the root of this repository.
Each subdirectory in the `input` directory represents a dataset and should contain gzipped TSV files. 

_Note that this includes a dataset for each domain included in the evaluation._
_Each of them contains the data from all domain-representative datasets for their domain._
_This is provided as convenience since those domain-representative datasets are also available separately._

The input datasets are [attached to the release of this repository](https://github.com/arucard21/domain-similarity/releases/tag/1.0) as separate `.tar` files.
They can be extracted with this command:
```shell
tar -xf filename.tar
```
More information about the source of each dataset is provided [here](https://arucard21.github.io/domain-similarity/).

The `Services` dataset (`datasets.data-cityofnewyork-us.services`) had to be split up due to file size constraints.
It can be combined again with this command:
```shell
cat datasets.data-cityofnewyork-us.services.tar0* > datasets.data-cityofnewyork-us.services.tar
```
Or you can extract it directly with this command:
```shell
cat datasets.data-cityofnewyork-us.services.tar0* | tar -xf -
```
### Running the evaluation
Before running the evaluation, you need to build the D4 code as this it is a dependency of this implementation.
Go into the `domain-discovery-d4` subdirectory and build it using Maven with the following commands:
```shell
cd domain-discovery-d4
mvn clean install
```
You can now go back to the root directory and reproduce the evaluation using Gradle with the following commands:
```shell
cd ..
./gradlew run
```
Due the high memory requirements, Gradle configures the JVM with the arguments `-Xms2g -Xmx15g` to increase the allowed memory usage.
This can be changed in `build.gradle`.

The evaluation will now be run and may take several hours to complete.
It is implemented such that if it stops partway through, the next time it will skip what it has already created intermediate outcomes for.
So it will resume from where it stopped when you run it again.
This may require removing the last intermediate outcome it was working on if this was only partially written to disk.

### Inspecting the results of the evaluation
This should result in the following three output directories:
* output-variations
* output-evaluation-movie
* output-evaluation-fifa-players

The `output-variations` directory contains the results for different variations on how to generate the dataset domain terms.
This can best be inspected through the `results_linked.ods` spreadsheet.
This reads the necessary result files and provides an overview and several graphs for them.

The `output-evaluation-movie` and `output-evaluation-fifa-players` directories contain the results for the two domains that were evaluated in the thesis. 
The results can be inspected in three files that are provided in each directory.
* `datasets_focused_on_domain.csv` provides the names and corresponding similarity scores of the datasets that were considered to focus on the domain.
* `all_similarity_scores.csv` provides the names and corresponding similarity scores of all datasets that were provided as input.
This file also includes the size of the dataset, the number of dataset domain terms, and the number of matched terms.
* `result_validation.json` contains several statistics calculated during the evaluation, including the accuracy, precision, and recall for that domain.
