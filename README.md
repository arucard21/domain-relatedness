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
In order to use the reproduce the evaluation, you need to download the input data and extract it to a directory named `input`.
Each subdirectory should represent a dataset and contain gzipped TSV files.
The evaluation can then be reproduced by running the main method in the `Experiments` class.
Due the high memory requirements, you may need to configure the JVM with these arguments `-Xms2g -Xmx15g`.

This should result in the following three output directories:
* output-variations
* output-evaluation-movie
* output-evaluation-fifa-players

The `output-variations` directory contains the results for different variations on how to generate the dataset domain terms.
This can best be inspected through the `results_linked.ods` spreadsheet.
This reads the necessary result files and provides an overview and several graphs for them.

The `output-evaluation-movie` and `output-evaluation-fifa-players` directories contain the results for the two domains that were evaluated in the thesis. 
The results can be inspected in three files that are provided in each directory.
`datasets_focused_on_domain.csv` provides the names and corresponding similarity scores of the datasets that were considered to focus on the domain.
`all_similarity_scores.csv` provides the names and corresponding similarity scores of all datasets that were provided as input.
This file also includes some more information, like the size of the dataset, the number of dataset domain terms, and the number of matched terms.
`result_validation.json` contains several statistics calculated during the evaluation, most notable the accuracy, precision, and recall of the technique.
