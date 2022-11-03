package com.github.arucard21.evaluation;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.github.arucard21.dataset_discovery.DatasetDiscovery;
import com.github.arucard21.dataset_discovery.objects.DatasetSimilarity;
import com.github.arucard21.dataset_discovery.objects.MatchingResult;
import com.github.arucard21.dataset_domain_terms.DatasetDomainTerms;
import com.github.arucard21.evaluation.objects.ResultValidation;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import org.opendata.curation.d4.D4Config;

public class Experiments {
	public static final Path INPUT_DIR = Paths.get("input");
	public static final Path VARIATIONS_OUTPUT_DIR= Paths.get("output-variations");
	public static final Path VARIATIONS_OUTPUT_DOMAIN_REPRESENTATION_DIR = VARIATIONS_OUTPUT_DIR.resolve("domain-representation");
	public static final Path VARIATIONS_OUTPUT_DOMAIN_SIMILARITY_DIR = VARIATIONS_OUTPUT_DIR.resolve("domain-similarity");
	public static final Path VARIATIONS_OUTPUT_DOMAIN_REPRESENTATION_PRECISION_OPTIMIZED = VARIATIONS_OUTPUT_DOMAIN_REPRESENTATION_DIR.resolve("precision");
	public static final Path VARIATIONS_OUTPUT_DOMAIN_REPRESENTATION_PRECISION_OPTIMIZED_TF = VARIATIONS_OUTPUT_DOMAIN_REPRESENTATION_DIR.resolve("precision-tf");
	public static final Path VARIATIONS_OUTPUT_DOMAIN_REPRESENTATION_ACCURACY_OPTIMIZED = VARIATIONS_OUTPUT_DOMAIN_REPRESENTATION_DIR.resolve("accuracy");
	public static final Path VARIATIONS_OUTPUT_DOMAIN_REPRESENTATION_ALL_TERMS = VARIATIONS_OUTPUT_DOMAIN_REPRESENTATION_DIR.resolve("all-terms");
	public static final Path VARIATIONS_OUTPUT_DOMAIN_REPRESENTATION_NO_EXPAND = VARIATIONS_OUTPUT_DOMAIN_REPRESENTATION_DIR.resolve("no-expand");

	public static final Path EVALUATION_MOVIE_OUTPUT_DIR= Paths.get("output-evaluation-movie");
	public static final Path EVALUATION_FIFA_PLAYERS_OUTPUT_DIR= Paths.get("output-evaluation-fifa-players");
	public static final String EVALUATION_DOMAIN_REPRESENTATION_DIR = "domain-representation";
	public static final String DATASET_RESULT_VALIDATION_FILE_NAME = "result_validation.json";

	public static final String JACCARD_INDEX = D4Config.EQSIM_JI;
	public static final String TERM_FREQUENCY_BASED_JACCARD = D4Config.EQSIM_TFICF;
	public static final String ROBUSTIFIER_LIBERAL = D4Config.ROBUST_LIBERAL;
	public static final String ROBUSTIFIER_IGNORE_LAST = D4Config.ROBUST_IGNORELAST;
	public static final String TRIMMER_CONSERVATIVE = D4Config.TRIMMER_CONSERVATIVE;
	public static final String TRIMMER_LIBERAL = D4Config.TRIMMER_LIBERAL;
	public static final String TRIMMER_CENTRIST = D4Config.TRIMMER_CENTRIST;
	public static final OpenOption[] CREATE_AND_APPEND = new OpenOption[]{StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.APPEND};

	public static final Map<String, Boolean> MOVIE_DOMAIN_GROUND_TRUTH = Map.ofEntries(
			Map.entry("imdb", true),
			Map.entry("tmdb", true),
			Map.entry("indian", true),
			Map.entry("movies", true),
			Map.entry("netflix", true),
			Map.entry("tmdb-350k", true),
			Map.entry("rt", false),
			Map.entry("steam", false),
			Map.entry("datasets.data-cityofnewyork-us.education", false),
			Map.entry("datasets.data-cityofnewyork-us.finance", false),
			Map.entry("datasets.data-cityofnewyork-us.services", false),
			Map.entry("Energy consumption of the Netherlands", false),
			Map.entry("FIFA 22 complete player dataset", false),
			Map.entry("Fifa Players Ratings", false),
			Map.entry("FIFA Players & Stats", false),
			Map.entry("Football Events", false),
			Map.entry("FIFA23 OFFICIAL DATASET", false),
			Map.entry("Instacart Market Basket Analysis", false),
			Map.entry("NIPS Papers", false),
			Map.entry("Uber Pickups in New York City", false),
			Map.entry("US Baby Names", false));
	public static final Map<String, Boolean> FIFA_PLAYERS_DOMAIN_GROUND_TRUTH = Map.ofEntries(
			Map.entry("imdb", false),
			Map.entry("tmdb", false),
			Map.entry("indian", false),
			Map.entry("movies", false),
			Map.entry("netflix", false),
			Map.entry("tmdb-350k", false),
			Map.entry("rt", false),
			Map.entry("steam", false),
			Map.entry("datasets.data-cityofnewyork-us.education", false),
			Map.entry("datasets.data-cityofnewyork-us.finance", false),
			Map.entry("datasets.data-cityofnewyork-us.services", false),
			Map.entry("Energy consumption of the Netherlands", false),
			Map.entry("FIFA 22 complete player dataset", true),
			Map.entry("FIFA Players & Stats", true),
			Map.entry("Fifa Players Ratings", true),
			Map.entry("Football Events", false),
			Map.entry("FIFA23 OFFICIAL DATASET", true),
			Map.entry("Instacart Market Basket Analysis", false),
			Map.entry("NIPS Papers", false),
			Map.entry("Uber Pickups in New York City", false),
			Map.entry("US Baby Names", false));
	public static final Path MOVIE_DOMAIN_INPUT_DATASETS_COMBINED = INPUT_DIR.resolve("domain-data-movies");
	public static final Path FIFA_PLAYERS_DOMAIN_INPUT_DATASETS_COMBINED = INPUT_DIR.resolve("domain-data-fifa-players");
	public static final List<Path> VARIATIONS_INPUT_DATASETS = List.of(
			INPUT_DIR.resolve("indian"),
			INPUT_DIR.resolve("movies"),
			INPUT_DIR.resolve("netflix"),
			INPUT_DIR.resolve("tmdb-350k"),
			INPUT_DIR.resolve("rt"),
			INPUT_DIR.resolve("steam"),
			INPUT_DIR.resolve("datasets.data-cityofnewyork-us.education"),
			INPUT_DIR.resolve("datasets.data-cityofnewyork-us.finance"),
			INPUT_DIR.resolve("datasets.data-cityofnewyork-us.services"));
	public static final List<Path> EVALUATION_INPUT_DATASETS_RANDOM_ORDER = List.of(
			INPUT_DIR.resolve("steam"),
			INPUT_DIR.resolve("Energy consumption of the Netherlands"),
			INPUT_DIR.resolve("netflix"),
			INPUT_DIR.resolve("FIFA23 OFFICIAL DATASET"),
			INPUT_DIR.resolve("datasets.data-cityofnewyork-us.education"),
			INPUT_DIR.resolve("datasets.data-cityofnewyork-us.services"),
			INPUT_DIR.resolve("Instacart Market Basket Analysis"),
			INPUT_DIR.resolve("imdb"),
			INPUT_DIR.resolve("tmdb"),
			INPUT_DIR.resolve("Fifa Players Ratings"),
			INPUT_DIR.resolve("Football Events"),
			INPUT_DIR.resolve("indian"),
			INPUT_DIR.resolve("FIFA Players & Stats"),
			INPUT_DIR.resolve("US Baby Names"),
			INPUT_DIR.resolve("Uber Pickups in New York City"),
			INPUT_DIR.resolve("datasets.data-cityofnewyork-us.finance"),
			INPUT_DIR.resolve("FIFA 22 complete player dataset"),
			INPUT_DIR.resolve("movies"),
			INPUT_DIR.resolve("NIPS Papers"),
			INPUT_DIR.resolve("tmdb-350k"),
			INPUT_DIR.resolve("rt")
			);

	public static void main(String[] args) throws IOException {
		runVariationsExperiments();
		runEvaluationExperiments();
    }

	private static void runVariationsExperiments() throws IOException {
		Path precisionDatasetDomainPath = new DatasetDomainTerms(MOVIE_DOMAIN_INPUT_DATASETS_COMBINED, VARIATIONS_OUTPUT_DOMAIN_REPRESENTATION_PRECISION_OPTIMIZED)
				.similarityAlgorithm(JACCARD_INDEX)
				.pruningStrategy(TRIMMER_CONSERVATIVE)
				.columnExpansionDisabled(false)
				.allTermsFromColumnDomainsIncluded(false)
				.generate();
		Path termFreqDatasetDomainPath = new DatasetDomainTerms(MOVIE_DOMAIN_INPUT_DATASETS_COMBINED, VARIATIONS_OUTPUT_DOMAIN_REPRESENTATION_PRECISION_OPTIMIZED_TF)
				.similarityAlgorithm(TERM_FREQUENCY_BASED_JACCARD)
				.pruningStrategy(TRIMMER_CONSERVATIVE)
				.columnExpansionDisabled(false)
				.allTermsFromColumnDomainsIncluded(false)
				.generate();
		Path accuracyDatasetDomainPath = new DatasetDomainTerms(MOVIE_DOMAIN_INPUT_DATASETS_COMBINED, VARIATIONS_OUTPUT_DOMAIN_REPRESENTATION_ACCURACY_OPTIMIZED)
				.similarityAlgorithm(JACCARD_INDEX)
				.pruningStrategy(TRIMMER_CENTRIST)
				.columnExpansionDisabled(false)
				.allTermsFromColumnDomainsIncluded(false)
				.generate();
		Path noExpandDatasetDomainPath = new DatasetDomainTerms(MOVIE_DOMAIN_INPUT_DATASETS_COMBINED, VARIATIONS_OUTPUT_DOMAIN_REPRESENTATION_NO_EXPAND)
				.similarityAlgorithm(JACCARD_INDEX)
				.pruningStrategy(TRIMMER_CONSERVATIVE)
				.columnExpansionDisabled(true)
				.allTermsFromColumnDomainsIncluded(false)
				.generate();
		Path allTermsDatasetDomainPath = new DatasetDomainTerms(MOVIE_DOMAIN_INPUT_DATASETS_COMBINED, VARIATIONS_OUTPUT_DOMAIN_REPRESENTATION_ALL_TERMS)
				.similarityAlgorithm(JACCARD_INDEX)
				.pruningStrategy(TRIMMER_CONSERVATIVE)
				.columnExpansionDisabled(false)
				.allTermsFromColumnDomainsIncluded(true)
				.generate();
    	for(Path inputDataset: VARIATIONS_INPUT_DATASETS) {
    		calculateSimilarityToTargetDatasetForAllVariations(
    				inputDataset,
    				VARIATIONS_OUTPUT_DOMAIN_SIMILARITY_DIR,
					precisionDatasetDomainPath,
					termFreqDatasetDomainPath,
					accuracyDatasetDomainPath,
					noExpandDatasetDomainPath,
					allTermsDatasetDomainPath);
    	}
	}

	private static void runEvaluationExperiments() throws IOException {
		evaluateAccuracyForMovieDomain();
		evaluateAccuracyForFifaPlayersDomain();
	}

	private static void evaluateAccuracyForMovieDomain() throws IOException {
		evaluateAccuracyForDatasetDomain(MOVIE_DOMAIN_INPUT_DATASETS_COMBINED, MOVIE_DOMAIN_GROUND_TRUTH, EVALUATION_MOVIE_OUTPUT_DIR);
	}

	private static void evaluateAccuracyForFifaPlayersDomain() throws IOException {
		evaluateAccuracyForDatasetDomain(FIFA_PLAYERS_DOMAIN_INPUT_DATASETS_COMBINED, FIFA_PLAYERS_DOMAIN_GROUND_TRUTH, EVALUATION_FIFA_PLAYERS_OUTPUT_DIR);
	}

	private static void evaluateAccuracyForDatasetDomain(Path domainDatasetsCombinedPath, Map<String, Boolean> domainTruth, Path outputPath) throws IOException {
		Path domainRepresentationOutputPath = outputPath.resolve(EVALUATION_DOMAIN_REPRESENTATION_DIR);
		Path evaluationMovieDomainPath = new DatasetDomainTerms(domainDatasetsCombinedPath, domainRepresentationOutputPath)
				.similarityAlgorithm(JACCARD_INDEX)
				.pruningStrategy(TRIMMER_CONSERVATIVE)
				.columnExpansionDisabled(false)
				.allTermsFromColumnDomainsIncluded(false)
				.generate();
		ResultValidation resultValidationMovieDomain = evaluateAccuracy(evaluationMovieDomainPath, domainTruth, EVALUATION_INPUT_DATASETS_RANDOM_ORDER, outputPath);
		System.out.println("For the domain from: " + domainDatasetsCombinedPath.getFileName().toString());
		System.out.println("Accuracy: "+resultValidationMovieDomain.getAccuracy());
		System.out.println("Precision: "+resultValidationMovieDomain.getPrecision());
		System.out.println("Recall: "+resultValidationMovieDomain.getRecall());
		System.out.println("Lowest domain similarity score: "+resultValidationMovieDomain.getLowestDomainSimilarityScore());
		System.out.println("Highest non-domain similarity score: "+resultValidationMovieDomain.getHighestNonDomainSimilarityScore());
		System.out.println("Domain range size: "+resultValidationMovieDomain.getDomainRangeSize());
		System.out.println("Non-domain range size: "+resultValidationMovieDomain.getNonDomainRangeSize());
		System.out.println("");
	}


	private static ResultValidation evaluateAccuracy(Path datasetDomainPath, Map<String, Boolean> domainTruth, List<Path> inputDatasets, Path outputPath) throws IOException {
		DatasetDiscovery datasetDiscoveryForDomain = new DatasetDiscovery(datasetDomainPath, outputPath, inputDatasets);
		Set<String> similarDatasets = datasetDiscoveryForDomain.discoverDatasetsFocusedOnSameDomain();
		List<DatasetSimilarity> datasetSimilarityScores = datasetDiscoveryForDomain.getDatasetSimilarityScores();
		ResultValidation resultValidationOriginalOrder = validateResults(domainTruth, datasetSimilarityScores, similarDatasets, outputPath);
    	return resultValidationOriginalOrder;
	}

	private static ResultValidation validateResults(Map<String, Boolean> domainTruth, List<DatasetSimilarity> similarityScores, Set<String> similarDatasets, Path outputPath) throws IOException {
		Path resultValidationPath = outputPath == null ? null : outputPath.resolve(DATASET_RESULT_VALIDATION_FILE_NAME);
		if(resultValidationPath != null && resultValidationPath.toFile().exists()) {
			try(Reader reader = Files.newBufferedReader(resultValidationPath)){
		    	return new Gson().fromJson(reader, ResultValidation.class);
	    	}
		}

		int truePositives = 0;
		int falsePositives = 0;
		int trueNegatives = 0;
		int falseNegatives = 0;
		double highestDomainScore = -1;
		double lowestDomainScore = Double.MAX_VALUE;
		double highestNonDomainScore = -1;
		double lowestNonDomainScore = Double.MAX_VALUE;
		for(DatasetSimilarity dataset : similarityScores) {
			String datasetName = dataset.getDatasetName();
			double datasetSimilarityScore = dataset.getSimilarityScore();
			boolean isSimilarTruth = domainTruth.get(datasetName);
			boolean isSimilar = similarDatasets.contains(datasetName);
			if(isSimilar) {
				if(datasetSimilarityScore > highestDomainScore) {
					highestDomainScore = datasetSimilarityScore;
				}
				if(datasetSimilarityScore < lowestDomainScore) {
					lowestDomainScore = datasetSimilarityScore;
				}
			}
			else {
				if(datasetSimilarityScore > highestNonDomainScore) {
					highestNonDomainScore = datasetSimilarityScore;
				}
				if(datasetSimilarityScore < lowestNonDomainScore) {
					lowestNonDomainScore = datasetSimilarityScore;
				}
			}

			if(isSimilarTruth && isSimilar) {
				truePositives++;
			}
			else if(isSimilarTruth && !isSimilar) {
				falseNegatives++;
			}
			else if(!isSimilarTruth && isSimilar) {
				falsePositives++;
			}
			else if(!isSimilarTruth && !isSimilar) {
				trueNegatives++;
			}

		}
		ResultValidation resultValidation = new ResultValidation(truePositives, falsePositives, trueNegatives, falseNegatives, lowestDomainScore, highestDomainScore, lowestNonDomainScore, highestNonDomainScore);
		if(resultValidationPath != null) {
			try(Writer writer = Files.newBufferedWriter(resultValidationPath)){
		    	new GsonBuilder().setPrettyPrinting().create().toJson(resultValidation, writer);
	    	}
		}
		return resultValidation;
	}

	private static boolean outputExists(Path outputPath) {
		if(outputPath != null && outputPath.toFile().exists()) {
			System.out.println(outputPath.toString() + " already exists. Skipping this step.");
			return true;
		}
		else {
			return false;
		}
	}

	private static Path calculateSimilarityToTargetDatasetForAllVariations(Path datasetFolderName, Path outputDirPath, Path... domainRepresentationVariationPaths) throws IOException {
		Path outputForDataset = outputDirPath.resolve(datasetFolderName.getFileName());
		Path similarityScoresCsvPath = outputForDataset.resolve(DatasetDiscovery.DATASET_SIMILARITY_SCORES_CSV_NAME);
		if(!outputExists(similarityScoresCsvPath)) {
			for(Path domainRepresentationVariationPath: domainRepresentationVariationPaths) {
				new DatasetDiscovery(domainRepresentationVariationPath, outputDirPath, List.of(datasetFolderName)).discoverDatasetsFocusedOnSameDomain();

				Path matchingResultPath = outputForDataset.resolve(String.format("matching_result_%s_%s.json",
						outputForDataset.getFileName().toString(),
						domainRepresentationVariationPath.getParent().getFileName().toString()));
				try(Reader reader = Files.newBufferedReader(matchingResultPath)){
					MatchingResult matchingResult = new Gson().fromJson(reader, MatchingResult.class);
					writeSimilarityScoreToCsvFile(
							domainRepresentationVariationPath.getParent().getFileName().toString(),
							matchingResult,
							outputForDataset.resolve(DatasetDiscovery.DATASET_SIMILARITY_SCORES_CSV_NAME));
				}
			}
		}
		return similarityScoresCsvPath;
	}

	private static void writeSimilarityScoreToCsvFile(String name, MatchingResult matchingResult, Path similarityScoresCsvPath) throws IOException {
		if(!similarityScoresCsvPath.toFile().exists()) {
			Files.writeString(similarityScoresCsvPath, "domain_representation_type,domain_representation_size,dataset_size,matched,overlap_coefficient\n", CREATE_AND_APPEND);
		}
		String csvOutput = String.format(
				"%s,%d,%d,%d,%f\n",
				name,
				matchingResult.getDomainRepresentationSize(),
				matchingResult.getDatasetSize(),
				matchingResult.getMatched(),
				matchingResult.getSimilarityScore());
		Files.writeString(similarityScoresCsvPath, csvOutput, CREATE_AND_APPEND);
	}
}
