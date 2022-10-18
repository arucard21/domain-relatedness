package domain.similarity;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonIOException;
import com.google.gson.JsonSyntaxException;

import org.opendata.core.constraint.Threshold;
import org.opendata.curation.d4.D4;
import org.opendata.curation.d4.D4Config;
import org.opendata.curation.d4.telemetry.TelemetryPrinter;
import org.opendata.db.term.Term;
import org.opendata.db.term.TermConsumer;
import org.opendata.db.term.TermIndexReader;

public class DomainSimilarity {
	public static boolean LOG_MATCHED_TERMS = true;
	public static boolean LOG_DURATION = false;

	public static final Path INPUT_DIR = Paths.get("input");
	public static final Path VARIATIONS_OUTPUT_DIR= Paths.get("output-variations");
	public static final Path VARIATIONS_OUTPUT_DOMAIN_REPRESENTATION_DIR = VARIATIONS_OUTPUT_DIR.resolve("domain-representation");
	public static final Path VARIATIONS_OUTPUT_DOMAIN_SIMILARITY_DIR = VARIATIONS_OUTPUT_DIR.resolve("domain-similarity");
	public static final Path VARIATIONS_OUTPUT_DOMAIN_REPRESENTATION_PRECISION_OPTIMIZED = VARIATIONS_OUTPUT_DOMAIN_REPRESENTATION_DIR.resolve("precision");
	public static final Path VARIATIONS_OUTPUT_DOMAIN_REPRESENTATION_PRECISION_OPTIMIZED_TF = VARIATIONS_OUTPUT_DOMAIN_REPRESENTATION_DIR.resolve("precision-tf");
	public static final Path VARIATIONS_OUTPUT_DOMAIN_REPRESENTATION_ACCURACY_OPTIMIZED = VARIATIONS_OUTPUT_DOMAIN_REPRESENTATION_DIR.resolve("accuracy");
	public static final Path VARIATIONS_OUTPUT_DOMAIN_REPRESENTATION_ALL_TERMS = VARIATIONS_OUTPUT_DOMAIN_REPRESENTATION_DIR.resolve("all-terms");
	public static final Path VARIATIONS_OUTPUT_DOMAIN_REPRESENTATION_NO_EXPAND = VARIATIONS_OUTPUT_DOMAIN_REPRESENTATION_DIR.resolve("no-expand");

	public static final String EVALUATION_SELECTED_VARIANT = "precision";
	public static final Path EVALUATION_MOVIE_OUTPUT_DIR= Paths.get("output-evaluation-movie");
	public static final Path EVALUATION_FIFA_PLAYERS_OUTPUT_DIR= Paths.get("output-evaluation-fifa-players");
	public static final String EVALUATION_DOMAIN_REPRESENTATION_DIR = "domain-representation";
	public static final String EVALUATION_TASK_DURATIONS_CSV = "task_durations_evaluation.csv";
	public static final String EVALUATION_OUTPUT_ALL_SIMILARITY_SCORES_CSV = "all_similarity_scores.csv";
	public static final String EVALUATION_OUTPUT_DATASETS_FOCUSED_ON_DOMAIN = "datasets_focused_on_domain.csv";
	public static final String EVALUATION_THRESHOLD_VALUES = "threshold_values.csv";

	public static final String COLUMNS_DIR_NAME = "columns";
	public static final String COLUMNS_METADATA_FILE_NAME = "columns.tsv";
	public static final String TERM_INDEX_FILE_NAME = "term-index.txt.gz";
	public static final String EQUIVALENCE_CLASSES_FILE_NAME = "compressed-term-index.txt.gz";
	public static final String SIGNATURES_FILE_NAME = "signatures.txt.gz";
	public static final String EXPANDED_COLUMNS_FILE_NAME = "expanded-columns.txt.gz";
	public static final String LOCAL_DOMAINS_FILE_NAME = "local-domains.txt.gz";
	public static final String COLUMN_DOMAINS_INTERNAL_FILE_NAME = "strong-domains.txt.gz";
	public static final String COLUMN_DOMAINS_DIR_NAME = "domains";
	public static final String DATASET_DOMAIN_DIR_NAME = "dataset-domain";
	public static final String DATASET_SIMILARITY_SCORES_CSV_NAME = "similarity_scores.csv";
	public static final String DATASET_RESULT_VALIDATION_FILE_NAME = "result_validation.json";
	public static final String DATASET_RESULT_VALIDATION_ALL_SUBSETS_FILE_NAME = "result_validation_all_subsets.csv";

	public static final String JACCARD_INDEX = D4Config.EQSIM_JI;
	public static final String TERM_FREQUENCY_BASED_JACCARD = D4Config.EQSIM_TFICF;
	public static final String ROBUSTIFIER_LIBERAL = D4Config.ROBUST_LIBERAL;
	public static final String ROBUSTIFIER_IGNORE_LAST = D4Config.ROBUST_IGNORELAST;
	public static final String TRIMMER_CONSERVATIVE = D4Config.TRIMMER_CONSERVATIVE;
	public static final String TRIMMER_LIBERAL = D4Config.TRIMMER_LIBERAL;
	public static final String TRIMMER_CENTRIST = D4Config.TRIMMER_CENTRIST;
	public static final OpenOption[] createAndAppend = new OpenOption[]{StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.APPEND};

	public static final Map<String, Boolean> MOVIE_DOMAIN_LABEL = Map.ofEntries(
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
	public static final Map<String, Boolean> FIFA_PLAYERS_DOMAIN_LABEL = Map.ofEntries(
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
	private static double currentThreshold;

	public static void main(String[] args) throws IOException {
		runVariationsExperiments();
//		LOG_DURATION = true;
		runEvaluationExperiments();
    }

	private static void runVariationsExperiments() throws IOException {
		Path precisionDatasetDomainPath = generateDomainRepresentation(MOVIE_DOMAIN_INPUT_DATASETS_COMBINED, 	VARIATIONS_OUTPUT_DOMAIN_REPRESENTATION_PRECISION_OPTIMIZED, 	JACCARD_INDEX, 					ROBUSTIFIER_LIBERAL, 		TRIMMER_CONSERVATIVE, 	false, 	false);
		Path termFreqDatasetDomainPath = generateDomainRepresentation(MOVIE_DOMAIN_INPUT_DATASETS_COMBINED, 	VARIATIONS_OUTPUT_DOMAIN_REPRESENTATION_PRECISION_OPTIMIZED_TF, TERM_FREQUENCY_BASED_JACCARD, 	ROBUSTIFIER_LIBERAL, 		TRIMMER_CONSERVATIVE, 	false, 	false);
		Path accuracyDatasetDomainPath = generateDomainRepresentation(MOVIE_DOMAIN_INPUT_DATASETS_COMBINED, 	VARIATIONS_OUTPUT_DOMAIN_REPRESENTATION_ACCURACY_OPTIMIZED, 	JACCARD_INDEX, 					ROBUSTIFIER_LIBERAL, 		TRIMMER_CENTRIST, 		false, 	true);
		Path noExpandDatasetDomainPath = generateDomainRepresentation(MOVIE_DOMAIN_INPUT_DATASETS_COMBINED, 	VARIATIONS_OUTPUT_DOMAIN_REPRESENTATION_NO_EXPAND, 				JACCARD_INDEX, 					ROBUSTIFIER_LIBERAL, 		TRIMMER_CONSERVATIVE, 	true, 	false);
		Path allTermsDatasetDomainPath = generateDomainRepresentation(MOVIE_DOMAIN_INPUT_DATASETS_COMBINED, 	VARIATIONS_OUTPUT_DOMAIN_REPRESENTATION_ALL_TERMS, 				JACCARD_INDEX, 					ROBUSTIFIER_LIBERAL, 		TRIMMER_CONSERVATIVE, 	false, 	true);
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
		evaluateAccuracyForDatasetDomain(MOVIE_DOMAIN_INPUT_DATASETS_COMBINED, MOVIE_DOMAIN_LABEL, EVALUATION_MOVIE_OUTPUT_DIR);
	}

	private static void evaluateAccuracyForFifaPlayersDomain() throws IOException {
		evaluateAccuracyForDatasetDomain(FIFA_PLAYERS_DOMAIN_INPUT_DATASETS_COMBINED, FIFA_PLAYERS_DOMAIN_LABEL, EVALUATION_FIFA_PLAYERS_OUTPUT_DIR);
	}

	private static void evaluateAccuracyForDatasetDomain(Path domainDatasetsCombinedPath, Map<String, Boolean> domainTruth, Path outputPath) throws IOException {
		Path domainRepresentationPath = outputPath.resolve(EVALUATION_DOMAIN_REPRESENTATION_DIR);
		Path evaluationMovieDomainPath = generateDomainRepresentation(domainDatasetsCombinedPath, domainRepresentationPath, JACCARD_INDEX, ROBUSTIFIER_LIBERAL, TRIMMER_CONSERVATIVE, false, false);
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
		List<Path> datasetSimilarityScorePaths = new ArrayList<>();
		for(Path inputDataset: inputDatasets) {
			Path termIndexPath = generateTargetDatasetTermIndex(inputDataset, outputPath);
			Path similarityScoresCsvPath = termIndexPath.getParent().resolve(DATASET_SIMILARITY_SCORES_CSV_NAME);
			MatchingResult matchingResult = calculateSimilarityToTargetDatasetForDomainRepresentation(termIndexPath, datasetDomainPath);
			writeSingleSimilarityScoreToCsvFile(EVALUATION_SELECTED_VARIANT, matchingResult, similarityScoresCsvPath);
			datasetSimilarityScorePaths.add(similarityScoresCsvPath);
    	}
		List<DatasetSimilarity> allSimilarityScores = combineDatasetSimilarityScoresInCsvFile(datasetSimilarityScorePaths, outputPath);
    	ResultValidation resultValidationOriginalOrder = evaluateSimilarityScores(domainTruth, allSimilarityScores, outputPath);
    	evaluateAllSubsets(domainTruth, allSimilarityScores, outputPath);
    	return resultValidationOriginalOrder;
	}

	private static void evaluateAllSubsets(Map<String, Boolean> domainTruth, List<DatasetSimilarity> allSimilarityScores, Path outputPath) throws IOException {
		Path resultPath = outputPath.resolve(DATASET_RESULT_VALIDATION_ALL_SUBSETS_FILE_NAME);
		if(resultPath.toFile().exists()) {
			return;
		}
		else {
			String csvheader = "dataset_amount,accuracy,precision,recall,negative_precision,negative_recall,threshold\n";
			Files.writeString(resultPath, csvheader, createAndAppend);
		}
		for(int i = 1; i <= allSimilarityScores.size(); i++) {
			ResultValidation resultValidation = evaluateSimilarityScores(domainTruth, allSimilarityScores.subList(0, i), null);
			String csvEntry = String.format("%d,%f,%f,%f,%f,%f,%f\n",
					i,
					resultValidation.getAccuracy(),
					resultValidation.getPrecision(),
					resultValidation.getRecall(),
					resultValidation.getNegPrecision(),
					resultValidation.getNegRecall(),
					currentThreshold);
			Files.writeString(resultPath, csvEntry, createAndAppend);
		}
	}

	private static ResultValidation evaluateSimilarityScores(Map<String, Boolean> domainTruth, List<DatasetSimilarity> allSimilarityScores, Path outputPath) throws IOException {
		Set<String> similarDatasets = discoverDatasetsSimilarToDomain(allSimilarityScores, outputPath);
    	ResultValidation resultValidation = validateResults(domainTruth, allSimilarityScores, similarDatasets, outputPath);
		return resultValidation;
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

	private static Path generateDomainRepresentation(Path domainRepresentativeDatasetPath, Path outputPath, String simAlgo, String robustifier, String trimmer, boolean noExpand, boolean allTerms) throws IOException {
		Path columnDomainsPath = generateColumnDomains(domainRepresentativeDatasetPath, outputPath, simAlgo, robustifier, trimmer, noExpand);
		return generateDatasetDomain(columnDomainsPath, allTerms);
	}

	private static Path generateColumnDomains(Path domainRepresentativeDatasetPath, Path outputPath, String simAlgo, String robustifier, String trimmer, boolean noExpand) throws IOException {
		ZonedDateTime start = ZonedDateTime.now();
		ensureOutputDirExists(outputPath);
		Path columnsPath = generateColumnFiles(domainRepresentativeDatasetPath, outputPath);
		Path termIndexPath = generateTermIndex(columnsPath);
		Path eqsPath = generateEquivalenceClasses(termIndexPath);
		Path signaturesPath = computeSignatures(eqsPath, simAlgo, robustifier);
		Path expandedColumnsPath;
		if(noExpand) {
			expandedColumnsPath = noExpandColumns(eqsPath);
		}
		else {
			expandedColumnsPath = expandColumns(eqsPath, signaturesPath, trimmer);
		}
		Path localDomainsPath = discoverLocalDomains(eqsPath, signaturesPath, expandedColumnsPath);
		Path columnDomainsInternalFormatPath = pruneToStrongDomains(eqsPath, localDomainsPath);
		Path columnDomainsPath = exportStrongDomains(termIndexPath, eqsPath, columnDomainsInternalFormatPath);
		logDuration(start, "generating column domain", outputPath);
		return columnDomainsPath;
	}

	private static Path generateDatasetDomain(Path columnDomainsPath, boolean allTerms) throws IOException {
		if(!inputExists(columnDomainsPath)) {
			throw new IllegalArgumentException(String.format("The path \"%s\" to the column domains used as input does not exist"));
		}
		Path datasetDomainPath = columnDomainsPath.getParent().resolve(DATASET_DOMAIN_DIR_NAME);
		if(!outputExists(datasetDomainPath)) {
			ensureOutputDirExists(datasetDomainPath);
			ZonedDateTime start = ZonedDateTime.now();
			Files.list(columnDomainsPath).forEach(columnDomainPath -> writeColumnDomainTermsUsedInDatasetDomain(columnDomainPath, datasetDomainPath, allTerms));
			logDuration(start, "generating dataset domain", columnDomainsPath.getParent().getParent());
		}
		return datasetDomainPath;
	}

	private static void writeColumnDomainTermsUsedInDatasetDomain(Path columnDomainPath, Path datasetDomainPath, boolean allTerms) {
		Path datasetDomainTermsForColumnDomain = datasetDomainPath.resolve(columnDomainPath.getFileName());
		if(outputExists(datasetDomainTermsForColumnDomain)) {
			return;
		}
		try(Reader reader = Files.newBufferedReader(columnDomainPath)){
    		ExportedStrongDomain strongDomain = new GsonBuilder()
    				.create()
    				.fromJson(reader, ExportedStrongDomain.class);
	    	ColumnDomain columnDomain = new ColumnDomain();
	    	columnDomain.setColumns(strongDomain.getColumns());
	    	if(allTerms) {
	    		columnDomain.setTerms(strongDomain.getTerms().stream()
	    				.flatMap(termsBlock -> termsBlock.stream())
		    			.flatMap(eq -> eq.getTerms().stream())
		    			.toList());
	    	}
	    	else {
	    		columnDomain.setTerms(strongDomain.getTerms().get(0).stream()
		    			.flatMap(eq -> eq.getTerms().stream())
		    			.toList());
	    	}

	    	try(Writer writer = Files.newBufferedWriter(datasetDomainTermsForColumnDomain)){
		    	new GsonBuilder()
				.setPrettyPrinting()
				.create()
				.toJson(columnDomain, writer);
	    	}
    	} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static boolean inputExists(Path inputPath) {
		if(inputPath.toFile().exists()) {
			return true;
		}
		else {
			System.err.println(inputPath.toString() + " does not exist. Cannot continue without this.");
			return false;
		}
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
		Path termIndexPath = generateTargetDatasetTermIndex(datasetFolderName, outputDirPath);
		Path similarityScoresCsvPath = termIndexPath.getParent().resolve(DATASET_SIMILARITY_SCORES_CSV_NAME);
		if(!outputExists(similarityScoresCsvPath)) {
			for(Path domainRepresentationVariationPath: domainRepresentationVariationPaths) {
				MatchingResult matchingResultPath = calculateSimilarityToTargetDatasetForDomainRepresentation(termIndexPath, domainRepresentationVariationPath);
				writeSimilarityScoreToCsvFile(
						domainRepresentationVariationPath.getParent().getFileName().toString(),
						matchingResultPath,
						similarityScoresCsvPath);
			}
		}
		return similarityScoresCsvPath;
	}

	private static void writeSingleSimilarityScoreToCsvFile(String name, MatchingResult matchingResult, Path similarityScoresCsvPath) throws IOException {
		if(similarityScoresCsvPath.toFile().exists()) {
			return;
		}
		writeSimilarityScoreToCsvFile(name, matchingResult, similarityScoresCsvPath);
	}

	private static void writeSimilarityScoreToCsvFile(String name, MatchingResult matchingResult, Path similarityScoresCsvPath) throws IOException {
		if(!similarityScoresCsvPath.toFile().exists()) {
			Files.writeString(similarityScoresCsvPath, "domain_representation_type,domain_representation_size,dataset_size,matched,overlap_coefficient\n", createAndAppend);
		}
		String csvOutput = String.format(
				"%s,%d,%d,%d,%f\n",
				name,
				matchingResult.getDomainRepresentationSize(),
				matchingResult.getDatasetSize(),
				matchingResult.getMatched(),
				matchingResult.getSimilarityScore());
		Files.writeString(similarityScoresCsvPath, csvOutput, createAndAppend);
	}

	private static Path generateTargetDatasetTermIndex(Path inputDataset, Path outputDirPath) throws IOException {
		ZonedDateTime start = ZonedDateTime.now();
		Path outputPath = convertDatasetInputPathToOutputPath(inputDataset, outputDirPath);
		ensureOutputDirExists(outputPath);
		Path columnsPath = generateColumnFiles(inputDataset, outputPath);
		Path termIndexPath = generateTermIndex(columnsPath);
		logDuration(start, "generating term index for " + inputDataset, outputDirPath);
		return termIndexPath;
	}

	private static MatchingResult calculateSimilarityToTargetDatasetForDomainRepresentation(Path termIndexPath, Path datasetDomainPath) throws IOException {
		if(!inputExists(termIndexPath)) {
			throw new IllegalArgumentException(String.format("The input dataset %s does not exist.", termIndexPath.toString()));
		}
		Path outputPath = termIndexPath.getParent();
		ensureOutputDirExists(outputPath);
		Path matchingResultPath = outputPath.resolve(String.format("matching_result_%s_%s.json", termIndexPath.getParent().getFileName().toString(), datasetDomainPath.getParent().getFileName().toString()));
		if(outputExists(matchingResultPath)) {
			try(Reader reader = Files.newBufferedReader(matchingResultPath)){
				MatchingResult existingMatchingResult = new Gson().fromJson(reader, MatchingResult.class);
				return existingMatchingResult;
			}
		}
		ZonedDateTime start = ZonedDateTime.now();
		MatchingResult matchingResult = countTargetDatasetMatchedTerms(termIndexPath, datasetDomainPath);
		double overlapCoefficient = (matchingResult.getMatched() * 1d) / (Math.min(matchingResult.getDatasetSize(), matchingResult.getDomainRepresentationSize()));
		matchingResult.setSimilarityScore(overlapCoefficient);
		try(Writer writer = Files.newBufferedWriter(matchingResultPath)){
	    	new GsonBuilder().setPrettyPrinting().create().toJson(matchingResult, writer);
    	}
		logDuration(
				start,
				"calculating similarity score for " + termIndexPath.getParent().toString() + " using the " + datasetDomainPath.getParent() + " domain representation",
				termIndexPath.getParent().getParent());
		return matchingResult;
	}

	private static Path convertDatasetInputPathToOutputPath(Path inputDataset, Path outputDirPath) {
		return outputDirPath.resolve(inputDataset.getFileName());
	}

	private static MatchingResult countTargetDatasetMatchedTerms(Path termIndexPath, Path domainRepresentationTermsPath) throws JsonSyntaxException, JsonIOException, IOException {
		if(!inputExists(termIndexPath) || !inputExists(domainRepresentationTermsPath)) {
			throw new IllegalArgumentException();
		}
		Path outputPath = termIndexPath.getParent();
		ensureOutputDirExists(outputPath);

		List<ColumnDomain> datasetDomain = new ArrayList<>();
		Files.list(domainRepresentationTermsPath).forEach(datasetDomainFile -> {
			try(Reader reader = Files.newBufferedReader(datasetDomainFile)){
				datasetDomain.add(new GsonBuilder()
						.create()
						.fromJson(reader, ColumnDomain.class));
			} catch (IOException e) {
				e.printStackTrace();
			}
		});
		Set<String> datasetDomainTerms = datasetDomain.stream()
				.flatMap(columnDomain -> columnDomain.getTerms().stream())
				.collect(Collectors.toSet());
		if(datasetDomainTerms.size() == 0) {
			String error = "The domain representation did not contain any terms. The similarity cannot be calculated.";
			System.err.println(error);
			throw new IllegalStateException(error);
		}
		Set<String> datasetTerms = readTermsFromIndexFile(termIndexPath);
		Set<String> matchedDatasetTerms = datasetTerms.stream()
				.filter(datasetTerm -> datasetDomainTerms.contains(datasetTerm))
				.collect(Collectors.toSet());

		if(LOG_MATCHED_TERMS) {
			Path matchedTermsPath = outputPath
					.resolve("matched-terms.txt");
			for(String matchedTerm: matchedDatasetTerms) {
				Files.writeString(matchedTermsPath, matchedTerm+"\n", createAndAppend);
			}
			Files.writeString(matchedTermsPath, "\n", createAndAppend);
		}
		return new MatchingResult(matchedDatasetTerms.size(), datasetDomainTerms.size(), datasetTerms.size());
	}

	private static List<DatasetSimilarity> combineDatasetSimilarityScoresInCsvFile(List<Path> datasetSimilarityScoresPaths, Path outputPath) throws IOException {
		Path combinedSimilarityScoresPath = outputPath.resolve(EVALUATION_OUTPUT_ALL_SIMILARITY_SCORES_CSV);
		if(outputExists(combinedSimilarityScoresPath)) {
			return readDatasetSimilarityScores(combinedSimilarityScoresPath);
		}
		else {
			List<DatasetSimilarity> datasetSimilarityScores = new ArrayList<>();
			ZonedDateTime start = ZonedDateTime.now();
			Files.writeString(combinedSimilarityScoresPath, "dataset_name,domain_representation_type,domain_representation_size,dataset_size,matched,overlap_coefficient\n", createAndAppend);
			for (Path datasetSimilarityScoresPath : datasetSimilarityScoresPaths) {
				if(!inputExists(datasetSimilarityScoresPath)) {
					continue;
				}
				String datasetResult = Files.readAllLines(datasetSimilarityScoresPath).stream()
						.filter(line -> line.startsWith(EVALUATION_SELECTED_VARIANT+","))
						.findAny()
						.orElseThrow();
				String similarityScore = datasetResult.split(",")[4];
				String datasetName = datasetSimilarityScoresPath.getParent().getFileName().toString();
				datasetSimilarityScores.add(new DatasetSimilarity(datasetName, Double.valueOf(similarityScore)));
				Files.writeString(combinedSimilarityScoresPath, datasetName + "," + datasetResult + "\n", createAndAppend);
			}
			logDuration(start, "writing precision similarity scores to a single file", outputPath);
			return datasetSimilarityScores;
		}
	}

	private static Set<String> discoverDatasetsSimilarToDomain(List<DatasetSimilarity> similarityScores, Path outputPath) throws IOException {
		Path domainSimilarityResultPath = outputPath == null ? null : outputPath.resolve(EVALUATION_OUTPUT_DATASETS_FOCUSED_ON_DOMAIN);
		Set<DatasetSimilarity> similarDatasets;
		if(outputExists(domainSimilarityResultPath)) {
			similarDatasets = readSimilarDataset(domainSimilarityResultPath);
		}
		else {
			ZonedDateTime start = ZonedDateTime.now();
			List<DatasetSimilarity> sortedByDescendingSimilarityScore = new ArrayList<>(similarityScores);
			Set<String> addedSimilarityScoreNames = addSimilarityScores(sortedByDescendingSimilarityScore);
			Collections.sort(sortedByDescendingSimilarityScore, Comparator.comparingDouble(DatasetSimilarity::getSimilarityScore).reversed());
			calculateConsecutiveDrops(sortedByDescendingSimilarityScore);
			List<SimilarDatasetsGroup> groupedByConsecutiveSteepestDrop = groupDatasetsByConsecutiveDrop(sortedByDescendingSimilarityScore, outputPath);
			List<SimilarDatasetsGroup> selectedGroups = groupedByConsecutiveSteepestDrop.subList(0, groupedByConsecutiveSteepestDrop.size()-1);
			similarDatasets = selectedGroups.stream()
					.flatMap(group -> group.getSimilarDatasets().stream())
					.filter(datasetSimilarity -> !addedSimilarityScoreNames.contains(datasetSimilarity.getDatasetName()))
					.collect(Collectors.toSet());
			if(domainSimilarityResultPath != null) {
				String groupHeader = "dataset_name,similarity_score\n";
				Files.writeString(domainSimilarityResultPath, groupHeader, createAndAppend);
				for(DatasetSimilarity dataset : similarDatasets) {
					String datasetOutput = String.format("%s,%f\n", dataset.getDatasetName(), dataset.getSimilarityScore());
					Files.writeString(domainSimilarityResultPath, datasetOutput, createAndAppend);
				}
			}
			logDuration(start, "determining datasets that are considered similar to the domain", outputPath);
		}
		return similarDatasets.stream().map(DatasetSimilarity::getDatasetName).collect(Collectors.toSet());
	}

	private static Set<String> addSimilarityScores(List<DatasetSimilarity> sortedByDescendingSimilarityScore) {
		Set<String> addedSimilarityScoreNames = new HashSet<>();
		Set<String> datasetNames = sortedByDescendingSimilarityScore.stream()
				.map(DatasetSimilarity::getDatasetName)
				.collect(Collectors.toSet());
		List<Double> addedScores = List.of(0d, 0d, 0.5d, 1d);
		for (int i = 0; i < 4; i++) {
			String nameForAddedScore = UUID.randomUUID().toString();
			//Ensure uniqueness of name
			while(datasetNames.contains(nameForAddedScore)) {
				nameForAddedScore = UUID.randomUUID().toString();
			}
			addedSimilarityScoreNames.add(nameForAddedScore);
			sortedByDescendingSimilarityScore.add(new DatasetSimilarity(nameForAddedScore, addedScores.get(i)));
		}
		return addedSimilarityScoreNames;
	}

	private static List<DatasetSimilarity> readDatasetSimilarityScores(Path similarityScoresPath) throws IOException {
		List<DatasetSimilarity> similarityScores = new ArrayList<>();

		List<String> allLines = Files.readAllLines(similarityScoresPath);
		allLines.remove(0); // remove header line
		allLines.forEach(line -> {
			String[] values = line.split(",");
			String dataset = values[0];
			double score = Double.valueOf(values[values.length-1]);
			similarityScores.add(new DatasetSimilarity(dataset, score));
		});
		return similarityScores;
	}

	private static Set<DatasetSimilarity> readSimilarDataset(Path similarDatasetsPath) throws IOException {
		Set<DatasetSimilarity> similarDatasets = new HashSet<>();

		List<String> allLines = Files.readAllLines(similarDatasetsPath);
		allLines.remove(0); // remove header line
		allLines.forEach(line -> {
			String[] values = line.split(",");
			String dataset = values[0];
			double score = Double.valueOf(values[values.length-1]);
			similarDatasets.add(new DatasetSimilarity(dataset, score));
		});
		return similarDatasets;
	}

	private static void calculateConsecutiveDrops(List<DatasetSimilarity> sortedByDescendingSimilarityScore) {
		for(int i = 0; i < sortedByDescendingSimilarityScore.size(); i++) {
			DatasetSimilarity currentScore = sortedByDescendingSimilarityScore.get(i);
			currentScore.setSimilarityOrderIndex(i);
			if(i == 0) {
				currentScore.setConsecutiveDrop(-1d); // the first item has no consecutive drop
			}
			else {
				DatasetSimilarity previousScore = sortedByDescendingSimilarityScore.get(i-1);
				currentScore.setConsecutiveDrop(previousScore.getSimilarityScore() - currentScore.getSimilarityScore());
			}
		}
	}

	private static List<SimilarDatasetsGroup> groupDatasetsByConsecutiveDrop(List<DatasetSimilarity> sortedByDescendingSimilarityScore, Path outputPath) throws IOException {
		List<SimilarDatasetsGroup> groupedByConsecutiveDrop = new ArrayList<>();
		List<DatasetSimilarity> validDropsSortedDescending = new ArrayList<>(sortedByDescendingSimilarityScore.subList(1, sortedByDescendingSimilarityScore.size()));
		Collections.sort(validDropsSortedDescending, Comparator.comparingDouble(DatasetSimilarity::getConsecutiveDrop).reversed());
		double secondHighestDrop = validDropsSortedDescending.get(1).getConsecutiveDrop();
		double secondLowestDrop = validDropsSortedDescending.get(validDropsSortedDescending.size()-2).getConsecutiveDrop();
		double dropThreshold = (secondHighestDrop + secondLowestDrop)/2d;
		currentThreshold = dropThreshold;
		for(DatasetSimilarity score : sortedByDescendingSimilarityScore) {
			if(groupedByConsecutiveDrop.isEmpty() || score.getConsecutiveDrop() > dropThreshold) {
				SimilarDatasetsGroup group = new SimilarDatasetsGroup(new ArrayList<>());
				group.getSimilarDatasets().add(score);
				groupedByConsecutiveDrop.add(group);
			}
			else {
				groupedByConsecutiveDrop.get(groupedByConsecutiveDrop.size()-1).getSimilarDatasets().add(score);
			}
		}
		return groupedByConsecutiveDrop;
	}

	private static Set<String> readTermsFromIndexFile(Path termIndexFile) throws IOException{
		Set<String> datasetTerms = new HashSet<>();
		TermConsumer consumer = new TermConsumer() {
			@Override
			public void open() {}

			@Override
			public void consume(Term term) {
				datasetTerms.add(term.name());
			}

			@Override
			public void close() {}
		};
		new TermIndexReader(termIndexFile.toFile()).read(consumer);
		return datasetTerms;
	}

	private static void ensureOutputDirExists(Path outputDirPath) {
		File outputFile = outputDirPath.toFile();
		if(!outputFile.exists()) {
			outputFile.mkdirs();
		}
	}

	private static void logDuration(ZonedDateTime startTime, String taskDescription, Path outputPath) throws IOException {
		if(!LOG_DURATION) {
			return;
		}
		if(outputPath == null) {
			return;
		}
		Path durationLogPath = outputPath.resolve(EVALUATION_TASK_DURATIONS_CSV);
		ZonedDateTime endTime = ZonedDateTime.now();
		Duration duration = Duration.between(startTime, endTime);
		Files.writeString(durationLogPath, duration.toString()+ ",\"" + taskDescription + "\"\n", createAndAppend);
	}

	private static Path generateColumnFiles(Path inputPath, Path outputPath) {
		// ----------------------------------------------------------------
	    // GENERATE COLUMN FILES
	    // ----------------------------------------------------------------
	    try {
	    	Path outputColumnsPath = outputPath.resolve(COLUMNS_DIR_NAME);
	        if(!outputExists(outputColumnsPath)) {
				new D4().columns(
		                inputPath.toFile(),
		                outputPath.resolve(COLUMNS_METADATA_FILE_NAME).toFile(),
		                1000,
		                6,
		                true,
		                outputColumnsPath.toFile()
		        );
	        }
	        return outputColumnsPath;
	    } catch (java.lang.InterruptedException | java.io.IOException ex) {
	    	System.err.print("Generating columns failed with exception: ");
	    	ex.printStackTrace();
	        System.exit(-1);
	    }
	    return null;
	}

	private static Path generateTermIndex(Path columnsPath) {
		// ----------------------------------------------------------------
	    // GENERATE TERM INDEX
	    // ----------------------------------------------------------------
	    try {
	        Path outputTermIndex = columnsPath.getParent().resolve(TERM_INDEX_FILE_NAME);
	        if(!outputExists(outputTermIndex)) {
				new D4().termIndex(
						columnsPath.toFile(),
		                Threshold.getConstraint("GT0.5"),
		                10000000,
		                false,
		                6,
		                true,
		                outputTermIndex.toFile()
		        );
	        }
	        return outputTermIndex;
	    } catch (java.lang.InterruptedException | java.io.IOException ex) {
	    	System.err.print("Generating term index failed with exception: ");
	    	ex.printStackTrace();
	        System.exit(-1);
	    }
	    return null;
	}

	private static Path generateEquivalenceClasses(Path termIndexPath) {
		// ----------------------------------------------------------------
	    // GENERATE EQUIVALENCE CLASSES
	    // ----------------------------------------------------------------
	    try {
	        Path outputEquivalenceClasses = termIndexPath.getParent().resolve(EQUIVALENCE_CLASSES_FILE_NAME);
	        if(!outputExists(outputEquivalenceClasses)) {
				new D4().eqs(
						termIndexPath.toFile(),
		                true,
		                outputEquivalenceClasses.toFile()
		        );
	        }
	        return outputEquivalenceClasses;
	    } catch (java.io.IOException ex) {
	    	System.err.print("Generating equivalence classes failed with exception: ");
	    	ex.printStackTrace();
	        System.exit(-1);
	    }
	    return null;
	}

	private static Path computeSignatures(Path equivalenceClassesPath, String simAlgo, String robustifier) {
		// ----------------------------------------------------------------
	    // COMPUTE SIGNATURES
	    // ----------------------------------------------------------------
	    try {
	        Path outputSignatures = equivalenceClassesPath.getParent().resolve(SIGNATURES_FILE_NAME);
	        if(!outputExists(outputSignatures)) {
				new D4().signatures(
						equivalenceClassesPath.toFile(),
		                simAlgo,
		                robustifier,
		                true,
		                false,
		                D4Config.ROBUST_IGNORELAST.equals(robustifier), // ignore minor drops only when ignore-last robustifier is used
		                6,
		                true,
		                new TelemetryPrinter(),
		                outputSignatures.toFile()
		        );
	        }
	        return outputSignatures;
	    } catch (java.lang.InterruptedException | java.io.IOException ex) {
	    	System.err.print("Computing signatures failed with exception: ");
	    	ex.printStackTrace();
	        System.exit(-1);
	    }
	    return null;
	}

	private static Path expandColumns(Path equivalenceClassesPath, Path signaturesPath, String trimmer) {
		// ----------------------------------------------------------------
	    // EXPAND COLUMNS
	    // ----------------------------------------------------------------
	    try {
	        Path outputExpandColumns = signaturesPath.getParent().resolve(EXPANDED_COLUMNS_FILE_NAME);
	        if(!outputExists(outputExpandColumns)) {
				new D4().expandColumns(
						equivalenceClassesPath.toFile(),
		                signaturesPath.toFile(),
		                trimmer,
		                Threshold.getConstraint("GT0.25"),
		                5,
		                new BigDecimal("0.05"),
		                6,
		                true,
		                new TelemetryPrinter(),
		                outputExpandColumns.toFile()
		        );
	        }
	        return outputExpandColumns;
	    } catch (java.io.IOException ex) {
	    	System.err.print("Expanding columns failed with exception: ");
	    	ex.printStackTrace();
	        System.exit(-1);
	    }
	    return null;
	}

	private static Path noExpandColumns(Path equivalenceClassesPath) {
		// ----------------------------------------------------------------
	    // EXPAND COLUMNS
	    // ----------------------------------------------------------------
	    try {
	        Path outputNoExpandColumns = equivalenceClassesPath.getParent().resolve(EXPANDED_COLUMNS_FILE_NAME);
	        if(!outputExists(outputNoExpandColumns)) {
				new D4().writeColumns(
						equivalenceClassesPath.toFile(),
		                true,
		                outputNoExpandColumns.toFile()
		        );
	        }
	        return outputNoExpandColumns;
	    } catch (java.io.IOException ex) {
	    	System.err.print("Non-expanding columns failed with exception: ");
	    	ex.printStackTrace();
	        System.exit(-1);
	    }
	    return null;
	}

	private static Path discoverLocalDomains(Path equivalenceClassesPath, Path signaturesPath, Path expandedColumnsPath) {
		// ----------------------------------------------------------------
	    // DISCOVER LOCAL DOMAINS
	    // ----------------------------------------------------------------
	    try {
	        Path outputLocalDomains = expandedColumnsPath.getParent().resolve(LOCAL_DOMAINS_FILE_NAME);
	        if(!outputExists(outputLocalDomains)) {
				new D4().localDomains(
		                equivalenceClassesPath.toFile(),
		                expandedColumnsPath.toFile(),
		                signaturesPath.toFile(),
		                D4Config.TRIMMER_CONSERVATIVE,
		                false,
		                6,
		                false,
		                true,
		                new TelemetryPrinter(),
		                outputLocalDomains.toFile()
		        );
	        }
	        return outputLocalDomains;
	    } catch (java.io.IOException ex) {
	    	System.err.print("Discovering local domains failed with exception: ");
	    	ex.printStackTrace();
	        System.exit(-1);
	    }
	    return null;
	}

	private static Path pruneToStrongDomains(Path equivalenceClassesPath, Path localDomainsPath) {
		// ----------------------------------------------------------------
	    // PRUNE STRONG DOMAINS
	    // ----------------------------------------------------------------
	    try {
	        Path outputStrongDomains = localDomainsPath.getParent().resolve(COLUMN_DOMAINS_INTERNAL_FILE_NAME);
	        if(!outputExists(outputStrongDomains)) {
				new D4().strongDomains(
		                equivalenceClassesPath.toFile(),
		                localDomainsPath.toFile(),
		                Threshold.getConstraint("GT0.5"),
		                Threshold.getConstraint("GT0.1"),
		                new BigDecimal("0.25"),
		                6,
		                true,
		                new TelemetryPrinter(),
		                outputStrongDomains.toFile()
		        );
	        }
	        return outputStrongDomains;
	    } catch (java.lang.InterruptedException | java.io.IOException ex) {
	    	System.err.print("Pruning strong domains failed with exception: ");
	    	ex.printStackTrace();
	        System.exit(-1);
	    }
	    return null;
	}

	private static Path exportStrongDomains(Path termIndexPath, Path equivalenceClassesPath, Path columnDomainsInternalFormatPath) {
		// ----------------------------------------------------------------
	    // EXPORT
	    // ----------------------------------------------------------------
	    try {
	        Path outputExport = columnDomainsInternalFormatPath.getParent().resolve(COLUMN_DOMAINS_DIR_NAME);
	        if(!outputExists(outputExport)) {
				new D4().exportStrongDomains(
		                equivalenceClassesPath.toFile(),
		                termIndexPath.toFile(),
		                columnDomainsInternalFormatPath.getParent().resolve(COLUMNS_METADATA_FILE_NAME).toFile(),
		                columnDomainsInternalFormatPath.toFile(),
		                100,
		                true,
		                outputExport.toFile()
		        );
	        }
			return outputExport;
	    } catch (java.io.IOException ex) {
	    	System.err.print("Exporting strong domains failed with exception: ");
	    	ex.printStackTrace();
	        System.exit(-1);
	    }
	    return null;
	}
}
