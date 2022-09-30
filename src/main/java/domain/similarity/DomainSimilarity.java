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
import java.util.Set;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

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
	public static boolean LOG_MATCHED_TERMS = false;
	public static boolean LOG_DURATION = false;

	public static final Path INPUT_DIR = Paths.get("input");
	public static final Path INPUT_DOMAIN_REPRESENTATION_DATA = INPUT_DIR.resolve("domain-data");
	public static final Path VARIATIONS_OUTPUT_DIR= Paths.get("output-variations");
	public static final Path VARIATIONS_OUTPUT_DOMAIN_REPRESENTATION_DIR = VARIATIONS_OUTPUT_DIR.resolve("domain-representation");
	public static final Path VARIATIONS_OUTPUT_DOMAIN_SIMILARITY_DIR = VARIATIONS_OUTPUT_DIR.resolve("domain-similarity");
	public static final Path VARIATIONS_OUTPUT_DOMAIN_REPRESENTATION_PRECISION_OPTIMIZED = VARIATIONS_OUTPUT_DOMAIN_REPRESENTATION_DIR.resolve("precision");
	public static final Path VARIATIONS_OUTPUT_DOMAIN_REPRESENTATION_PRECISION_OPTIMIZED_TF = VARIATIONS_OUTPUT_DOMAIN_REPRESENTATION_DIR.resolve("precision-tf");
	public static final Path VARIATIONS_OUTPUT_DOMAIN_REPRESENTATION_ACCURACY_OPTIMIZED = VARIATIONS_OUTPUT_DOMAIN_REPRESENTATION_DIR.resolve("accuracy");
	public static final Path VARIATIONS_OUTPUT_DOMAIN_REPRESENTATION_ALL_TERMS = VARIATIONS_OUTPUT_DOMAIN_REPRESENTATION_DIR.resolve("all-terms");
	public static final Path VARIATIONS_OUTPUT_DOMAIN_REPRESENTATION_NO_EXPAND = VARIATIONS_OUTPUT_DOMAIN_REPRESENTATION_DIR.resolve("no-expand");

	public static final Path EVALUATION_OUTPUT_DIR= Paths.get("output-evaluation");
	public static final Path EVALUATION_OUTPUT_DOMAIN_REPRESENTATION = EVALUATION_OUTPUT_DIR.resolve("domain-representation");
	public static final Path EVALUATION_OUTPUT_DOMAIN_SIMILARITY = EVALUATION_OUTPUT_DIR.resolve("domain-similarity");
	public static final Path EVALUATION_TASK_DURATIONS_CSV = EVALUATION_OUTPUT_DIR.resolve("task_durations_evaluation.csv");
	public static final Path EVALUATION_OUTPUT_ALL_SIMILARITY_SCORES_CSV = EVALUATION_OUTPUT_DIR.resolve("similarity_scores.csv");
	public static final Path EVALUATION_OUTPUT_DATASETS_FOCUSED_ON_DOMAIN = EVALUATION_OUTPUT_DIR.resolve("datasets_focused_on_domain.csv");

	public static final String COLUMN_DOMAINS_DIR_NAME = "domains";
	public static final String DATASET_DOMAIN_DIR_NAME = "dataset-domain";
	public static final String DATASET_SIMILARITY_SCORES_CSV_NAME = "similarity_scores.csv";
	public static final String JACCARD_INDEX = D4Config.EQSIM_JI;
	public static final String TERM_FREQUENCY_BASED_JACCARD = D4Config.EQSIM_TFICF;
	public static final String ROBUSTIFIER_LIBERAL = D4Config.ROBUST_LIBERAL;
	public static final String ROBUSTIFIER_IGNORE_LAST = D4Config.ROBUST_IGNORELAST;
	public static final String TRIMMER_CONSERVATIVE = D4Config.TRIMMER_CONSERVATIVE;
	public static final String TRIMMER_LIBERAL = D4Config.TRIMMER_LIBERAL;
	public static final String TRIMMER_CENTRIST = D4Config.TRIMMER_CENTRIST;
	public static final OpenOption[] createAndAppend = new OpenOption[]{StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.APPEND};
	public static final Logger LOGGER = Logger.getLogger(D4.class.getName());
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
	public static final List<Path> EVALUATION_INPUT_DATASETS = List.of(
			INPUT_DIR.resolve("imdb"),
			INPUT_DIR.resolve("tmdb"),
			INPUT_DIR.resolve("indian"),
			INPUT_DIR.resolve("movies"),
			INPUT_DIR.resolve("netflix"),
			INPUT_DIR.resolve("tmdb-350k"),
			INPUT_DIR.resolve("rt"),
			INPUT_DIR.resolve("steam"),
			INPUT_DIR.resolve("datasets.data-cityofnewyork-us.education"),
			INPUT_DIR.resolve("datasets.data-cityofnewyork-us.finance"),
			INPUT_DIR.resolve("datasets.data-cityofnewyork-us.services"));

	public static void main(String[] args) throws IOException {
		runVariationsExperiments();
		LOG_DURATION = true;
		runEvaluationExperiments();
    }

	private static void runVariationsExperiments() throws IOException {
		generateDomainRepresentation(INPUT_DOMAIN_REPRESENTATION_DATA, VARIATIONS_OUTPUT_DOMAIN_REPRESENTATION_PRECISION_OPTIMIZED, 	JACCARD_INDEX, 					ROBUSTIFIER_LIBERAL, 		TRIMMER_CONSERVATIVE, 	false, 	false);
    	generateDomainRepresentation(INPUT_DOMAIN_REPRESENTATION_DATA, VARIATIONS_OUTPUT_DOMAIN_REPRESENTATION_PRECISION_OPTIMIZED_TF, 	TERM_FREQUENCY_BASED_JACCARD, 	ROBUSTIFIER_LIBERAL, 		TRIMMER_CONSERVATIVE, 	false, 	false);
    	generateDomainRepresentation(INPUT_DOMAIN_REPRESENTATION_DATA, VARIATIONS_OUTPUT_DOMAIN_REPRESENTATION_ACCURACY_OPTIMIZED, 		JACCARD_INDEX, 					ROBUSTIFIER_LIBERAL, 		TRIMMER_CENTRIST, 		false, 	true);
    	generateDomainRepresentation(INPUT_DOMAIN_REPRESENTATION_DATA, VARIATIONS_OUTPUT_DOMAIN_REPRESENTATION_NO_EXPAND, 				JACCARD_INDEX, 					ROBUSTIFIER_LIBERAL, 		TRIMMER_CONSERVATIVE, 	true, 	false);
    	generateDomainRepresentation(INPUT_DOMAIN_REPRESENTATION_DATA, VARIATIONS_OUTPUT_DOMAIN_REPRESENTATION_ALL_TERMS, 				JACCARD_INDEX, 					ROBUSTIFIER_LIBERAL, 		TRIMMER_CONSERVATIVE, 	false, 	true);
    	for(Path inputDataset: VARIATIONS_INPUT_DATASETS) {
    		calculateSimilarityToTargetDatasetForAllVariations(
    				inputDataset,
    				VARIATIONS_OUTPUT_DOMAIN_SIMILARITY_DIR,
    				List.of(
    						VARIATIONS_OUTPUT_DOMAIN_REPRESENTATION_PRECISION_OPTIMIZED,
    						VARIATIONS_OUTPUT_DOMAIN_REPRESENTATION_PRECISION_OPTIMIZED_TF,
    						VARIATIONS_OUTPUT_DOMAIN_REPRESENTATION_ACCURACY_OPTIMIZED,
    						VARIATIONS_OUTPUT_DOMAIN_REPRESENTATION_NO_EXPAND,
    						VARIATIONS_OUTPUT_DOMAIN_REPRESENTATION_ALL_TERMS));
    	}
	}

	private static void runEvaluationExperiments() throws IOException {
		generateDomainRepresentation(INPUT_DOMAIN_REPRESENTATION_DATA, EVALUATION_OUTPUT_DOMAIN_REPRESENTATION, JACCARD_INDEX, ROBUSTIFIER_LIBERAL, TRIMMER_CONSERVATIVE, false, false);
		for(Path inputDataset: EVALUATION_INPUT_DATASETS) {
			generateTargetDatasetTermIndex(inputDataset, EVALUATION_OUTPUT_DOMAIN_SIMILARITY);
			calculateSimilarityToTargetDatasetForDomainRepresentation(inputDataset, EVALUATION_OUTPUT_DOMAIN_REPRESENTATION, EVALUATION_OUTPUT_DOMAIN_SIMILARITY);
    	}
    	List<Path> datasetOutputPaths = EVALUATION_INPUT_DATASETS.stream()
				.map(inputDatasetPath -> convertDatasetInputPathToOutputPath(inputDatasetPath, EVALUATION_OUTPUT_DOMAIN_SIMILARITY))
				.toList();
		writeSimilarityScoresToFile(datasetOutputPaths, EVALUATION_OUTPUT_ALL_SIMILARITY_SCORES_CSV);
    	discoverDatasetsSimilarToDomain(EVALUATION_OUTPUT_ALL_SIMILARITY_SCORES_CSV, EVALUATION_OUTPUT_DATASETS_FOCUSED_ON_DOMAIN);
	}

	private static void generateDomainRepresentation(Path domainRepresentativeDatasetsPath, Path outputPath, String simAlgo, String robustifier, String trimmer, boolean noExpand, boolean allTerms) throws IOException {
		generateColumnDomains(domainRepresentativeDatasetsPath, outputPath, simAlgo, robustifier, trimmer, noExpand);
		generateDatasetDomain(outputPath, allTerms);
	}

	private static void generateColumnDomains(Path domainRepresentativeDatasetsPath, Path outputPath, String simAlgo, String robustifier, String trimmer, boolean noExpand) throws IOException {
		ZonedDateTime start = ZonedDateTime.now();
		ensureOutputPathExists(outputPath);
		generateColumnFiles(domainRepresentativeDatasetsPath, outputPath);
		generateTermIndex(outputPath);
		generateEquivalenceClasses(outputPath);
		computeSignatures(outputPath, simAlgo, robustifier);
		if(noExpand) {
			noExpandColumns(outputPath);
		}
		else {
			expandColumns(outputPath, trimmer);
		}
		discoverLocalDomains(outputPath);
		pruneToStrongDomains(outputPath);
		exportStrongDomains(outputPath);
		logDuration(start, "generating column domain");
	}

	private static void generateDatasetDomain(Path outputPath, boolean allTerms) throws IOException {
		// read exported JSON and output new JSON that only keeps the first block
		Path exportedColumnDomainsPath = outputPath.resolve(COLUMN_DOMAINS_DIR_NAME);
		if(!inputExists(exportedColumnDomainsPath)) {
			return;
		}
		Path datasetDomainPath = outputPath.resolve(DATASET_DOMAIN_DIR_NAME);
		if(outputExists(datasetDomainPath)) {
			return;
		}
		ensureOutputPathExists(datasetDomainPath);
		ZonedDateTime start = ZonedDateTime.now();
		Files.list(exportedColumnDomainsPath).forEach(columnDomainPath -> writeColumnDomainTermsUsedInDatasetDomain(columnDomainPath, datasetDomainPath, allTerms));
		logDuration(start, "generating dataset domain");
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
		if(outputPath.toFile().exists()) {
			System.out.println(outputPath.toString() + " already exists. Skipping this step.");
			return true;
		}
		else {
			return false;
		}
	}

	private static void calculateSimilarityToTargetDatasetForAllVariations(Path datasetFolderName, Path outputDirPath, List<Path> domainRepresentationVariationPaths) throws IOException {
		generateTargetDatasetTermIndex(datasetFolderName, outputDirPath);
		for(Path domainRepresentationVariationPath: domainRepresentationVariationPaths) {
			calculateSimilarityToTargetDatasetForDomainRepresentation(datasetFolderName, domainRepresentationVariationPath, outputDirPath);
		}
	}

	private static void generateTargetDatasetTermIndex(Path inputDataset, Path outputDirPath) throws IOException {
		ZonedDateTime start = ZonedDateTime.now();
		Path outputPath = convertDatasetInputPathToOutputPath(inputDataset, outputDirPath);
		ensureOutputPathExists(outputPath);
		generateColumnFiles(inputDataset, outputPath);
		generateTermIndex(outputPath);
		logDuration(start, "generating term index for " + inputDataset);
	}

	private static void calculateSimilarityToTargetDatasetForDomainRepresentation(Path inputDataset, Path domainRepresentationPath, Path outputDirPath) throws IOException {
		if(!inputExists(inputDataset)) {
			return;
		}
		Path outputPath = convertDatasetInputPathToOutputPath(inputDataset, outputDirPath);
		ensureOutputPathExists(outputPath);
		Path matchingResultFile = outputPath.resolve(String.format("matching_result_%s_%s.txt", inputDataset.getFileName().toString(), domainRepresentationPath.getFileName().toString()));
		Path matchingResultCsv = outputPath.resolve(DATASET_SIMILARITY_SCORES_CSV_NAME);
		if(outputExists(matchingResultFile)) {
			return;
		}
		ZonedDateTime start = ZonedDateTime.now();
		MatchingResult matchingResult = calculateTargetDatasetMatchedTerms(inputDataset, domainRepresentationPath.resolve(DATASET_DOMAIN_DIR_NAME), outputPath);
		String matchingResultMessage = String.format(
				"Matched %d terms on a dataset with %d terms, using a domain representation with %d terms\n",
				matchingResult.getMatched(),
				matchingResult.getDatasetSize(),
				matchingResult.getDomainRepresentationSize());
		System.out.print(matchingResultMessage);
		Files.writeString(matchingResultFile, matchingResultMessage, createAndAppend);
		if(!matchingResultCsv.toFile().exists()) {
			Files.writeString(matchingResultCsv, "domain_representation_type,domain_representation_size,dataset_size,matched,overlap_coefficient\n", createAndAppend);
		}
		double overlapCoefficient = (matchingResult.getMatched() * 1d) / (Math.min(matchingResult.getDatasetSize(), matchingResult.getDomainRepresentationSize()));
		String csvOutput = String.format(
				"%s,%d,%d,%d,%f\n",
				outputPath,
				matchingResult.getDomainRepresentationSize(),
				matchingResult.getDatasetSize(),
				matchingResult.getMatched(),
				overlapCoefficient);
		Files.writeString(matchingResultCsv, csvOutput, createAndAppend);
		logDuration(start, "calculating similarity score for " + inputDataset + " using the " + outputPath + " domain representation");
	}

	private static Path convertDatasetInputPathToOutputPath(Path inputDataset, Path outputDirPath) {
		return outputDirPath.resolve(inputDataset.getFileName());
	}

	private static MatchingResult calculateTargetDatasetMatchedTerms(Path inputDataset, Path domainRepresentationTermsPath, Path outputPath) throws JsonSyntaxException, JsonIOException, IOException {
		if(!inputExists(inputDataset) || !inputExists(domainRepresentationTermsPath)) {
			throw new IllegalArgumentException();
		}
		ensureOutputPathExists(outputPath);

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
		Path termIndexPath = outputPath.resolve("term-index.txt.gz");
		Set<String> datasetTerms = readTermsFromIndexFile(termIndexPath);
		Set<String> matchedDatasetTerms = datasetTerms.stream()
				.filter(datasetTerm -> datasetDomainTerms.contains(datasetTerm))
				.collect(Collectors.toSet());

		if(LOG_MATCHED_TERMS) {
			Path matchedTermsPath = outputPath
					.resolve("matched-terms")
					.resolve(domainRepresentationTermsPath.getParent().toString()+".txt");
			ensureOutputPathExists(matchedTermsPath);
			for(String matchedTerm: datasetTerms) {
				Files.writeString(matchedTermsPath, matchedTerm+"\n", createAndAppend);
			}
		}
		return new MatchingResult(matchedDatasetTerms.size(), datasetDomainTerms.size(), datasetTerms.size());
	}

	private static void writeSimilarityScoresToFile(List<Path> datasetOutputPaths, Path outputPath) throws IOException {
		if(outputExists(outputPath)) {
			return;
		}
		ZonedDateTime start = ZonedDateTime.now();
		Files.writeString(outputPath, "dataset_name,domain_representation_type,domain_representation_size,dataset_size,matched,overlap_coefficient\n", createAndAppend);
		for (Path datasetOutputPath : datasetOutputPaths) {
			Path datasetSimilarityScoresPath = datasetOutputPath.resolve(DATASET_SIMILARITY_SCORES_CSV_NAME);
			if(!inputExists(datasetSimilarityScoresPath)) {
				continue;
			}
			String similarityScore = Files.readAllLines(datasetSimilarityScoresPath).stream()
					.filter(line -> line.startsWith("precision,"))
					.findAny()
					.orElseThrow();
			Files.writeString(outputPath, datasetOutputPath.getFileName().toString() + "," + similarityScore + "\n", createAndAppend);
		}
		logDuration(start, "writing all-terms similarity scores to a single file");
	}

	private static void discoverDatasetsSimilarToDomain(Path similarityScoresPath, Path outputPath) throws IOException {
		if(outputExists(outputPath)) {
			return;
		}
		ZonedDateTime start = ZonedDateTime.now();
		List<DatasetSimilarity> sortedByDescendingSimilarityScore = readDatasetSimilarityScores(similarityScoresPath);
		Set<String> addedSimilarityScoreNames = addSimilarityScores(sortedByDescendingSimilarityScore);
		Collections.sort(sortedByDescendingSimilarityScore, Comparator.comparingDouble(DatasetSimilarity::getSimilarityScore).reversed());
		calculateConsecutiveDrops(sortedByDescendingSimilarityScore);
		List<SimilarDatasetsGroup> groupedByConsecutiveSteepestDrop = groupDatasetsByConsecutiveDrop(sortedByDescendingSimilarityScore);
		List<SimilarDatasetsGroup> selectedGroups = groupedByConsecutiveSteepestDrop.subList(0, groupedByConsecutiveSteepestDrop.size()-1);
		List<DatasetSimilarity> datasetsSimilarToDomain = selectedGroups.stream()
				.flatMap(group -> group.getSimilarDatasets().stream())
				.filter(datasetSimilarity -> !addedSimilarityScoreNames.contains(datasetSimilarity.getDatasetName()))
				.toList();
		String groupHeader = "dataset_name,similarity_score\n";
		Files.writeString(outputPath, groupHeader, createAndAppend);
		for(DatasetSimilarity dataset : datasetsSimilarToDomain) {
			String datasetOutput = String.format("%s,%f\n", dataset.getDatasetName(), dataset.getSimilarityScore());
			Files.writeString(outputPath, datasetOutput, createAndAppend);
		}
		logDuration(start, "determining datasets that are considered similar to the domain");
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

	private static List<SimilarDatasetsGroup> groupDatasetsByConsecutiveDrop(List<DatasetSimilarity> sortedByDescendingSimilarityScore) {
		List<SimilarDatasetsGroup> groupedByConsecutiveDrop = new ArrayList<>();
		List<DatasetSimilarity> validDropsSortedDescending = new ArrayList<>(sortedByDescendingSimilarityScore.subList(1, sortedByDescendingSimilarityScore.size()));
		Collections.sort(validDropsSortedDescending, Comparator.comparingDouble(DatasetSimilarity::getConsecutiveDrop).reversed());
		double secondHighestDrop = validDropsSortedDescending.get(1).getConsecutiveDrop();
		double secondLowestDrop = validDropsSortedDescending.get(validDropsSortedDescending.size()-2).getConsecutiveDrop();
		double dropThreshold = (secondHighestDrop + secondLowestDrop)/2d;
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

	private static void ensureOutputPathExists(Path outputPath) {
		File outputFile = outputPath.toFile();
		if(!outputFile.exists()) {
			outputFile.mkdirs();
		}
	}

	private static void logDuration(ZonedDateTime startTime, String taskDescription) throws IOException {
		if(!LOG_DURATION) {
			return;
		}
		ZonedDateTime endTime = ZonedDateTime.now();
		Duration duration = Duration.between(startTime, endTime);
		Files.writeString(EVALUATION_TASK_DURATIONS_CSV, duration.toString()+ ",\"" + taskDescription + "\"\n", createAndAppend);
	}

	private static void generateColumnFiles(Path dataFolder, Path outputPath) {
		// ----------------------------------------------------------------
	    // GENERATE COLUMN FILES
	    // ----------------------------------------------------------------
	    try {
	        Path outputColumnsPath = outputPath.resolve("columns");
	        if(outputExists(outputColumnsPath)) {
				return;
			}
			new D4().columns(
	                dataFolder.toFile(),
	                outputPath.resolve("columns.tsv").toFile(),
	                1000,
	                6,
	                true,
	                outputColumnsPath.toFile()
	        );
	    } catch (java.lang.InterruptedException | java.io.IOException ex) {
	        LOGGER.log(Level.SEVERE, "Generating columns failed with exception: ", ex);
	        System.exit(-1);
	    }
	}

	private static void generateTermIndex(Path outputPath) {
		// ----------------------------------------------------------------
	    // GENERATE TERM INDEX
	    // ----------------------------------------------------------------
	    try {
	        Path outputTermIndex = outputPath.resolve("term-index.txt.gz");
	        if(outputExists(outputTermIndex)) {
	        	return;
	        }

			new D4().termIndex(
	                outputPath.resolve("columns").toFile(),
	                Threshold.getConstraint("GT0.5"),
	                10000000,
	                false,
	                6,
	                true,
	                outputTermIndex.toFile()
	        );
	    } catch (java.lang.InterruptedException | java.io.IOException ex) {
	        LOGGER.log(Level.SEVERE, "Generating term index failed with exception: ", ex);
	        System.exit(-1);
	    }
	}

	private static void generateEquivalenceClasses(Path outputPath) {
		// ----------------------------------------------------------------
	    // GENERATE EQUIVALENCE CLASSES
	    // ----------------------------------------------------------------
	    try {
	        Path outputEquivalenceClasses = outputPath.resolve("compressed-term-index.txt.gz");
	        if(outputExists(outputEquivalenceClasses)) {
	        	return;
	        }
			new D4().eqs(
	                outputPath.resolve("term-index.txt.gz").toFile(),
	                true,
	                outputEquivalenceClasses.toFile()
	        );
	    } catch (java.io.IOException ex) {
	        LOGGER.log(Level.SEVERE, "Generating equivalence classes failed with exception: ", ex);
	        System.exit(-1);
	    }
	}

	private static void computeSignatures(Path outputPath, String simAlgo, String robustifier) {
		// ----------------------------------------------------------------
	    // COMPUTE SIGNATURES
	    // ----------------------------------------------------------------
	    try {
	        Path outputSignatures = outputPath.resolve("signatures.txt.gz");
	        if(outputExists(outputSignatures)) {
	        	return;
	        }
			new D4().signatures(
	                outputPath.resolve("compressed-term-index.txt.gz").toFile(),
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
	    } catch (java.lang.InterruptedException | java.io.IOException ex) {
	        LOGGER.log(Level.SEVERE, "Computing signatures failed with exception: ", ex);
	        System.exit(-1);
	    }
	}

	private static void expandColumns(Path outputPath, String trimmer) {
		// ----------------------------------------------------------------
	    // EXPAND COLUMNS
	    // ----------------------------------------------------------------
	    try {
	        Path outputExpandColumns = outputPath.resolve("expanded-columns.txt.gz");
	        if(outputExists(outputExpandColumns)) {
	        	return;
	        }
			new D4().expandColumns(
	                outputPath.resolve("compressed-term-index.txt.gz").toFile(),
	                outputPath.resolve("signatures.txt.gz").toFile(),
	                trimmer,
	                Threshold.getConstraint("GT0.25"),
	                5,
	                new BigDecimal("0.05"),
	                6,
	                true,
	                new TelemetryPrinter(),
	                outputExpandColumns.toFile()
	        );
	    } catch (java.io.IOException ex) {
	        LOGGER.log(Level.SEVERE, "Expanding columns failed with exception: ", ex);
	        System.exit(-1);
	    }
	}

	private static void noExpandColumns(Path outputPath) {
		// ----------------------------------------------------------------
	    // EXPAND COLUMNS
	    // ----------------------------------------------------------------
	    try {
	        Path outputNoExpandColumns = outputPath.resolve("expanded-columns.txt.gz");
	        if(outputExists(outputNoExpandColumns)) {
	        	return;
	        }
			new D4().writeColumns(
	                outputPath.resolve("compressed-term-index.txt.gz").toFile(),
	                true,
	                outputNoExpandColumns.toFile()
	        );
	    } catch (java.io.IOException ex) {
	        LOGGER.log(Level.SEVERE, "Non-expanding columns failed with exception: ", ex);
	        System.exit(-1);
	    }
	}

	private static void discoverLocalDomains(Path outputPath) {
		// ----------------------------------------------------------------
	    // DISCOVER LOCAL DOMAINS
	    // ----------------------------------------------------------------
	    try {
	        Path outputLocalDomains = outputPath.resolve("local-domains.txt.gz");
	        if(outputExists(outputLocalDomains)) {
	        	return;
	        }
			new D4().localDomains(
	                outputPath.resolve("compressed-term-index.txt.gz").toFile(),
	                outputPath.resolve("expanded-columns.txt.gz").toFile(),
	                outputPath.resolve("signatures.txt.gz").toFile(),
	                D4Config.TRIMMER_CONSERVATIVE,
	                false,
	                6,
	                false,
	                true,
	                new TelemetryPrinter(),
	                outputLocalDomains.toFile()
	        );
	    } catch (java.io.IOException ex) {
	        LOGGER.log(Level.SEVERE, "Discovering local domains failed with exception: ", ex);
	        System.exit(-1);
	    }
	}

	private static void pruneToStrongDomains(Path outputPath) {
		// ----------------------------------------------------------------
	    // PRUNE STRONG DOMAINS
	    // ----------------------------------------------------------------
	    try {
	        Path outputStrongDomains = outputPath.resolve("strong-domains.txt.gz");
	        if(outputExists(outputStrongDomains)) {
	        	return;
	        }
			new D4().strongDomains(
	                outputPath.resolve("compressed-term-index.txt.gz").toFile(),
	                outputPath.resolve("local-domains.txt.gz").toFile(),
	                Threshold.getConstraint("GT0.5"),
	                Threshold.getConstraint("GT0.1"),
	                new BigDecimal("0.25"),
	                6,
	                true,
	                new TelemetryPrinter(),
	                outputStrongDomains.toFile()
	        );
	    } catch (java.lang.InterruptedException | java.io.IOException ex) {
	        LOGGER.log(Level.SEVERE, "Pruning strong domains failed with exception: ", ex);
	        System.exit(-1);
	    }
	}

	private static void exportStrongDomains(Path outputPath) {
		// ----------------------------------------------------------------
	    // EXPORT
	    // ----------------------------------------------------------------
	    try {
	        Path outputExport = outputPath.resolve(COLUMN_DOMAINS_DIR_NAME);
	        if(outputExists(outputExport)) {
	        	return;
	        }
			new D4().exportStrongDomains(
	                outputPath.resolve("compressed-term-index.txt.gz").toFile(),
	                outputPath.resolve("term-index.txt.gz").toFile(),
	                outputPath.resolve("columns.tsv").toFile(),
	                outputPath.resolve("strong-domains.txt.gz").toFile(),
	                100,
	                true,
	                outputExport.toFile()
	        );
	    } catch (java.io.IOException ex) {
	        LOGGER.log(Level.SEVERE, "Exporting strong domains failed with exception: ", ex);
	        System.exit(-1);
	    }
	}
}
