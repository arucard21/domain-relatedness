package domain.similarity;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
	public static final String DOMAIN_REPRESENTATION_INPUT_DATA = "domain-data";
	public static final String DOMAIN_REPRESENTATION_OUTPUT_PATH = "output/domain-representation";
	public static final String DOMAIN_SIMILARITY_OUTPUT_PATH = "output/domain-similarity";
	public static final String DOMAIN_REPRESENTATION_PRECISION_OPTIMIZED = "precision";
	public static final String DOMAIN_REPRESENTATION_PRECISION_OPTIMIZED_TF = "precision-tf";
	public static final String DOMAIN_REPRESENTATION_ACCURACY_OPTIMIZED = "accuracy";
	public static final String DOMAIN_REPRESENTATION_RECALL_OPTIMIZED = "recall";
	public static final String DOMAIN_REPRESENTATION_RECALL_OPTIMIZED_TF = "recall-tf";
	public static final String DOMAIN_REPRESENTATION_ALL_TERMS = "all-terms";
	public static final String DOMAIN_REPRESENTATION_ALL_TERMS_TF = "all-terms-tf";
	public static final String DOMAIN_REPRESENTATION_NO_EXPAND = "no-expand";
	public static final String DOMAIN_REPRESENTATION_NO_EXPAND_TF = "no-expand-tf";
	public static final String DOMAIN_REPRESENTATION_MANUAL = "manual";
	public static final String DATASET_DOMAIN_FOLDER = "dataset-domain";
	public static final String JACCARD_INDEX = D4Config.EQSIM_JI;
	public static final String TERM_FREQUENCY_BASED_JACCARD = D4Config.EQSIM_TFICF;
	public static final String ROBUSTIFIER_LIBERAL = D4Config.ROBUST_LIBERAL;
	public static final String ROBUSTIFIER_IGNORE_LAST = D4Config.ROBUST_IGNORELAST;
	public static final String TRIMMER_CONSERVATIVE = D4Config.TRIMMER_CONSERVATIVE;
	public static final String TRIMMER_LIBERAL = D4Config.TRIMMER_LIBERAL;
	public static final String TRIMMER_CENTRIST = D4Config.TRIMMER_CENTRIST;
	public static final Path TASK_DURATIONS_FILE = Paths.get("task_durations.csv");
	public static final OpenOption[] createAndAppend = new OpenOption[]{StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.APPEND};
	public static final Logger LOGGER = Logger.getLogger(D4.class.getName());
	public static final List<String> TARGET_DATASETS = List.of(
//			"imdb",
//			"tmdb",
			"rt",
			"indian",
			"movies",
			"netflix",
			"tmdb-350k",
			"steam",
			"datasets.data-cityofnewyork-us.education",
			"datasets.data-cityofnewyork-us.finance",
			"datasets.data-cityofnewyork-us.services");

	public static void main(String[] args) throws IOException {
//		copyManualDomainRepresentation();

    	generateDomainRepresentation(DOMAIN_REPRESENTATION_PRECISION_OPTIMIZED, 	JACCARD_INDEX, 					ROBUSTIFIER_LIBERAL, 		TRIMMER_CONSERVATIVE, 	false, 	false);
    	generateDomainRepresentation(DOMAIN_REPRESENTATION_PRECISION_OPTIMIZED_TF, 	TERM_FREQUENCY_BASED_JACCARD, 	ROBUSTIFIER_LIBERAL, 		TRIMMER_CONSERVATIVE, 	false, 	false);

    	generateDomainRepresentation(DOMAIN_REPRESENTATION_ACCURACY_OPTIMIZED, 		JACCARD_INDEX, 					ROBUSTIFIER_LIBERAL, 		TRIMMER_CENTRIST, 		false, 	true);

//    	generateDomainRepresentation(DOMAIN_REPRESENTATION_RECALL_OPTIMIZED, 		JACCARD_INDEX, 					ROBUSTIFIER_IGNORE_LAST, 	TRIMMER_LIBERAL, 		false, 	false);
//    	generateDomainRepresentation(DOMAIN_REPRESENTATION_RECALL_OPTIMIZED_TF, 	TERM_FREQUENCY_BASED_JACCARD, 	ROBUSTIFIER_IGNORE_LAST, 	TRIMMER_LIBERAL, 		false, 	false);

    	generateDomainRepresentation(DOMAIN_REPRESENTATION_NO_EXPAND, 				JACCARD_INDEX, 					ROBUSTIFIER_LIBERAL, 		TRIMMER_CONSERVATIVE, 	true, 	false);
//    	generateDomainRepresentation(DOMAIN_REPRESENTATION_NO_EXPAND_TF, 			TERM_FREQUENCY_BASED_JACCARD, 	ROBUSTIFIER_LIBERAL, 		TRIMMER_CONSERVATIVE, 	true, 	false);

    	generateDomainRepresentation(DOMAIN_REPRESENTATION_ALL_TERMS, 				JACCARD_INDEX, 					ROBUSTIFIER_LIBERAL, 		TRIMMER_CONSERVATIVE, 	false, 	true);
//    	generateDomainRepresentation(DOMAIN_REPRESENTATION_ALL_TERMS_TF, 			TERM_FREQUENCY_BASED_JACCARD, 	ROBUSTIFIER_LIBERAL, 		TRIMMER_CONSERVATIVE, 	false, 	true);

    	for(String targetDataset: TARGET_DATASETS) {
    		calculateSimilarityToTargetDataset(targetDataset);
    	}
    	calculateOverlapCoefficientOfTargetDatasets();

    	writeAllTermsScoreToFile(TARGET_DATASETS);
    	discoverDatasetsSimilarToDomain(TARGET_DATASETS);
    }

//	private static void copyManualDomainRepresentation() throws IOException {
//		String movieTitlesColumnDomain = "dataset-domain/1.json";
//		Path source = Paths.get(DOMAIN_REPRESENTATION_MANUAL, movieTitlesColumnDomain);
//		Path target = Paths.get(DOMAIN_REPRESENTATION_OUTPUT_PATH, DOMAIN_REPRESENTATION_MANUAL, movieTitlesColumnDomain);
//		if(target.toFile().exists()) {
//			return;
//		}
//		target.toFile().getParentFile().mkdirs();
//		Files.copy(source, target);
//	}

	private static void generateDomainRepresentation(String outputFolder, String simAlgo, String robustifier, String trimmer, boolean noExpand, boolean allTerms) throws IOException {
		generateColumnDomains(outputFolder, simAlgo, robustifier, trimmer, noExpand);
		generateDatasetDomain(outputFolder, allTerms);
	}

	private static void generateColumnDomains(String outputFolder, String simAlgo, String robustifier, String trimmer, boolean noExpand) throws IOException {
		ZonedDateTime start = ZonedDateTime.now();
		File outputPath = createOutputPath(DOMAIN_REPRESENTATION_OUTPUT_PATH, outputFolder);
		generateColumnFiles(DOMAIN_REPRESENTATION_INPUT_DATA, outputPath);
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

	private static void generateDatasetDomain(String outputFolder, boolean allTerms) throws IOException {
		ZonedDateTime start = ZonedDateTime.now();
		// read exported JSON and output new JSON that only keeps the first block
		File outputPath = createOutputPath(DOMAIN_REPRESENTATION_OUTPUT_PATH, outputFolder);
		File datasetDomainPath = new File(outputPath, DATASET_DOMAIN_FOLDER);
		if(datasetDomainPath.exists()) {
			System.out.println(datasetDomainPath.getName() +" already exists. Skipping this step.");
	    	return;
		}
		datasetDomainPath.mkdirs();
		File exportedDomainsFolder = new File(outputPath, "domains");
		if(!exportedDomainsFolder.exists()) {
			System.err.println(exportedDomainsFolder.getName() + " does not exist. Cannot generate dataset domain without this.");
			return;
		}
		for (File strongDomainFile : exportedDomainsFolder.listFiles()) {
	    	try(Reader reader = new FileReader(strongDomainFile, StandardCharsets.UTF_8)){
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
		    	File columnDomainFile = new File(datasetDomainPath, strongDomainFile.getName());
		    	try(Writer writer = new FileWriter(columnDomainFile, StandardCharsets.UTF_8)){
			    	new GsonBuilder()
					.setPrettyPrinting()
					.create()
					.toJson(columnDomain, writer);
		    	}
	    	}
		}
		logDuration(start, "generating dataset domain");
	}

	private static void calculateSimilarityToTargetDataset(String datasetFolderName) throws IOException {
		generateTargetDatasetTermIndex(datasetFolderName);
		calculateSimilarityToTargetDatasetForDomainRepresentation(datasetFolderName, DOMAIN_REPRESENTATION_PRECISION_OPTIMIZED);
		calculateSimilarityToTargetDatasetForDomainRepresentation(datasetFolderName, DOMAIN_REPRESENTATION_PRECISION_OPTIMIZED_TF);
		calculateSimilarityToTargetDatasetForDomainRepresentation(datasetFolderName, DOMAIN_REPRESENTATION_ACCURACY_OPTIMIZED);
//		calculateSimilarityToTargetDatasetForDomainRepresentation(datasetFolderName, DOMAIN_REPRESENTATION_RECALL_OPTIMIZED);
//		calculateSimilarityToTargetDatasetForDomainRepresentation(datasetFolderName, DOMAIN_REPRESENTATION_RECALL_OPTIMIZED_TF);
		calculateSimilarityToTargetDatasetForDomainRepresentation(datasetFolderName, DOMAIN_REPRESENTATION_NO_EXPAND);
//		calculateSimilarityToTargetDatasetForDomainRepresentation(datasetFolderName, DOMAIN_REPRESENTATION_NO_EXPAND_TF);
		calculateSimilarityToTargetDatasetForDomainRepresentation(datasetFolderName, DOMAIN_REPRESENTATION_ALL_TERMS);
//		calculateSimilarityToTargetDatasetForDomainRepresentation(datasetFolderName, DOMAIN_REPRESENTATION_ALL_TERMS_TF);
//		calculateSimilarityToTargetDatasetForDomainRepresentation(datasetFolderName, DOMAIN_REPRESENTATION_MANUAL);
	}

	private static void generateTargetDatasetTermIndex(String datasetFolderName) throws IOException {
		ZonedDateTime start = ZonedDateTime.now();
		File outputPath = createOutputPath(DOMAIN_SIMILARITY_OUTPUT_PATH, datasetFolderName);
		generateColumnFiles(datasetFolderName, outputPath);
		generateTermIndex(outputPath);
		logDuration(start, "generating term index for " + datasetFolderName);
	}

	private static void calculateSimilarityToTargetDatasetForDomainRepresentation(String datasetFolderName, String domainRepresentationFolderName) throws IOException {
		ZonedDateTime start = ZonedDateTime.now();
		File outputPath = createOutputPath(DOMAIN_SIMILARITY_OUTPUT_PATH, datasetFolderName);
		Path matchingResultFile = Paths.get(outputPath.getPath(), String.format("matching_result_%s_%s.txt", datasetFolderName, domainRepresentationFolderName));
		Path matchingResultCsv = Paths.get(outputPath.getPath(), String.format("%s.csv", datasetFolderName));
		if(!matchingResultFile.toFile().exists()) {
			MatchingResult matchingResult = calculateTargetDatasetMatchedTerms(
					datasetFolderName,
					new File(
							createOutputPath(
									DOMAIN_REPRESENTATION_OUTPUT_PATH,
									domainRepresentationFolderName),
							DATASET_DOMAIN_FOLDER));
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
					domainRepresentationFolderName,
					matchingResult.getDomainRepresentationSize(),
					matchingResult.getDatasetSize(),
					matchingResult.getMatched(),
					overlapCoefficient);
			Files.writeString(matchingResultCsv, csvOutput, createAndAppend);
		}
		else {
			System.out.println(matchingResultFile.toFile().getName() +" already exists. Skipping this step.");
		}
		logDuration(start, "calculating similarity score for " + datasetFolderName + " using the " + domainRepresentationFolderName + " domain representation");
	}

	private static MatchingResult calculateTargetDatasetMatchedTerms(String datasetFolderName, File domainRepresentationPath) throws JsonSyntaxException, JsonIOException, IOException {
		File outputPath = createOutputPath(DOMAIN_SIMILARITY_OUTPUT_PATH, datasetFolderName);
		List<ColumnDomain> datasetDomain = new ArrayList<>();
		for(File columnDomainFile : domainRepresentationPath.listFiles()) {
			try(Reader reader = new FileReader(columnDomainFile, StandardCharsets.UTF_8)){
				datasetDomain.add(new GsonBuilder()
						.create()
						.fromJson(reader, ColumnDomain.class));
			}
		}
		Set<String> datasetDomainTerms = datasetDomain.stream()
				.flatMap(columnDomain -> columnDomain.getTerms().stream())
				.collect(Collectors.toSet());
		if(datasetDomainTerms.size() == 0) {
			String error = "The domain representation did not contain any terms. The similarity cannot be calculated.";
			System.err.println(error);
			throw new IllegalStateException(error);
		}
		File termIndexFile = new File(outputPath, "term-index.txt.gz");
		Set<String> datasetTerms = readTermsFromIndexFile(termIndexFile);
		Set<String> matchedDatasetTerms = datasetTerms.stream()
				.filter(datasetTerm -> datasetDomainTerms.contains(datasetTerm))
				.collect(Collectors.toSet());

		File matchedTermsFile = new File(outputPath, "matched-terms/"+domainRepresentationPath.getName()+".txt");
		matchedTermsFile.getParentFile().mkdirs();
		try(OutputStream out = Files.newOutputStream(matchedTermsFile.toPath(), createAndAppend)){
			for(String matchedTerm: datasetTerms) {
				out.write((matchedTerm + "\n").getBytes(StandardCharsets.UTF_8));
			}
		}
		return new MatchingResult(matchedDatasetTerms.size(), datasetDomainTerms.size(), datasetTerms.size());
	}

	private static void calculateOverlapCoefficientOfTargetDatasets() throws IOException {
		File datasetsDirectOverlapCoefficientCsv = new File("datasets_direct_overlap_coefficient.csv");
		if(datasetsDirectOverlapCoefficientCsv.exists()) {
			System.out.println(datasetsDirectOverlapCoefficientCsv.getName() +" already exists. Skipping this step.");
			return;
		}
		else {
			Files.writeString(datasetsDirectOverlapCoefficientCsv.toPath(), ","+String.join(",", TARGET_DATASETS)+"\n", createAndAppend);
		}
		Map<String, Set<String>> termsPerDataset = new HashMap<>();
		for(int i = 0; i < TARGET_DATASETS.size(); i++) {
			ZonedDateTime start = ZonedDateTime.now();
			String targetDataset = TARGET_DATASETS.get(i);
			File outputPath = createOutputPath(DOMAIN_SIMILARITY_OUTPUT_PATH, targetDataset);
			File termIndexFile = new File(outputPath, "term-index.txt.gz");
			if(!termsPerDataset.containsKey(targetDataset)) {
				termsPerDataset.put(targetDataset, readTermsFromIndexFile(termIndexFile));
			}
			Files.writeString(datasetsDirectOverlapCoefficientCsv.toPath(), targetDataset, createAndAppend);
			for(int j = 0; j < TARGET_DATASETS.size(); j++) {
				if(j < i+1) {
					Files.writeString(datasetsDirectOverlapCoefficientCsv.toPath(), ",", createAndAppend);
					continue;
				}
				String comparisonDataset = TARGET_DATASETS.get(j);
				File comparisonOutputPath = createOutputPath(DOMAIN_SIMILARITY_OUTPUT_PATH, comparisonDataset);
				File comparisonTermIndexFile = new File(comparisonOutputPath, "term-index.txt.gz");
				if(!termsPerDataset.containsKey(comparisonDataset)) {
					termsPerDataset.put(comparisonDataset, readTermsFromIndexFile(comparisonTermIndexFile));
				}
				Set<String> matchedDatasetTerms = termsPerDataset.get(comparisonDataset).stream()
						.filter(datasetTerm -> termsPerDataset.get(targetDataset).contains(datasetTerm))
						.collect(Collectors.toSet());
				double overlapCoefficient = (matchedDatasetTerms.size() * 1d) / (Math.min(termsPerDataset.get(targetDataset).size(), termsPerDataset.get(comparisonDataset).size()));
				Files.writeString(datasetsDirectOverlapCoefficientCsv.toPath(), ","+Double.toString(overlapCoefficient), createAndAppend);
			}
			Files.writeString(datasetsDirectOverlapCoefficientCsv.toPath(), "\n", createAndAppend);
			logDuration(start, "calculating direct overlap coefficient for dataset: " + targetDataset);
		}
	}

	private static void writeAllTermsScoreToFile(List<String> targetDatasets) throws IOException {
		File similarityScoresFile = new File("similarity_scores.csv");
		if(similarityScoresFile.exists()) {
			System.out.println(similarityScoresFile.getName() +" already exists. Skipping this step.");
			return;
		}
		ZonedDateTime start = ZonedDateTime.now();
		Files.writeString(similarityScoresFile.toPath(), "dataset_name,domain_representation_type,domain_representation_size,dataset_size,matched,overlap_coefficient\n", createAndAppend);
		for (String datasetFolderName : targetDatasets) {
			File outputPath = createOutputPath(DOMAIN_SIMILARITY_OUTPUT_PATH, datasetFolderName);
			File allScoresFile = new File(outputPath, datasetFolderName+".csv");
			String allTermsScore = Files.readAllLines(allScoresFile.toPath()).stream()
					.filter(line -> line.startsWith("all-terms,"))
					.findAny()
					.orElseThrow();
			Files.writeString(similarityScoresFile.toPath(), datasetFolderName + "," + allTermsScore + "\n", createAndAppend);
		}
		logDuration(start, "writing all-terms similarity scores to a single file");
	}

	private static void discoverDatasetsSimilarToDomain(List<String> targetDatasets) throws IOException {
		File similarDatasetsFile = new File("datasets_similar_to_domain.csv");
		if(similarDatasetsFile.exists()) {
			System.out.println(similarDatasetsFile.getName() +" already exists. Skipping this step.");
			return;
		}
		ZonedDateTime start = ZonedDateTime.now();
		List<DatasetSimilarity> sortedByDescendingSimilarityScore = readDatasetsAndSortByDescendingSimilarityScore();
		calculateConsecutiveDrops(sortedByDescendingSimilarityScore);
		List<SimilarDatasetsGroup> groupedByConsecutiveSteepestDrop = groupDatasetsByConsecutiveDrop(sortedByDescendingSimilarityScore);
		List<DatasetSimilarity> datasetsSimilarToDomain = groupedByConsecutiveSteepestDrop.subList(0, groupedByConsecutiveSteepestDrop.size()-1).stream()
				.flatMap(group -> group.getSimilarDatasets().stream())
				.toList();

		String groupHeader = "dataset_name,similarity_score\n";
		Files.writeString(similarDatasetsFile.toPath(), groupHeader, createAndAppend);
		for(DatasetSimilarity dataset : datasetsSimilarToDomain) {
			String datasetOutput = String.format("%s,%f\n", dataset.getDatasetName(), dataset.getSimilarityScore());
			Files.writeString(similarDatasetsFile.toPath(), datasetOutput, createAndAppend);
		}
		logDuration(start, "determining datasets that are considered similar to the domain");
	}

	private static List<DatasetSimilarity> readDatasetsAndSortByDescendingSimilarityScore() throws IOException {
		Path similarityScoresPath = Paths.get("similarity_scores.csv");
		List<DatasetSimilarity> sortedByDescendingSimilarityScore = new ArrayList<>();

		List<String> allLines = Files.readAllLines(similarityScoresPath);
		allLines.remove(0); // remove header line
		allLines.forEach(line -> {
			String[] values = line.split(",");
			String dataset = values[0];
			double score = Double.valueOf(values[values.length-1]);
			sortedByDescendingSimilarityScore.add(new DatasetSimilarity(dataset, score));
		});
		Collections.sort(sortedByDescendingSimilarityScore, Comparator.comparingDouble(DatasetSimilarity::getSimilarityScore).reversed());
		return sortedByDescendingSimilarityScore;
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

	private static Set<String> readTermsFromIndexFile(File termIndexFile) throws IOException{
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
		new TermIndexReader(termIndexFile).read(consumer);
		return datasetTerms;
	}

	private static File createOutputPath(String outputParent, String outputFolder) {
		File outputPath = new File(outputParent, outputFolder);
		if(!outputPath.exists()) {
			outputPath.mkdirs();
		}
		return outputPath;
	}

	private static void logDuration(ZonedDateTime startTime, String taskDescription) throws IOException {
		ZonedDateTime endTime = ZonedDateTime.now();
		Duration duration = Duration.between(startTime, endTime);
		Files.writeString(TASK_DURATIONS_FILE, duration.toString()+ ",\"" + taskDescription + "\"\n", createAndAppend);

	}

	private static void generateColumnFiles(String dataFolder, File outputPath) {
		// ----------------------------------------------------------------
	    // GENERATE COLUMN FILES
	    // ----------------------------------------------------------------
	    try {
	        File outputColumnFiles = new File(outputPath, "columns");
	        if(outputColumnFiles.exists()) {
	        	System.out.println(outputColumnFiles.getName() +" already exists. Skipping this step.");
	        	return;
	        }
			new D4().columns(
	                new File("input", dataFolder),
	                new File(outputPath, "columns.tsv"),
	                1000,
	                6,
	                true,
	                false,
	                outputColumnFiles
	        );
	    } catch (java.lang.InterruptedException | java.io.IOException ex) {
	        LOGGER.log(Level.SEVERE, "Generating columns failed with exception: ", ex);
	        System.exit(-1);
	    }
	}

	private static void generateTermIndex(File outputPath) {
		// ----------------------------------------------------------------
	    // GENERATE TERM INDEX
	    // ----------------------------------------------------------------
	    try {
	        File outputTermIndex = new File(outputPath, "term-index.txt.gz");
	        if(outputTermIndex.exists()) {
	        	System.out.println(outputTermIndex.getName() +" already exists. Skipping this step.");
	        	return;
	        }
			new D4().termIndex(
	                new File(outputPath, "columns"),
	                Threshold.getConstraint("GT0.5"),
	                10000000,
	                false,
	                6,
	                true,
	                outputTermIndex
	        );
	    } catch (java.lang.InterruptedException | java.io.IOException ex) {
	        LOGGER.log(Level.SEVERE, "Generating term index failed with exception: ", ex);
	        System.exit(-1);
	    }
	}

	private static void generateEquivalenceClasses(File outputPath) {
		// ----------------------------------------------------------------
	    // GENERATE EQUIVALENCE CLASSES
	    // ----------------------------------------------------------------
	    try {
	        File outputEquivalenceClasses = new File(outputPath, "compressed-term-index.txt.gz");
	        if(outputEquivalenceClasses.exists()) {
	        	System.out.println(outputEquivalenceClasses.getName() +" already exists. Skipping this step.");
	        	return;
	        }
			new D4().eqs(
	                new File(outputPath, "term-index.txt.gz"),
	                true,
	                outputEquivalenceClasses
	        );
	    } catch (java.io.IOException ex) {
	        LOGGER.log(Level.SEVERE, "Generating equivalence classes failed with exception: ", ex);
	        System.exit(-1);
	    }
	}

	private static void computeSignatures(File outputPath, String simAlgo, String robustifier) {
		// ----------------------------------------------------------------
	    // COMPUTE SIGNATURES
	    // ----------------------------------------------------------------
	    try {
	        File outputSignatures = new File(outputPath, "signatures.txt.gz");
	        if(outputSignatures.exists()) {
	        	System.out.println(outputSignatures.getName() +" already exists. Skipping this step.");
	        	return;
	        }
			new D4().signatures(
	                new File(outputPath, "compressed-term-index.txt.gz"),
	                simAlgo,
	                robustifier,
	                true,
	                false,
	                D4Config.ROBUST_IGNORELAST.equals(robustifier), // ignore minor drops only when ignore-last robustifier is used
	                6,
	                true,
	                new TelemetryPrinter(),
	                outputSignatures
	        );
	    } catch (java.lang.InterruptedException | java.io.IOException ex) {
	        LOGGER.log(Level.SEVERE, "Computing signatures failed with exception: ", ex);
	        System.exit(-1);
	    }
	}

	private static void expandColumns(File outputPath, String trimmer) {
		// ----------------------------------------------------------------
	    // EXPAND COLUMNS
	    // ----------------------------------------------------------------
	    try {
	        File outputExpandColumns = new File(outputPath, "expanded-columns.txt.gz");
	        if(outputExpandColumns.exists()) {
	        	System.out.println(outputExpandColumns.getName() +" already exists. Skipping this step.");
	        	return;
	        }
			new D4().expandColumns(
	                new File(outputPath, "compressed-term-index.txt.gz"),
	                new File(outputPath, "signatures.txt.gz"),
	                trimmer,
	                Threshold.getConstraint("GT0.25"),
	                5,
	                new BigDecimal("0.05"),
	                6,
	                true,
	                new TelemetryPrinter(),
	                outputExpandColumns
	        );
	    } catch (java.io.IOException ex) {
	        LOGGER.log(Level.SEVERE, "Expanding columns failed with exception: ", ex);
	        System.exit(-1);
	    }
	}

	private static void noExpandColumns(File outputPath) {
		// ----------------------------------------------------------------
	    // EXPAND COLUMNS
	    // ----------------------------------------------------------------
	    try {
	        File outputNoExpandColumns = new File(outputPath, "expanded-columns.txt.gz");
	        if(outputNoExpandColumns.exists()) {
	        	System.out.println(outputNoExpandColumns.getName() +" already exists. Skipping this step.");
	        	return;
	        }
			new D4().writeColumns(
	                new File(outputPath, "compressed-term-index.txt.gz"),
	                true,
	                outputNoExpandColumns
	        );
	    } catch (java.io.IOException ex) {
	        LOGGER.log(Level.SEVERE, "Non-expanding columns failed with exception: ", ex);
	        System.exit(-1);
	    }
	}

	private static void discoverLocalDomains(File outputPath) {
		// ----------------------------------------------------------------
	    // DISCOVER LOCAL DOMAINS
	    // ----------------------------------------------------------------
	    try {
	        File outputLocalDomains = new File(outputPath, "local-domains.txt.gz");
	        if(outputLocalDomains.exists()) {
	        	System.out.println(outputLocalDomains.getName() +" already exists. Skipping this step.");
	        	return;
	        }
			new D4().localDomains(
	                new File(outputPath, "compressed-term-index.txt.gz"),
	                new File(outputPath, "expanded-columns.txt.gz"),
	                new File(outputPath, "signatures.txt.gz"),
	                D4Config.TRIMMER_CONSERVATIVE,
	                false,
	                6,
	                false,
	                true,
	                new TelemetryPrinter(),
	                outputLocalDomains
	        );
	    } catch (java.io.IOException ex) {
	        LOGGER.log(Level.SEVERE, "Discovering local domains failed with exception: ", ex);
	        System.exit(-1);
	    }
	}

	private static void pruneToStrongDomains(File outputPath) {
		// ----------------------------------------------------------------
	    // PRUNE STRONG DOMAINS
	    // ----------------------------------------------------------------
	    try {
	        File outputStrongDomains = new File(outputPath, "strong-domains.txt.gz");
	        if(outputStrongDomains.exists()) {
	        	System.out.println(outputStrongDomains.getName() +" already exists. Skipping this step.");
	        	return;
	        }
			new D4().strongDomains(
	                new File(outputPath, "compressed-term-index.txt.gz"),
	                new File(outputPath, "local-domains.txt.gz"),
	                Threshold.getConstraint("GT0.5"),
	                Threshold.getConstraint("GT0.1"),
	                new BigDecimal("0.25"),
	                6,
	                true,
	                new TelemetryPrinter(),
	                outputStrongDomains
	        );
	    } catch (java.lang.InterruptedException | java.io.IOException ex) {
	        LOGGER.log(Level.SEVERE, "Pruning strong domains failed with exception: ", ex);
	        System.exit(-1);
	    }
	}

	private static void exportStrongDomains(File outputPath) {
		// ----------------------------------------------------------------
	    // EXPORT
	    // ----------------------------------------------------------------
	    try {
	        File outputExport = new File(outputPath, "domains");
	        if(outputExport.exists()) {
	        	System.out.println(outputExport.getName() +" already exists. Skipping this step.");
	        	return;
	        }
			new D4().exportStrongDomains(
	                new File(outputPath, "compressed-term-index.txt.gz"),
	                new File(outputPath, "term-index.txt.gz"),
	                new File(outputPath, "columns.tsv"),
	                new File(outputPath, "strong-domains.txt.gz"),
	                100,
	                true,
	                outputExport
	        );
	    } catch (java.io.IOException ex) {
	        LOGGER.log(Level.SEVERE, "Exporting strong domains failed with exception: ", ex);
	        System.exit(-1);
	    }
	}
}
