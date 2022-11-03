package com.github.arucard21.dataset_discovery;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import com.github.arucard21.dataset_discovery.objects.DatasetSimilarity;
import com.github.arucard21.dataset_discovery.objects.MatchingResult;
import com.github.arucard21.dataset_discovery.objects.SimilarDatasetsGroup;
import com.github.arucard21.dataset_domain_terms.objects.ColumnDomain;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonIOException;
import com.google.gson.JsonSyntaxException;

import org.opendata.core.constraint.Threshold;
import org.opendata.curation.d4.D4;
import org.opendata.db.term.Term;
import org.opendata.db.term.TermConsumer;
import org.opendata.db.term.TermIndexReader;

public class DatasetDiscovery {
	public static final String COLUMNS_DIR_NAME = "columns";
	public static final String COLUMNS_METADATA_FILE_NAME = "columns.tsv";
	public static final String TERM_INDEX_FILE_NAME = "term-index.txt.gz";
	public static final String DATASET_SIMILARITY_SCORES_CSV_NAME = "similarity_scores.csv";
	public static final String DATASET_DISCOVERY_SELECTED_VARIANT = "precision";
	public static final String DATASET_DISCOVERY_ALL_SIMILARITY_SCORES_CSV = "all_similarity_scores.csv";
	public static final String DATASET_DISCOVERY_OUTPUT_DATASETS_FOCUSED_ON_DOMAIN = "datasets_focused_on_domain.csv";
	public static final OpenOption[] CREATE_AND_APPEND = new OpenOption[]{StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.APPEND};

	/**
	 * Path to the directory containing dataset domain terms.
	 *
	 * Though this is a conceptually a single list, it is stored in separate JSON files in this directory.
	 * Each JSON file matches the column domain that those dataset domain terms were derived from.
	 */
	private final Path datasetDomainTermsDirectory;
	/**
	 * Path to the directory where the dataset discovery result is stored, along with any intermediate output that is generated.
	 */
	private final Path outputDirectory;
	/**
	 * A list of paths to directories, each of which contain a dataset that should be used as input for this dataset discovery technique.
	 */
	private final List<Path> inputDatasets;
	/**
	 * Determines whether the matched terms should be written to an output file.
	 *
	 * This may be resource-intensive so it should only be disabled when necessary.
	 */
	private boolean matchedTermsLogged = false;
	/**
	 * A list containing the similarity score for each dataset.
	 *
	 * This is calculated while datasets are discoverd so it is only available after discoverDatasetsFocusedOnSameDomain() is run.
	 */
	private List<DatasetSimilarity> datasetSimilarityScores;

	public DatasetDiscovery(Path datasetDomainTermsDirectory, Path outputDirectory, List<Path> inputDatasets) {
		this.datasetDomainTermsDirectory = datasetDomainTermsDirectory;
		this.outputDirectory = outputDirectory;
		this.inputDatasets = inputDatasets;
	}

	public Set<String> discoverDatasetsFocusedOnSameDomain() throws IOException {
		List<Path> datasetSimilarityScorePaths = new ArrayList<>();
		for(Path inputDataset: inputDatasets) {
			Path termIndexPath = generateTargetDatasetTermIndex(inputDataset);
			Path similarityScoresCsvPath = termIndexPath.getParent().resolve(DATASET_SIMILARITY_SCORES_CSV_NAME);
			MatchingResult matchingResult = calculateSimilarityToTargetDatasetForDomainRepresentation(termIndexPath);
			writeSingleSimilarityScoreToCsvFile(matchingResult, similarityScoresCsvPath);
			datasetSimilarityScorePaths.add(similarityScoresCsvPath);
    	}
		datasetSimilarityScores = combineDatasetSimilarityScoresInCsvFile(datasetSimilarityScorePaths);
		Set<String> similarDatasets = discoverDatasetsSimilarToDomain();
    	return similarDatasets;
	}

	private Path generateTargetDatasetTermIndex(Path inputDataset) throws IOException {
		Path outputForDatasetPath = convertDatasetInputPathToOutputPathSubdirectory(inputDataset, outputDirectory);
		ensureOutputDirExists(outputForDatasetPath);
		Path columnsPath = generateColumnFiles(inputDataset, outputForDatasetPath);
		Path termIndexPath = generateTermIndex(columnsPath);
		return termIndexPath;
	}

	private MatchingResult calculateSimilarityToTargetDatasetForDomainRepresentation(Path termIndexPath) throws IOException {
		if(!inputExists(termIndexPath)) {
			throw new IllegalArgumentException(String.format("The input dataset %s does not exist.", termIndexPath.toString()));
		}
		Path outputPath = termIndexPath.getParent();
		ensureOutputDirExists(outputPath);
		Path matchingResultPath = outputPath.resolve(String.format("matching_result_%s_%s.json", termIndexPath.getParent().getFileName().toString(), datasetDomainTermsDirectory.getParent().getFileName().toString()));
		if(outputExists(matchingResultPath)) {
			try(Reader reader = Files.newBufferedReader(matchingResultPath)){
				MatchingResult existingMatchingResult = new Gson().fromJson(reader, MatchingResult.class);
				return existingMatchingResult;
			}
		}
		MatchingResult matchingResult = countTargetDatasetMatchedTerms(termIndexPath);
		double overlapCoefficient = (matchingResult.getMatched() * 1d) / (Math.min(matchingResult.getDatasetSize(), matchingResult.getDomainRepresentationSize()));
		matchingResult.setSimilarityScore(overlapCoefficient);
		try(Writer writer = Files.newBufferedWriter(matchingResultPath)){
	    	new GsonBuilder().setPrettyPrinting().create().toJson(matchingResult, writer);
    	}
		return matchingResult;
	}

	private MatchingResult countTargetDatasetMatchedTerms(Path termIndexPath) throws JsonSyntaxException, JsonIOException, IOException {
		if(!inputExists(termIndexPath) || !inputExists(datasetDomainTermsDirectory)) {
			throw new IllegalArgumentException();
		}
		Path outputPath = termIndexPath.getParent();
		ensureOutputDirExists(outputPath);

		List<ColumnDomain> datasetDomain = new ArrayList<>();
		Files.list(datasetDomainTermsDirectory).forEach(datasetDomainFile -> {
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

		if(matchedTermsLogged) {
			Path matchedTermsPath = outputPath
					.resolve("matched-terms.txt");
			for(String matchedTerm: matchedDatasetTerms) {
				Files.writeString(matchedTermsPath, matchedTerm+"\n", CREATE_AND_APPEND);
			}
			Files.writeString(matchedTermsPath, "\n", CREATE_AND_APPEND);
		}
		return new MatchingResult(matchedDatasetTerms.size(), datasetDomainTerms.size(), datasetTerms.size());
	}

	private Set<String> readTermsFromIndexFile(Path termIndexFile) throws IOException{
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

	private void writeSingleSimilarityScoreToCsvFile(MatchingResult matchingResult, Path similarityScoresCsvPath) throws IOException {
		if(similarityScoresCsvPath.toFile().exists()) {
			return;
		}
		writeSimilarityScoreToCsvFile(matchingResult, similarityScoresCsvPath);
	}

	private void writeSimilarityScoreToCsvFile(MatchingResult matchingResult, Path similarityScoresCsvPath) throws IOException {
		if(!similarityScoresCsvPath.toFile().exists()) {
			Files.writeString(similarityScoresCsvPath, "domain_representation_type,domain_representation_size,dataset_size,matched,overlap_coefficient\n", CREATE_AND_APPEND);
		}
		String csvOutput = String.format(
				"%s,%d,%d,%d,%f\n",
				DATASET_DISCOVERY_SELECTED_VARIANT,
				matchingResult.getDomainRepresentationSize(),
				matchingResult.getDatasetSize(),
				matchingResult.getMatched(),
				matchingResult.getSimilarityScore());
		Files.writeString(similarityScoresCsvPath, csvOutput, CREATE_AND_APPEND);
	}

	private List<DatasetSimilarity> combineDatasetSimilarityScoresInCsvFile(List<Path> datasetSimilarityScoresPaths) throws IOException {
		Path combinedSimilarityScoresPath = outputDirectory.resolve(DATASET_DISCOVERY_ALL_SIMILARITY_SCORES_CSV);
		if(outputExists(combinedSimilarityScoresPath)) {
			return readDatasetSimilarityScores(combinedSimilarityScoresPath);
		}
		else {
			List<DatasetSimilarity> datasetSimilarityScores = new ArrayList<>();
			Files.writeString(combinedSimilarityScoresPath, "dataset_name,domain_representation_type,domain_representation_size,dataset_size,matched,overlap_coefficient\n", CREATE_AND_APPEND);
			for (Path datasetSimilarityScoresPath : datasetSimilarityScoresPaths) {
				if(!inputExists(datasetSimilarityScoresPath)) {
					continue;
				}
				String datasetResult = Files.readAllLines(datasetSimilarityScoresPath).stream()
						.filter(line -> line.startsWith(DATASET_DISCOVERY_SELECTED_VARIANT+","))
						.findAny()
						.orElseThrow();
				String similarityScore = datasetResult.split(",")[4];
				String datasetName = datasetSimilarityScoresPath.getParent().getFileName().toString();
				datasetSimilarityScores.add(new DatasetSimilarity(datasetName, Double.valueOf(similarityScore)));
				Files.writeString(combinedSimilarityScoresPath, datasetName + "," + datasetResult + "\n", CREATE_AND_APPEND);
			}
			return datasetSimilarityScores;
		}
	}

	private List<DatasetSimilarity> readDatasetSimilarityScores(Path similarityScoresPath) throws IOException {
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

	private Set<String> discoverDatasetsSimilarToDomain() throws IOException {
		Path domainSimilarityResultPath = outputDirectory == null ? null : outputDirectory.resolve(DATASET_DISCOVERY_OUTPUT_DATASETS_FOCUSED_ON_DOMAIN);
		Set<DatasetSimilarity> similarDatasets;
		if(outputExists(domainSimilarityResultPath)) {
			similarDatasets = readSimilarDataset(domainSimilarityResultPath);
		}
		else {
			List<DatasetSimilarity> sortedByDescendingSimilarityScore = new ArrayList<>(datasetSimilarityScores);
			Set<String> addedSimilarityScoreNames = addSimilarityScores(sortedByDescendingSimilarityScore);
			Collections.sort(sortedByDescendingSimilarityScore, Comparator.comparingDouble(DatasetSimilarity::getSimilarityScore).reversed());
			calculateConsecutiveDrops(sortedByDescendingSimilarityScore);
			List<SimilarDatasetsGroup> groupedByConsecutiveSteepestDrop = groupDatasetsByConsecutiveDrop(sortedByDescendingSimilarityScore, outputDirectory);
			List<SimilarDatasetsGroup> selectedGroups = groupedByConsecutiveSteepestDrop.subList(0, groupedByConsecutiveSteepestDrop.size()-1);
			similarDatasets = selectedGroups.stream()
					.flatMap(group -> group.getSimilarDatasets().stream())
					.filter(datasetSimilarity -> !addedSimilarityScoreNames.contains(datasetSimilarity.getDatasetName()))
					.collect(Collectors.toSet());
			if(domainSimilarityResultPath != null) {
				String groupHeader = "dataset_name,similarity_score\n";
				Files.writeString(domainSimilarityResultPath, groupHeader, CREATE_AND_APPEND);
				for(DatasetSimilarity dataset : similarDatasets) {
					String datasetOutput = String.format("%s,%f\n", dataset.getDatasetName(), dataset.getSimilarityScore());
					Files.writeString(domainSimilarityResultPath, datasetOutput, CREATE_AND_APPEND);
				}
			}
		}
		return similarDatasets.stream().map(DatasetSimilarity::getDatasetName).collect(Collectors.toSet());
	}

	private Set<DatasetSimilarity> readSimilarDataset(Path similarDatasetsPath) throws IOException {
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

	private boolean inputExists(Path inputPath) {
		if(inputPath.toFile().exists()) {
			return true;
		}
		else {
			System.err.println(inputPath.toString() + " does not exist. Cannot continue without this.");
			return false;
		}
	}

	private boolean outputExists(Path outputPath) {
		if(outputPath != null && outputPath.toFile().exists()) {
			System.out.println(outputPath.toString() + " already exists. Skipping this step.");
			return true;
		}
		else {
			return false;
		}
	}

	private void ensureOutputDirExists(Path outputDirPath) {
		File outputFile = outputDirPath.toFile();
		if(!outputFile.exists()) {
			outputFile.mkdirs();
		}
	}

	private Path convertDatasetInputPathToOutputPathSubdirectory(Path inputDataset, Path outputDirPath) {
		return outputDirPath.resolve(inputDataset.getFileName());
	}

	private Path generateColumnFiles(Path inputPath, Path outputPath) {
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

	private Path generateTermIndex(Path columnsPath) {
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

	public boolean isMatchedTermsLogged() {
		return matchedTermsLogged;
	}

	public DatasetDiscovery matchedTermsLogged(boolean matchedTermsLogged) {
		this.matchedTermsLogged = matchedTermsLogged;
		return this;
	}

	public List<DatasetSimilarity> getDatasetSimilarityScores() {
		if(datasetSimilarityScores == null) {
			throw new IllegalStateException("Dataset discovery has not been performed yet. The similarity scores are only available after discoverDatasetsFocusedOnSameDomain() has been run.");
		}
		return datasetSimilarityScores;

	}
}
