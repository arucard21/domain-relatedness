package domain.similarity;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
	public static final String DOMAIN_REPRESENTATION_DEFAULT = "default";
	public static final String DOMAIN_REPRESENTATION_DEFAULT_TF = "default-tf";
	public static final String DOMAIN_REPRESENTATION_NO_INDEX = "no-index";
	public static final String DOMAIN_REPRESENTATION_NO_INDEX_TF = "no-index-tf";
	public static final String DOMAIN_REPRESENTATION_NO_EXPAND = "no-expand";
	public static final String DOMAIN_REPRESENTATION_NO_EXPAND_TF = "no-expand-tf";
	public static final String DOMAIN_REPRESENTATION_NO_INDEX_NO_EXPAND = "no-index-no-expand";
	public static final String DOMAIN_REPRESENTATION_NO_INDEX_NO_EXPAND_TF = "no-index-no-expand-tf";
	public static final String DOMAIN_REPRESENTATION_MANUAL = "manual";
	public static final String DATASET_DOMAIN_FOLDER = "dataset-domain";
	public static final String JACCARD_INDEX = D4Config.EQSIM_JI;
	public static final String TERM_FREQUENCY_BASED_JACCARD = D4Config.EQSIM_TFICF;
	private static final Logger LOGGER = Logger.getLogger(D4.class.getName());

	public static void main(String[] args) throws IOException {
    	generateDomainRepresentation(DOMAIN_REPRESENTATION_DEFAULT, JACCARD_INDEX, false, false);
    	generateDomainRepresentation(DOMAIN_REPRESENTATION_DEFAULT_TF, TERM_FREQUENCY_BASED_JACCARD, false, false);
    	generateDomainRepresentation(DOMAIN_REPRESENTATION_NO_INDEX, JACCARD_INDEX, true, false);
    	generateDomainRepresentation(DOMAIN_REPRESENTATION_NO_INDEX_TF, TERM_FREQUENCY_BASED_JACCARD, true, false);
    	generateDomainRepresentation(DOMAIN_REPRESENTATION_NO_EXPAND, JACCARD_INDEX, false, true);
    	generateDomainRepresentation(DOMAIN_REPRESENTATION_NO_EXPAND_TF, TERM_FREQUENCY_BASED_JACCARD, false, true);
    	generateDomainRepresentation(DOMAIN_REPRESENTATION_NO_INDEX_NO_EXPAND, JACCARD_INDEX, true, true);
    	generateDomainRepresentation(DOMAIN_REPRESENTATION_NO_INDEX_NO_EXPAND_TF, TERM_FREQUENCY_BASED_JACCARD, true, true);

    	calculateSimilarityToTargetDataset("imdb");
    	calculateSimilarityToTargetDataset("tmdb");
    	calculateSimilarityToTargetDataset("rt");
    	calculateSimilarityToTargetDataset("indian");
    	calculateSimilarityToTargetDataset("movies");
    	calculateSimilarityToTargetDataset("netflix");
    	calculateSimilarityToTargetDataset("tmdb-350k");
    	calculateSimilarityToTargetDataset("steam");
    	calculateSimilarityToTargetDataset("datasets.data-cityofnewyork-us.education");
    	calculateSimilarityToTargetDataset("datasets.data-cityofnewyork-us.finance");
    	calculateSimilarityToTargetDataset("datasets.data-cityofnewyork-us.services");
    }

	private static void generateDomainRepresentation(String outputFolder, String simAlgo, boolean deleteIndex, boolean noExpand) throws IOException {
		generateColumnDomains(outputFolder, simAlgo, deleteIndex, noExpand);
		generateDatasetDomain(outputFolder);
	}

	private static void generateColumnDomains(String outputFolder, String simAlgo, boolean deleteIndex, boolean noExpand) {
		File outputPath = createOutputPath(DOMAIN_REPRESENTATION_OUTPUT_PATH, outputFolder);
		generateColumnFiles(DOMAIN_REPRESENTATION_INPUT_DATA, outputPath, deleteIndex, false);
		generateTermIndex(outputPath);
		generateEquivalenceClasses(outputPath);
		computeSignatures(outputPath, simAlgo);
		if(noExpand) {
			noExpandColumns(outputPath);
		}
		else {
			expandColumns(outputPath);
		}
		discoverLocalDomains(outputPath);
		pruneStrongDomains(outputPath);
		exportStrongDomains(outputPath);
	}

	private static void generateDatasetDomain(String outputFolder) throws IOException {
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
		    	columnDomain.setTerms(strongDomain.getTerms().get(0).stream()
		    			.flatMap(eq -> eq.getTerms().stream())
		    			.collect(Collectors.toList()));
		    	File columnDomainFile = new File(datasetDomainPath, strongDomainFile.getName());
		    	try(Writer writer = new FileWriter(columnDomainFile, StandardCharsets.UTF_8)){
			    	new GsonBuilder()
					.setPrettyPrinting()
					.create()
					.toJson(columnDomain, writer);
		    	}
	    	}
		}
	}

	private static void calculateSimilarityToTargetDataset(String datasetFolderName) throws IOException {
		generateTargetDatasetTermIndex(datasetFolderName);
		calculateSimilarityToTargetDatasetForDomainRepresentation(datasetFolderName, DOMAIN_REPRESENTATION_DEFAULT);
		calculateSimilarityToTargetDatasetForDomainRepresentation(datasetFolderName, DOMAIN_REPRESENTATION_DEFAULT_TF);
		calculateSimilarityToTargetDatasetForDomainRepresentation(datasetFolderName, DOMAIN_REPRESENTATION_NO_INDEX);
		calculateSimilarityToTargetDatasetForDomainRepresentation(datasetFolderName, DOMAIN_REPRESENTATION_NO_INDEX_TF);
		calculateSimilarityToTargetDatasetForDomainRepresentation(datasetFolderName, DOMAIN_REPRESENTATION_NO_EXPAND);
		calculateSimilarityToTargetDatasetForDomainRepresentation(datasetFolderName, DOMAIN_REPRESENTATION_NO_EXPAND_TF);
		calculateSimilarityToTargetDatasetForDomainRepresentation(datasetFolderName, DOMAIN_REPRESENTATION_NO_INDEX_NO_EXPAND);
		calculateSimilarityToTargetDatasetForDomainRepresentation(datasetFolderName, DOMAIN_REPRESENTATION_NO_INDEX_NO_EXPAND_TF);
		calculateSimilarityToTargetDatasetForDomainRepresentation(datasetFolderName, DOMAIN_REPRESENTATION_MANUAL);
	}

	private static void generateTargetDatasetTermIndex(String datasetFolderName) {
		File outputPath = createOutputPath(DOMAIN_SIMILARITY_OUTPUT_PATH, datasetFolderName);
		generateColumnFiles(datasetFolderName, outputPath, false, false);
		generateTermIndex(outputPath);
	}

	private static void calculateSimilarityToTargetDatasetForDomainRepresentation(String datasetFolderName, String domainRepresentationFolderName) throws IOException {
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
			Files.writeString(matchingResultFile, matchingResultMessage, StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
			if(!matchingResultCsv.toFile().exists()) {
				Files.writeString(matchingResultCsv, "domain_representation_type,domain_representation_size,dataset_size,matched,overlap_coefficient\n", StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
			}
			double overlapCoefficient = (matchingResult.getMatched() * 1d) / (Math.min(matchingResult.getDatasetSize(), matchingResult.getDomainRepresentationSize()));
			String csvOutput = String.format(
					"%s,%d,%d,%d,%f\n",
					domainRepresentationFolderName,
					matchingResult.getDomainRepresentationSize(),
					matchingResult.getDatasetSize(),
					matchingResult.getMatched(),
					overlapCoefficient);
			Files.writeString(matchingResultCsv, csvOutput, StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
		}
		else {
			System.out.println(matchingResultFile.toFile().getName() +" already exists. Skipping this step.");
		}
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

		Set<String> matchedDatasetTerms = datasetTerms.stream()
				.filter(datasetTerm -> datasetDomainTerms.contains(datasetTerm))
				.collect(Collectors.toSet());

		return new MatchingResult(matchedDatasetTerms.size(), datasetDomainTerms.size(), datasetTerms.size());
	}

	private static File createOutputPath(String outputParent, String outputFolder) {
		File outputPath = new File(outputParent, outputFolder);
		if(!outputPath.exists()) {
			outputPath.mkdirs();
		}
		return outputPath;
	}

	private static void deleteIndexAndForeignKeyColumns(File columnsFolder) {
		/*
		 * Delete only the second "directors" file which should match the one containing
		 * id's from IMDb. The first one should be from Rotten Tomatoes and should contain
		 * the actual names of directors.
		 */
		File[] directorsColumnFiles = columnsFolder.listFiles(new FilenameFilter() {
			@Override
			public boolean accept(File dir, String name) {
				return name.contains("directors");
			}
		});
		if(!directorsColumnFiles[1].delete()) {
			System.err.println("Could not delete column file: " + directorsColumnFiles[1].getName());
		}

		List<String> indexAndForeignKeyColumns = List.of(
				"titleId",
				"tconst",
				"nconst",
				"parentTconst",
				"knownForTitles",
				"writers",
				"credit_id",
				"imdb_id",
				"rotten_tomatoes_link");
		Stream.of(columnsFolder.listFiles())
		.filter(file -> indexAndForeignKeyColumns.stream().anyMatch(idx -> file.getName().contains(idx)))
		.forEach(file -> file.delete());;
	}

	private static void generateColumnFiles(String dataFolder, File outputPath, boolean deleteIndex, boolean split) {
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
	                split,
	                outputColumnFiles
	        );
			if(deleteIndex) {
				deleteIndexAndForeignKeyColumns(outputColumnFiles);
			}
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

	private static void computeSignatures(File outputPath, String simAlgo) {
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
	                D4Config.ROBUST_LIBERAL,
	                true,
	                false,
	                false,
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

	private static void expandColumns(File outputPath) {
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
	                D4Config.TRIMMER_CONSERVATIVE,
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

	private static void pruneStrongDomains(File outputPath) {
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
