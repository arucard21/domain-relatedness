package domain.relatedness;

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

public class DomainRelatedness {
	public static final String INPUT_DOMAIN_REPRESENTATION_DATA_FOLDER_NAME = "domain-data";
	private static final Logger LOGGER = Logger.getLogger(D4.class.getName());

	/**
	 * First, this generates the domain representation using D4. It does this using
	 * the default Jaccard Index as well as the improved TF-IDF-inspired similarity
	 * function, called TF-ICF.
	 *
	 * Then this generates a term index for a target database, using the D4
	 * implementation. This is then used to calculate the relatedness of the generated
	 * domain representation to the target dataset by counting how many terms are
	 * present in both. The relatedness is the percentage of this count relative to
	 * the total amount of terms in the domain representation.
	 *
	 * @throws IOException
	 */
    public static void main(String[] args) throws IOException {
    	generateDomainRepresentation("default", D4Config.EQSIM_JI, false, false);
    	generateDomainRepresentation("default-tf", D4Config.EQSIM_TFICF, false, false);
    	generateDomainRepresentation("no-index", D4Config.EQSIM_JI, true, false);
    	generateDomainRepresentation("noindex-tf", D4Config.EQSIM_TFICF, true, false);
    	generateDomainRepresentation("no-expand", D4Config.EQSIM_JI, false, true);
    	generateDomainRepresentation("no-expand-tf", D4Config.EQSIM_TFICF, false, true);
    	generateDomainRepresentation("no-index-no-expand", D4Config.EQSIM_JI, true, true);
    	generateDomainRepresentation("no-index-no-expand-tf", D4Config.EQSIM_TFICF, true, true);

//    	generateDomainRepresentationImproved();
//    	generateDomainRepresentationNoIndex();
//    	generateDomainRepresentationNoIndexImproved();
//    	generateDomainRepresentationNoExpand();
//    	generateDomainRepresentationNoExpandImproved();
//    	generateDomainRepresentationNoIndexNoExpand();
//    	generateDomainRepresentationNoIndexNoExpandImproved();
//    	generateDomainRepresentationNoIndexNoExpandSplitTerms();
//    	generateDomainRepresentationNoIndexNoExpandSplitTermsImproved();

    	calculateRelatednesToTargetDataset("imdb");
    	calculateRelatednesToTargetDataset("tmdb");
    	calculateRelatednesToTargetDataset("rt");
    	calculateRelatednesToTargetDataset("indian");
    	calculateRelatednesToTargetDataset("movies");
    	calculateRelatednesToTargetDataset("netflix");
    	calculateRelatednesToTargetDataset("tmdb-350k");
    	calculateRelatednesToTargetDataset("datasets.data-cityofnewyork-us.education");
    	calculateRelatednesToTargetDataset("datasets.data-cityofnewyork-us.finance");
    	calculateRelatednesToTargetDataset("datasets.data-cityofnewyork-us.services");

//    	calculateRelatednesToTargetDatasetSplitTerms("imdb");
//    	calculateRelatednesToTargetDatasetSplitTerms("tmdb");
//    	calculateRelatednesToTargetDatasetSplitTerms("rt");
//    	calculateRelatednesToTargetDatasetSplitTerms("indian");
//    	calculateRelatednesToTargetDatasetSplitTerms("movies");
//    	calculateRelatednesToTargetDatasetSplitTerms("netflix");
//    	calculateRelatednesToTargetDatasetSplitTerms("tmdb-350k");
//    	calculateRelatednesToTargetDatasetSplitTerms("datasets.data-cityofnewyork-us.education");
//    	calculateRelatednesToTargetDatasetSplitTerms("datasets.data-cityofnewyork-us.finance");
//    	calculateRelatednesToTargetDatasetSplitTerms("datasets.data-cityofnewyork-us.services");
    }

	private static void generateDomainRepresentation(String outputFolder, String simAlgo, boolean deleteIndex, boolean noExpand) throws IOException {
		generateColumnDomains(outputFolder, simAlgo, deleteIndex, noExpand);
		generateDatasetDomain(outputFolder);
	}

//	private static void generateDomainRepresentationImproved() throws IOException {
//    	generateColumnDomains("improved", D4Config.EQSIM_TFICF);
//    	generateDatasetDomain("improved");
//	}
//
//	private static void generateDomainRepresentationNoIndex() throws IOException {
//		generateColumnDomains("no-index", D4Config.EQSIM_JI);
//		generateDatasetDomain("no-index");
//	}
//
//	private static void generateDomainRepresentationNoIndexImproved() throws IOException {
//		generateColumnDomains("no-index-improved", D4Config.EQSIM_TFICF);
//		generateDatasetDomain("no-index-improved");
//	}
//
//	private static void generateDomainRepresentationNoExpand() throws IOException {
//		generateColumnDomains("no-expand", D4Config.EQSIM_JI);
//		generateDatasetDomain("no-expand");
//	}
//
//	private static void generateDomainRepresentationNoExpandImproved() throws IOException {
//		generateColumnDomains("no-expand-improved", D4Config.EQSIM_TFICF);
//		generateDatasetDomain("no-expand-improved");
//	}
//
//	private static void generateDomainRepresentationNoIndexNoExpand() throws IOException {
//		generateColumnDomains("no-index-no-expand", D4Config.EQSIM_JI);
//		generateDatasetDomain("no-index-no-expand");
//	}
//
//	private static void generateDomainRepresentationNoIndexNoExpandImproved() throws IOException {
//		generateColumnDomains("no-index-no-expand-improved", D4Config.EQSIM_TFICF);
//		generateDatasetDomain("no-index-no-expand-improved");
//	}

//	private static void generateDomainRepresentationNoIndexNoExpandSplitTerms() throws IOException {
//		generateColumnDomainsNoIndexNoExpandSplitTerms("no-index-no-expand-split-terms", D4Config.EQSIM_JI);
//		generateDatasetDomain("no-index-no-expand-split-terms");
//	}
//
//	private static void generateDomainRepresentationNoIndexNoExpandSplitTermsImproved() throws IOException {
//		generateColumnDomainsNoIndexNoExpandSplitTerms("no-index-no-expand-split-terms-improved", D4Config.EQSIM_TFICF);
//		generateDatasetDomain("no-index-no-expand-split-terms-improved");
//	}

	private static void generateTargetDatasetTermIndex(String datasetFolderName) {
		File outputPath = createOutputPath(datasetFolderName);
		generateColumnFiles(datasetFolderName, outputPath, false, false);
		generateTermIndex(outputPath);
	}

	private static void calculateRelatednesToTargetDataset(String datasetFolderName) throws IOException {
		generateTargetDatasetTermIndex(datasetFolderName);
		calculateRelatednesToTargetDatasetForDomainRepresentation(datasetFolderName, "default");
		calculateRelatednesToTargetDatasetForDomainRepresentation(datasetFolderName, "default-tf");
		calculateRelatednesToTargetDatasetForDomainRepresentation(datasetFolderName, "no-index");
		calculateRelatednesToTargetDatasetForDomainRepresentation(datasetFolderName, "no-index-tf");
		calculateRelatednesToTargetDatasetForDomainRepresentation(datasetFolderName, "no-index-no-expand");
		calculateRelatednesToTargetDatasetForDomainRepresentation(datasetFolderName, "no-index-no-expand-");
		calculateRelatednesToTargetDatasetForDomainRepresentation(datasetFolderName, "manual");
	}

//	private static void calculateRelatednesToTargetDatasetSplitTerms(String datasetFolderName) throws IOException {
//		generateTargetDatasetTermIndex(datasetFolderName, true);
//		String datasetSplitTermsFolderName = datasetFolderName+"-split-terms";
//		calculateRelatednesToTargetDatasetForDomainRepresentation(datasetSplitTermsFolderName, "no-index-no-expand-split-terms");
//		calculateRelatednesToTargetDatasetForDomainRepresentation(datasetSplitTermsFolderName, "no-index-no-expand-split-terms-improved");
//	}

	private static void calculateRelatednesToTargetDatasetForDomainRepresentation(String datasetFolderName, String domainRepresentationFolderName) throws IOException {
		Path relatednessResultFile = Paths.get(createOutputPath(datasetFolderName).getPath(), "relatedness_result_"+domainRepresentationFolderName+".txt");
		Path relatednessResultCsv = Paths.get(createOutputPath(datasetFolderName).getPath(), "relatedness_result.csv");
		if(!relatednessResultFile.toFile().exists()) {
			RelatednessResult datasetRelatedness = calculateTargetDatasetRelatedness(
					datasetFolderName,
					getDomainRepresentationFolder(domainRepresentationFolderName));
			String relatednessNumbersMessage = "Matched " + datasetRelatedness.getMatched() + " terms on a dataset with " + datasetRelatedness.getDatasetSize() + " terms, using a domain representation with " + datasetRelatedness.getDomainRepresentationSize() + " terms.";
			System.out.println(relatednessNumbersMessage);
			Files.writeString(relatednessResultFile, relatednessNumbersMessage + "\n", StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
			if(!relatednessResultCsv.toFile().exists()) {
				Files.writeString(relatednessResultCsv, "domain_representation_type,domain_representation_size,dataset_size,matched,jaccard_index\n", StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
			}
			double jaccardIndex = (datasetRelatedness.getMatched() * 1d) / (datasetRelatedness.getDatasetSize() + datasetRelatedness.getDomainRepresentationSize());
			String csvOutput = String.format("%s,%d,%d,%d,%f", domainRepresentationFolderName, datasetRelatedness.getDomainRepresentationSize(), datasetRelatedness.getDatasetSize(), datasetRelatedness.getMatched(), jaccardIndex);
			Files.writeString(relatednessResultCsv, csvOutput + "\n", StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
		}
		else {
			System.out.println(relatednessResultFile.toFile().getName() +" already exists. Skipping this step.");
		}
	}

//	private static void generateColumnDomains(String outputFolder, String simAlgo) {
//    	File outputPath = createOutputPath(outputFolder);
//		generateColumnFiles(INPUT_DOMAIN_REPRESENTATION_DATA_FOLDER_NAME, outputPath, false, false);
//		generateTermIndex(outputPath);
//		generateEquivalenceClasses(outputPath);
//		computeSignatures(outputPath, simAlgo);
//		expandColumns(outputPath);
//		discoverLocalDomains(outputPath);
//		pruneStrongDomains(outputPath);
//		exportStrongDomains(outputPath);
//	}
//
//	private static void generateColumnDomainsNoIndex(String outputFolder, String simAlgo) {
//		File outputPath = createOutputPath(outputFolder);
//		generateColumnFiles(INPUT_DOMAIN_REPRESENTATION_DATA_FOLDER_NAME, outputPath, true, false);
//		generateTermIndex(outputPath);
//		generateEquivalenceClasses(outputPath);
//		computeSignatures(outputPath, simAlgo);
//		expandColumns(outputPath);
//		discoverLocalDomains(outputPath);
//		pruneStrongDomains(outputPath);
//		exportStrongDomains(outputPath);
//	}
//
//	private static void generateColumnDomainsNoExpand(String outputFolder, String simAlgo) {
//		File outputPath = createOutputPath(outputFolder);
//		generateColumnFiles(INPUT_DOMAIN_REPRESENTATION_DATA_FOLDER_NAME, outputPath, false, false);
//		generateTermIndex(outputPath);
//		generateEquivalenceClasses(outputPath);
//		computeSignatures(outputPath, simAlgo);
//		noExpandColumns(outputPath);
//		discoverLocalDomains(outputPath);
//		pruneStrongDomains(outputPath);
//		exportStrongDomains(outputPath);
//	}
//
//	private static void generateColumnDomainsNoIndexNoExpand(String outputFolder, String simAlgo) {
//		File outputPath = createOutputPath(outputFolder);
//		generateColumnFiles(INPUT_DOMAIN_REPRESENTATION_DATA_FOLDER_NAME, outputPath, true, false);
//		generateTermIndex(outputPath);
//		generateEquivalenceClasses(outputPath);
//		computeSignatures(outputPath, simAlgo);
//		noExpandColumns(outputPath);
//		discoverLocalDomains(outputPath);
//		pruneStrongDomains(outputPath);
//		exportStrongDomains(outputPath);
//	}

	private static void generateColumnDomains(String outputFolder, String simAlgo, boolean deleteIndex, boolean noExpand) {
		File outputPath = createOutputPath(outputFolder);
		generateColumnFiles(INPUT_DOMAIN_REPRESENTATION_DATA_FOLDER_NAME, outputPath, deleteIndex, false);
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

//	private static void generateColumnDomainsNoIndexNoExpandSplitTerms(String outputFolder, String simAlgo) {
//		File outputPath = createOutputPath(outputFolder);
//		generateColumnFiles("domain-data", outputPath, true, true);
//		generateTermIndex(outputPath);
//		generateEquivalenceClasses(outputPath);
//		computeSignatures(outputPath, simAlgo);
//		noExpandColumns(outputPath);
//		discoverLocalDomains(outputPath);
//		pruneStrongDomains(outputPath);
//		exportStrongDomains(outputPath);
//	}

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

	private static File createOutputPath(String outputFolder) {
		File outputPath = new File("output", outputFolder);
		if(!outputPath.exists()) {
			outputPath.mkdirs();
		}
		return outputPath;
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

	/**
	 * Generate the dataset domain from the column domains.
	 *
	 * This dataset domain consists of all column domains, each of which
	 * only contain their first block of terms.
	 * @throws IOException
	 */
	private static void generateDatasetDomain(String outputFolder) throws IOException {
		// read exported JSON and output new JSON that only keeps the first block
		File outputPath = createOutputPath(outputFolder);
		File datasetDomainPath = new File(outputPath, "dataset-domain");
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

	private static File getDomainRepresentationFolder(String outputFolder) {
		File outputPath = createOutputPath(outputFolder);
		return new File(outputPath, "dataset-domain");
	}

	/**
	 * Calculate the relatedness score for the given dataset.
	 *
	 * This counts the amount of terms that are present in both the domain representation and the target dataset.
	 * It then returns this as a percentage of the total amount of terms in the domain representation.
	 *
	 * @param datasetFolderName
	 * @return
	 * @throws JsonSyntaxException
	 * @throws JsonIOException
	 * @throws IOException
	 */
	private static RelatednessResult calculateTargetDatasetRelatedness(String datasetFolderName, File domainRepresentationPath) throws JsonSyntaxException, JsonIOException, IOException {
		File outputPath = createOutputPath(datasetFolderName);
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
			String error = "The domain representation did not contain any terms. The relatedness score cannot be calculated.";
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

		return new RelatednessResult(matchedDatasetTerms.size(), datasetDomainTerms.size(), datasetTerms.size());
	}
}
