package com.github.arucard21.dataset_domain_terms;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;

import com.github.arucard21.dataset_domain_terms.objects.ColumnDomain;
import com.github.arucard21.dataset_domain_terms.objects.ExportedStrongDomain;
import com.google.gson.GsonBuilder;

import org.opendata.core.constraint.Threshold;
import org.opendata.curation.d4.D4;
import org.opendata.curation.d4.D4Config;
import org.opendata.curation.d4.telemetry.TelemetryPrinter;

public class DatasetDomainTerms {
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

	/**
	 * Path to the directory containing all tables from both domain-representative datasets, as (gzipped) TSV files.
	 */
	private final Path domainRepresentativeDatasetsDirectory;
	/**
	 * Path to the directory where the dataset domain terms is stored, along with any intermediate output that is generated.
	 */
	private final Path outputDirectory;
	private String similarityAlgorithm;
	private String pruningStrategy;
	private boolean columnExpansionDisabled;
	private boolean allTermsFromColumnDomainsIncluded;

	public DatasetDomainTerms(Path domainRepresentativeDatasetsDirectory, Path outputDirectory) {
		this.domainRepresentativeDatasetsDirectory = domainRepresentativeDatasetsDirectory;
		this.outputDirectory = outputDirectory;
	}

	public String getSimilarityAlgorithm() {
		return similarityAlgorithm;
	}

	public DatasetDomainTerms similarityAlgorithm(String similarityAlgorithm) {
		this.similarityAlgorithm = similarityAlgorithm;
		return this;
	}

	public String getPruningStrategy() {
		return pruningStrategy;
	}

	public DatasetDomainTerms pruningStrategy(String pruningStrategy) {
		this.pruningStrategy = pruningStrategy;
		return this;
	}

	public boolean isColumnExpansionDisabled() {
		return columnExpansionDisabled;
	}

	public DatasetDomainTerms columnExpansionDisabled(boolean columnExpansionDisabled) {
		this.columnExpansionDisabled = columnExpansionDisabled;
		return this;
	}

	public boolean isAllTermsFromColumnDomainsIncluded() {
		return allTermsFromColumnDomainsIncluded;
	}

	public DatasetDomainTerms allTermsFromColumnDomainsIncluded(boolean allTermsFromColumnDomainsIncluded) {
		this.allTermsFromColumnDomainsIncluded = allTermsFromColumnDomainsIncluded;
		return this;
	}

	public Path getDomainRepresentativeDatasetsDirectory() {
		return domainRepresentativeDatasetsDirectory;
	}

	public Path getOutputDirectory() {
		return outputDirectory;
	}

	public Path generate() throws IOException {
		Path columnDomainsPath = generateColumnDomains();
		return generateDatasetDomain(columnDomainsPath);
	}

	private Path generateColumnDomains() throws IOException {
		ensureOutputDirExists(outputDirectory);
		Path columnsPath = generateColumnFiles();
		Path termIndexPath = generateTermIndex(columnsPath);
		Path eqsPath = generateEquivalenceClasses(termIndexPath);
		Path signaturesPath = computeSignatures(eqsPath);
		Path expandedColumnsPath;
		if(columnExpansionDisabled) {
			expandedColumnsPath = noExpandColumns(eqsPath);
		}
		else {
			expandedColumnsPath = expandColumns(eqsPath, signaturesPath);
		}
		Path localDomainsPath = discoverLocalDomains(eqsPath, signaturesPath, expandedColumnsPath);
		Path columnDomainsInternalFormatPath = pruneToStrongDomains(eqsPath, localDomainsPath);
		Path columnDomainsPath = exportStrongDomains(termIndexPath, eqsPath, columnDomainsInternalFormatPath);
		return columnDomainsPath;
	}

	private Path generateDatasetDomain(Path columnDomainsPath) throws IOException {
		if(!inputExists(columnDomainsPath)) {
			throw new IllegalArgumentException(String.format("The path \"%s\" to the column domains used as input does not exist"));
		}
		Path datasetDomainPath = columnDomainsPath.getParent().resolve(DATASET_DOMAIN_DIR_NAME);
		if(!outputExists(datasetDomainPath)) {
			ensureOutputDirExists(datasetDomainPath);
			Files.list(columnDomainsPath).forEach(columnDomainPath -> writeColumnDomainTermsUsedInDatasetDomain(columnDomainPath, datasetDomainPath));
		}
		return datasetDomainPath;
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

	private void ensureOutputDirExists(Path outputPath) {
		File outputFile = outputPath.toFile();
		if(!outputFile.exists()) {
			outputFile.mkdirs();
		}
	}

	private Path generateColumnFiles() {
		// ----------------------------------------------------------------
	    // GENERATE COLUMN FILES
	    // ----------------------------------------------------------------
	    try {
	    	Path outputColumnsPath = outputDirectory.resolve(COLUMNS_DIR_NAME);
	        if(!outputExists(outputColumnsPath)) {
				new D4().columns(
		                domainRepresentativeDatasetsDirectory.toFile(),
		                outputDirectory.resolve(COLUMNS_METADATA_FILE_NAME).toFile(),
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

	private Path generateEquivalenceClasses(Path termIndexPath) {
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

	private Path computeSignatures(Path equivalenceClassesPath) {
		// ----------------------------------------------------------------
	    // COMPUTE SIGNATURES
	    // ----------------------------------------------------------------
	    try {
	        Path outputSignatures = equivalenceClassesPath.getParent().resolve(SIGNATURES_FILE_NAME);
	        if(!outputExists(outputSignatures)) {
				new D4().signatures(
						equivalenceClassesPath.toFile(),
						similarityAlgorithm,
		                D4Config.ROBUST_LIBERAL,
		                true,
		                false,
		                false,
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

	private Path expandColumns(Path equivalenceClassesPath, Path signaturesPath) {
		// ----------------------------------------------------------------
	    // EXPAND COLUMNS
	    // ----------------------------------------------------------------
	    try {
	        Path outputExpandColumns = signaturesPath.getParent().resolve(EXPANDED_COLUMNS_FILE_NAME);
	        if(!outputExists(outputExpandColumns)) {
				new D4().expandColumns(
						equivalenceClassesPath.toFile(),
		                signaturesPath.toFile(),
		                pruningStrategy,
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

	private Path noExpandColumns(Path equivalenceClassesPath) {
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

	private Path discoverLocalDomains(Path equivalenceClassesPath, Path signaturesPath, Path expandedColumnsPath) {
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

	private Path pruneToStrongDomains(Path equivalenceClassesPath, Path localDomainsPath) {
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

	private Path exportStrongDomains(Path termIndexPath, Path equivalenceClassesPath, Path columnDomainsInternalFormatPath) {
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

	private void writeColumnDomainTermsUsedInDatasetDomain(Path columnDomainPath, Path datasetDomainPath) {
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
	    	if(allTermsFromColumnDomainsIncluded) {
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
}
