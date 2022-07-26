package domain.relatedness;

import java.io.File;
import java.math.BigDecimal;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.opendata.core.constraint.Threshold;
import org.opendata.curation.d4.D4;
import org.opendata.curation.d4.D4Config;
import org.opendata.curation.d4.telemetry.TelemetryPrinter;

public class DomainRelatedness {
	private static final Logger LOGGER = Logger.getLogger(D4.class.getName());

    public static void main(String[] args) {
        generateColumnLevelDomains();
        generateColumnLevelDomainsWithImprovements();
    }

    private static void generateColumnLevelDomains() {
    	// ----------------------------------------------------------------
        // GENERATE COLUMN FILES
        // ----------------------------------------------------------------
    	String pathPrefix = "output/default/";
    	new File(pathPrefix).mkdirs();
        try {
            new D4().columns(
                    new File("input", "data"),
                    new File(pathPrefix, "columns.tsv"),
                    1000,
                    6,
                    true,
                    new File(pathPrefix, "columns")
            );
        } catch (java.lang.InterruptedException | java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "Generating columns failed with exception: ", ex);
            System.exit(-1);
        }

        // ----------------------------------------------------------------
        // GENERATE TERM INDEX
        // ----------------------------------------------------------------
        try {
            new D4().termIndex(
                    new File(pathPrefix, "columns"),
                    Threshold.getConstraint("GT0.5"),
                    10000000,
                    false,
                    6,
                    true,
                    new File(pathPrefix, "term-index.txt.gz")
            );
        } catch (java.lang.InterruptedException | java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "Generating term index failed with exception: ", ex);
            System.exit(-1);
        }
        // ----------------------------------------------------------------
        // GENERATE EQUIVALENCE CLASSES
        // ----------------------------------------------------------------
        try {
            new D4().eqs(
                    new File(pathPrefix, "term-index.txt.gz"),
                    true,
                    new File(pathPrefix, "compressed-term-index.txt.gz")
            );
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "Generating equivalence classes failed with exception: ", ex);
            System.exit(-1);
        }
        // ----------------------------------------------------------------
        // COMPUTE SIGNATURES
        // ----------------------------------------------------------------
        try {
            new D4().signatures(
                    new File(pathPrefix, "compressed-term-index.txt.gz"),
                    D4Config.EQSIM_JI,
                    D4Config.ROBUST_LIBERAL,
                    true,
                    false,
                    false,
                    6,
                    true,
                    new TelemetryPrinter(),
                    new File(pathPrefix, "signatures.txt.gz")
            );
        } catch (java.lang.InterruptedException | java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "Computing signatures failed with exception: ", ex);
            System.exit(-1);
        }
        // ----------------------------------------------------------------
        // EXPAND COLUMNS
        // ----------------------------------------------------------------
        try {
            new D4().expandColumns(
                    new File(pathPrefix, "compressed-term-index.txt.gz"),
                    new File(pathPrefix, "signatures.txt.gz"),
                    D4Config.TRIMMER_CONSERVATIVE,
                    Threshold.getConstraint("GT0.25"),
                    5,
                    new BigDecimal("0.05"),
                    6,
                    true,
                    new TelemetryPrinter(),
                    new File(pathPrefix, "expanded-columns.txt.gz")
            );
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "Expanding columns failed with exception: ", ex);
            System.exit(-1);
        }
        // ----------------------------------------------------------------
        // DISCOVER LOCAL DOMAINS
        // ----------------------------------------------------------------
        try {
            new D4().localDomains(
                    new File(pathPrefix, "compressed-term-index.txt.gz"),
                    new File(pathPrefix, "expanded-columns.txt.gz"),
                    new File(pathPrefix, "signatures.txt.gz"),
                    D4Config.TRIMMER_CONSERVATIVE,
                    false,
                    6,
                    false,
                    true,
                    new TelemetryPrinter(),
                    new File(pathPrefix, "local-domains.txt.gz")
            );
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "Discovering local domains failed with exception: ", ex);
            System.exit(-1);
        }
        // ----------------------------------------------------------------
        // PRUNE STRONG DOMAINS
        // ----------------------------------------------------------------
        try {
            new D4().strongDomains(
                    new File(pathPrefix, "compressed-term-index.txt.gz"),
                    new File(pathPrefix, "local-domains.txt.gz"),
                    Threshold.getConstraint("GT0.5"),
                    Threshold.getConstraint("GT0.1"),
                    new BigDecimal("0.25"),
                    6,
                    true,
                    new TelemetryPrinter(),
                    new File(pathPrefix, "strong-domains.txt.gz")
            );
        } catch (java.lang.InterruptedException | java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "Pruning strong domains failed with exception: ", ex);
            System.exit(-1);
        }
        // ----------------------------------------------------------------
        // EXPORT
        // ----------------------------------------------------------------
        try {
            new D4().exportStrongDomains(
                    new File(pathPrefix, "compressed-term-index.txt.gz"),
                    new File(pathPrefix, "term-index.txt.gz"),
                    new File(pathPrefix, "columns.tsv"),
                    new File(pathPrefix, "strong-domains.txt.gz"),
                    100,
                    true,
                    new File(pathPrefix, "domains")
            );
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "Exporting strong domains failed with exception: ", ex);
            System.exit(-1);
        }

    }

    private static void generateColumnLevelDomainsWithImprovements() {
    	// ----------------------------------------------------------------
        // GENERATE COLUMN FILES
        // ----------------------------------------------------------------
    	String pathPrefix = "output/improved/";
    	new File(pathPrefix).mkdirs();
        try {
            new D4().columns(
                    new File("input", "data"),
                    new File(pathPrefix, "columns.tsv"),
                    1000,
                    6,
                    true,
                    new File(pathPrefix, "columns")
            );
        } catch (java.lang.InterruptedException | java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "Generating columns failed with exception: ", ex);
            System.exit(-1);
        }

        // ----------------------------------------------------------------
        // GENERATE TERM INDEX
        // ----------------------------------------------------------------
        try {
            new D4().termIndex(
                    new File(pathPrefix, "columns"),
                    Threshold.getConstraint("GT0.5"),
                    10000000,
                    false,
                    6,
                    true,
                    new File(pathPrefix, "term-index.txt.gz")
            );
        } catch (java.lang.InterruptedException | java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "Generating term index failed with exception: ", ex);
            System.exit(-1);
        }
        // ----------------------------------------------------------------
        // GENERATE EQUIVALENCE CLASSES
        // ----------------------------------------------------------------
        try {
            new D4().eqs(
                    new File(pathPrefix, "term-index.txt.gz"),
                    true,
                    new File(pathPrefix, "compressed-term-index.txt.gz")
            );
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "Generating equivalence classes failed with exception: ", ex);
            System.exit(-1);
        }
        // ----------------------------------------------------------------
        // COMPUTE SIGNATURES
        // ----------------------------------------------------------------
        try {
            new D4().signatures(
                    new File(pathPrefix, "compressed-term-index.txt.gz"),
                    D4Config.EQSIM_TFICF,
                    D4Config.ROBUST_IGNORELAST,
                    true,
                    false,
                    true,
                    6,
                    true,
                    new TelemetryPrinter(),
                    new File(pathPrefix, "signatures.txt.gz")
            );
        } catch (java.lang.InterruptedException | java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "Computing signatures failed with exception: ", ex);
            System.exit(-1);
        }
        // ----------------------------------------------------------------
        // EXPAND COLUMNS
        // ----------------------------------------------------------------
        try {
            new D4().expandColumns(
                    new File(pathPrefix, "compressed-term-index.txt.gz"),
                    new File(pathPrefix, "signatures.txt.gz"),
                    D4Config.TRIMMER_CONSERVATIVE,
                    Threshold.getConstraint("GT0.25"),
                    5,
                    new BigDecimal("0.05"),
                    6,
                    true,
                    new TelemetryPrinter(),
                    new File(pathPrefix, "expanded-columns.txt.gz")
            );
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "Expanding columns failed with exception: ", ex);
            System.exit(-1);
        }
        // ----------------------------------------------------------------
        // DISCOVER LOCAL DOMAINS
        // ----------------------------------------------------------------
        try {
            new D4().localDomains(
                    new File(pathPrefix, "compressed-term-index.txt.gz"),
                    new File(pathPrefix, "expanded-columns.txt.gz"),
                    new File(pathPrefix, "signatures.txt.gz"),
                    D4Config.TRIMMER_CONSERVATIVE,
                    false,
                    6,
                    false,
                    true,
                    new TelemetryPrinter(),
                    new File(pathPrefix, "local-domains.txt.gz")
            );
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "Discovering local domains failed with exception: ", ex);
            System.exit(-1);
        }
        // ----------------------------------------------------------------
        // PRUNE STRONG DOMAINS
        // ----------------------------------------------------------------
        try {
            new D4().strongDomains(
                    new File(pathPrefix, "compressed-term-index.txt.gz"),
                    new File(pathPrefix, "local-domains.txt.gz"),
                    Threshold.getConstraint("GT0.5"),
                    Threshold.getConstraint("GT0.1"),
                    new BigDecimal("0.25"),
                    6,
                    true,
                    new TelemetryPrinter(),
                    new File(pathPrefix, "strong-domains.txt.gz")
            );
        } catch (java.lang.InterruptedException | java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "Pruning strong domains failed with exception: ", ex);
            System.exit(-1);
        }
        // ----------------------------------------------------------------
        // EXPORT
        // ----------------------------------------------------------------
        try {
            new D4().exportStrongDomains(
                    new File(pathPrefix, "compressed-term-index.txt.gz"),
                    new File(pathPrefix, "term-index.txt.gz"),
                    new File(pathPrefix, "columns.tsv"),
                    new File(pathPrefix, "strong-domains.txt.gz"),
                    100,
                    true,
                    new File(pathPrefix, "domains")
            );
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "Exporting strong domains failed with exception: ", ex);
            System.exit(-1);
        }
    }
}
