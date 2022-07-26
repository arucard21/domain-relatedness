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
        // ----------------------------------------------------------------
        // GENERATE COLUMN FILES
        // ----------------------------------------------------------------
        try {
            new D4().columns(
                    new File("data"),
                    new File("columns.tsv"),
                    1000,
                    6,
                    true,
                    new File("columns")
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
                    new File("columns"),
                    Threshold.getConstraint("GT0.5"),
                    10000000,
                    false,
                    6,
                    true,
                    new File("term-index.txt.gz")
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
                    new File("term-index.txt.gz"),
                    true,
                    new File("compressed-term-index.txt.gz")
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
                    new File("compressed-term-index.txt.gz"),
                    D4Config.EQSIM_JI,
                    D4Config.ROBUST_LIBERAL,
                    true,
                    false,
                    false,
                    6,
                    true,
                    new TelemetryPrinter(),
                    new File("signatures.txt.gz")
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
                    new File("compressed-term-index.txt.gz"),
                    new File("signatures.txt.gz"),
                    D4Config.TRIMMER_CENTRIST,
                    Threshold.getConstraint("GT0.25"),
                    5,
                    new BigDecimal("0.05"),
                    6,
                    true,
                    new TelemetryPrinter(),
                    new File("expanded-columns.txt.gz")
            );
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "Expanding columns failed with exception: ", ex);
            System.exit(-1);
        }
        // ----------------------------------------------------------------
        // DISCOVER LOCAL DOMAINS
        // ----------------------------------------------------------------
        File localDomainFile = new File("local-domains.txt.gz");
        try {
            new D4().localDomains(
                    new File("compressed-term-index.txt.gz"),
                    new File("expanded-columns.txt.gz"),
                    new File("signatures.txt.gz"),
                    D4Config.TRIMMER_CENTRIST,
                    false,
                    6,
                    false,
                    true,
                    new TelemetryPrinter(),
                    localDomainFile
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
                    new File("compressed-term-index.txt.gz"),
                    new File("local-domains.txt.gz"),
                    Threshold.getConstraint("GT0.5"),
                    Threshold.getConstraint("GT0.1"),
                    new BigDecimal("0.25"),
                    6,
                    true,
                    new TelemetryPrinter(),
                    new File("strong-domains.txt.gz")
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
                    new File("compressed-term-index.txt.gz"),
                    new File("term-index.txt.gz"),
                    new File("columns.tsv"),
                    new File("strong-domains.txt.gz"),
                    100,
                    true,
                    new File("domains")
            );
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "Exporting strong domains failed with exception: ", ex);
            System.exit(-1);
        }
    }
}
