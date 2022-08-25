package domain.relatedness;

import java.util.List;

public class ExportedStrongDomain {
	private List<ExportedColumn> columns;
	private List<List<ExportedEquivalenceClass>> terms;

	public List<ExportedColumn> getColumns() {
		return columns;
	}
	public void setColumns(List<ExportedColumn> columns) {
		this.columns = columns;
	}
	public List<List<ExportedEquivalenceClass>> getTerms() {
		return terms;
	}
	public void setTerms(List<List<ExportedEquivalenceClass>> terms) {
		this.terms = terms;
	}
}
