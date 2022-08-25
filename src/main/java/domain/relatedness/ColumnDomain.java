package domain.relatedness;

import java.util.List;

public class ColumnDomain {
	private List<ExportedColumn> columns;
	private List<String> terms;

	public List<ExportedColumn> getColumns() {
		return columns;
	}
	public void setColumns(List<ExportedColumn> columns) {
		this.columns = columns;
	}
	public List<String> getTerms() {
		return terms;
	}
	public void setTerms(List<String> terms) {
		this.terms = terms;
	}
}
