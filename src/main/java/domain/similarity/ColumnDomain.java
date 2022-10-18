package domain.similarity;

import java.util.List;

public class ColumnDomain {
	private List<ExportedColumn> columns;
	private List<String> terms;
	private long termCount;

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
		this.termCount = terms.size();
	}
	public long getTermCount() {
		return termCount;

	}
}
