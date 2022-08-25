package domain.similarity;

import java.util.List;

public class ExportedEquivalenceClass {
	private int id;
	private int termCount;
	private String weight;
	private List<String> terms;

	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	public int getTermCount() {
		return termCount;
	}
	public void setTermCount(int termCount) {
		this.termCount = termCount;
	}
	public String getWeight() {
		return weight;
	}
	public void setWeight(String weight) {
		this.weight = weight;
	}
	public List<String> getTerms() {
		return terms;
	}
	public void setTerms(List<String> terms) {
		this.terms = terms;
	}
}
