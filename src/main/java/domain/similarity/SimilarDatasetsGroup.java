package domain.similarity;

import java.util.ArrayList;
import java.util.List;

public class SimilarDatasetsGroup {
	private final List<DatasetSimilarity> similarDatasets;

	public SimilarDatasetsGroup(List<DatasetSimilarity> similarDatasets) {
		this.similarDatasets = new ArrayList<>(similarDatasets);
	}

	public List<DatasetSimilarity> getSimilarDatasets() {
		return similarDatasets;

	}
}
