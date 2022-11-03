package com.github.arucard21.dataset_discovery.objects;

public class DatasetSimilarity {
	private final String datasetName;
	private final double similarityScore;
	private int similarityOrderIndex;
	private double consecutiveDrop;

	public DatasetSimilarity(String datasetName, double similarityScore) {
		this.datasetName = datasetName;
		this.similarityScore = similarityScore;
	}

	public String getDatasetName() {
		return datasetName;
	}
	public double getSimilarityScore() {
		return similarityScore;
	}
	public int getSimilarityOrderIndex() {
		return similarityOrderIndex;
	}
	public void setSimilarityOrderIndex(int similarityOrderIndex) {
		this.similarityOrderIndex = similarityOrderIndex;
	}
	public double getConsecutiveDrop() {
		return consecutiveDrop;
	}
	public void setConsecutiveDrop(double consecutiveDrop) {
		this.consecutiveDrop = consecutiveDrop;
	}
}
