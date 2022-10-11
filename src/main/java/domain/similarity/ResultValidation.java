package domain.similarity;

public class ResultValidation {
	private final int truePositives;
	private final int falsePositives;
	private final int trueNegatives;
	private final int falseNegatives;
	private final double accuracy;
	private final double precision;
	private final double recall;
	private final double negPrecision;
	private final double negRecall;
	private final double lowestDomainSimilarityScore;
	private final double highestDomainSimilarityScore;
	private final double lowestNonDomainSimilarityScore;
	private final double highestNonDomainSimilarityScore;
	private final boolean domainScoresExceedNonDomain;
	private final double domainRangeSize;
	private final double nonDomainRangeSize;

	public ResultValidation(int truePositives, int falsePositives, int trueNegatives, int falseNegatives, double lowestDomainSimilarityScore, double highestDomainSimilarityScore, double lowestNonDomainSimilarityScore, double highestNonDomainSimilarityScore) {
		this.lowestDomainSimilarityScore = lowestDomainSimilarityScore;
		this.highestDomainSimilarityScore = highestDomainSimilarityScore;
		this.lowestNonDomainSimilarityScore = lowestNonDomainSimilarityScore;
		this.highestNonDomainSimilarityScore = highestNonDomainSimilarityScore;
		this.domainScoresExceedNonDomain = lowestDomainSimilarityScore > highestNonDomainSimilarityScore;
		this.domainRangeSize = highestDomainSimilarityScore = lowestDomainSimilarityScore;
		this.nonDomainRangeSize = highestNonDomainSimilarityScore - lowestNonDomainSimilarityScore;

		this.truePositives = truePositives;
		this.falsePositives = falsePositives;
		this.trueNegatives = trueNegatives;
		this.falseNegatives = falseNegatives;
		this.accuracy = (truePositives+trueNegatives+falsePositives+falseNegatives) > 0 ?
				(truePositives+trueNegatives)*1d/(truePositives+trueNegatives+falsePositives+falseNegatives)*1d
				: -1;
		this.precision = (truePositives+falsePositives) > 0 ?
				(truePositives)*1d/(truePositives+falsePositives)*1d
				: -1;
		this.recall = (truePositives+falseNegatives) > 0 ?
				(truePositives)*1d/(truePositives+falseNegatives)*1d
				: -1;
		this.negPrecision = (trueNegatives+falseNegatives) > 0 ?
				(trueNegatives)*1d/(trueNegatives+falseNegatives)*1d
				: -1;
		this.negRecall = (trueNegatives+falsePositives) > 0 ?
				(trueNegatives)*1d/(trueNegatives+falsePositives)*1d
				: -1;
	}

	public int getTruePositives() {
		return truePositives;
	}
	public int getFalsePositives() {
		return falsePositives;
	}
	public int getTrueNegatives() {
		return trueNegatives;
	}
	public int getFalseNegatives() {
		return falseNegatives;
	}
	public double getAccuracy() {
		return accuracy;
	}
	public double getPrecision() {
		return precision;
	}
	public double getRecall() {
		return recall;
	}
	public double getNegPrecision() {
		return negPrecision;
	}
	public double getNegRecall() {
		return negRecall;
	}
	public double getLowestDomainSimilarityScore() {
		return lowestDomainSimilarityScore;
	}
	public double getHighestDomainSimilarityScore() {
		return highestDomainSimilarityScore;
	}
	public double getLowestNonDomainSimilarityScore() {
		return lowestNonDomainSimilarityScore;
	}
	public double getHighestNonDomainSimilarityScore() {
		return highestNonDomainSimilarityScore;
	}
	public boolean isDomainScoresExceedNonDomain() {
		return domainScoresExceedNonDomain;
	}

	public double getDomainRangeSize() {
		return domainRangeSize;
	}
	public double getNonDomainRangeSize() {
		return nonDomainRangeSize;
	}
}
