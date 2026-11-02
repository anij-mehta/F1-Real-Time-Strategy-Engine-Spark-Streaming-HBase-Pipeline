# Pit Window Model Report

- Held-out race: `2024_Australia`
- Training races: `2023_Australia`, `2023_Bahrain`, `2023_Saudi`, `2024_Bahrain`, `2024_Saudi`
- Split strategy: race-grouped; the held-out race was never included in training.
- Class imbalance: positive examples are weighted by the training negative/positive ratio.
- Selected model: `DecisionTreeClassifier` (highest held-out F1; AUC breaks ties).

| Model | Accuracy | Precision | Recall | F1 | AUC |
| --- | ---: | ---: | ---: | ---: | ---: |
| LogisticRegression | 0.7844 | 0.2924 | 0.6330 | 0.4000 | 0.8381 |
| DecisionTreeClassifier | 0.7927 | 0.3228 | 0.7523 | 0.4518 | 0.6922 |
