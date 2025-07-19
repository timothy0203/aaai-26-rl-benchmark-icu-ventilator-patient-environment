# AAAI-26
Benchmarking Reinforcement Learning Algorithms for ICU Ventilator Settings: A Patient Environment for Doctor Agents

## Contents

- **scripts/cohort_features_sql.md**: Documentation for SQL queries used to extract cohort and features in BigQuery.
- **scripts/preprocessing.ipynb**: Data preprocessing steps to get baseline_charttime_ground_truth.csv for MIMIC-IV and eICU.
- **scripts/ventilation_patient_environment.ipynb**: Build a Gym Ventilated Patient Enviornment for d3rlpy RL agents.
- **scripts/analysis_benchmark.ipynb**: Analyzing benchmark results.
- **scripts/evaluation_metrics.ipynb**: Trajectory level evaluation comparasion with related works.
- **data/mimic_iv/min_max_values_guidelines.json**: Extubation guideline values for each features.