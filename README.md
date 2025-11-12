# Benchmarking Reinforcement Learning Algorithms for ICU Ventilator Settings: An Interpretable and Probabilistic Patient Environment for Doctor Agents
 
This repository provides a benchmarking framework for offline and reinforcement learning algorithms to optimize invasive ventilator settings in the ICU using MIMIC-IV and eICU data.
 
## Overview
- **Data Extraction & Cohort Definition** (`cohort_features_sql.md`): SQL logic (MIMIC-IV focus) for building ventilation cohorts excluding DNR/DNI, age filtering, and assembling baseline + charttime features.
- **Preprocessing** (`preprocessing.ipynb`):
  - Merge cohort, baseline, vitals, ventilator settings, ground truth (extubation / reintubation outcomes)
  - Generate variable-length and fixed-length (24h / 48h) trajectories
  - One-hot encode categorical (gender, race, ventilator mode group), derive hours_in, remove outliers (IQR and clinical ranges), fill missing (KNN for baseline, forward/backward fill for time series)
- **Environment Construction** (`ventilation_patient_environment.ipynb` / continuous variant):
  - Discretization / bin strategies (e.g. NEWS-based) for state/action space
  - Build state transition dictionaries with fallback approximation for unseen (neighbor search, distance thresholds)
  - Reward design components: extubation success, internal stabilization, action penalties, weaning guideline adherence
  - Gym-style patient environment & trajectory replay
- **Offline RL Training & Policies** (within environment notebook):
  - Algorithms: DiscreteBC, NFQ, DQN, DoubleDQN, DiscreteSAC, DiscreteBCQ, DiscreteCQL (d3rlpy)
- **Benchmark & Reward Design Analysis** (`analysis_benchmark.ipynb`): Aggregate metrics across reward settings and datasets (train/test/eICU).
- **Evaluation Metrics & Fidelity** (`evaluation_metrics.ipynb`): Distributional comparisons (trajectory & transition level), action diversity, anomaly %, extubation meet rate, time-to-meet, reward totals.
 
## Key Data Columns
- ID: `stay_id`, `before_weaning_hr` (converted to `hours_in` baseline reference)
- State vars (examples): `heart_rate`, `resp_rate`, `spo2` (optional: `sbp`, `dbp`, `mbp`, `tidal_volume_observed`, `RSBI`, `minute_ventilation`, `gcs`)
- Action vars: `fio2`, `respiratory_rate_set`, (optional: `peep`, `tidal_volume_set`, `plateau_pressure`, `ventilator_mode_group`)
- Baseline: `gender`, `age`, `race`, comorbid flags (e.g. sepsis, ARDS), (optional: `weight_kg`, `height_cm`, `tobacco`)
 
## Rewards (composable)
- Extubation success / stability windows
- Internal stabilization (vitals within ranges)
- Action stability penalty / action change penalty
 
## Metrics
- Cumulative reward
- Extubation meet rate (%)
- Mean trajectory length & time-to-meet
- Action diversity
- Anomalous / nonsense action percentage
- State/action distribution shift (train vs test vs external)
 
## Running Notebooks (Suggested Order)
1. `cohort_features_sql.md` (review SQL logic / run externally on BigQuery or local DB)
2. `preprocessing.ipynb`
3. `ventilation_patient_environment.ipynb` (build env + trajectories + train policies)
4. `analysis_benchmark.ipynb`
5. `evaluation_metrics.ipynb`
 
## Environment & Dependencies
Core libs: pandas, numpy, matplotlib, seaborn, scikit-learn, scipy, tqdm, torch, d3rlpy, tensorflow (for ehrMGAN benchmarking), jupyter.
 
## License & Use
Ensure proper data use approvals (MIMIC/eICU). This code expects pre-extracted CSVs placed under `data/` subfolders (`mimic_iv`, `eICU`).
 
---
Feel free to adapt binning, reward shaping, and feature inclusion to your clinical hypotheses.
