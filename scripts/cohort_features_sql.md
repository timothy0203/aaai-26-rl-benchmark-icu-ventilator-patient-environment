# MIMIC-IV SQL
## Cohort
- Primary Key for each Trajectory
	- condition
		- continuous intubation time >= 24hours: 12323
		- exclude the "stay_id" which has DNR/DNI: 12323 - 1375 = 10948
		- age >= 20 and <= 100: 10948 - 36 = 10912
	- stay_id
	- starttime
	- endtime
	- row_num: the number of reintubation time under this stay_id -> only pick first satify 24 hr
```sql=
WITH ventilation_events AS (
	SELECT
		subject_id,
		i.stay_id,
		starttime,
		endtime,
	ROW_NUMBER() OVER (PARTITION BY i.stay_id ORDER BY starttime) AS row_num
	FROM physionet-data.mimiciv_derived.ventilation as ventilation
	INNER JOIN physionet-data.mimiciv_icu.icustays as i
	ON ventilation.stay_id = i.stay_id
	WHERE TIMESTAMP_DIFF(endtime, starttime, SECOND) >= 24 * 3600
	AND ventilation_status = 'InvasiveVent'
),
ventilation_events_first_row AS (
	SELECT
		subject_id,
		stay_id,
		starttime,
		endtime
	FROM ventilation_events
	WHERE row_num = 1
),
stay_id_with_DNRDNI AS (
	SELECT DISTINCT c.stay_id,
	FROM `physionet-data.mimiciv_icu.chartevents` AS c
	INNER JOIN ventilation_events_first_row AS ventilation
	ON c.stay_id = ventilation.stay_id
	WHERE c.itemid = 223758 AND c.value IN 
	('DNR / DNI','DNI (do not intubate)', 'Comfort measures only', 'DNR (do not resuscitate)')
),
ventilation_events_exclude_DNRDNI AS (
	SELECT
		ventilation.subject_id,
		ventilation.stay_id,
		ventilation.starttime,
		ventilation.endtime
	FROM
		ventilation_events_first_row as ventilation
	LEFT JOIN stay_id_with_DNRDNI AS dnr
	ON ventilation.stay_id = dnr.stay_id
	WHERE dnr.stay_id IS NULL
),
ventilation_events_exclude_age AS (
	SELECT DISTINCT
		ventilation.subject_id,
		ventilation.stay_id,
		ventilation.starttime,
		ventilation.endtime
	FROM physionet-data.mimiciv_derived.age as a
	INNER JOIN ventilation_events_exclude_DNRDNI as ventilation
	ON a.subject_id = ventilation.subject_id
	WHERE age >= 20
),
Cohort AS (
	SELECT * FROM ventilation_events_exclude_age
)

SELECT *
FROM Cohort
ORDER BY stay_id
```
## Features (Baseline & Charttime)
- Baseline
	- race, gender, age, weight, height, tobacco
	- NOTE: since race will have one to mapping, currently have 11921 rows, need to be fixed at python
```sql=
WITH ventilation_events AS (
	SELECT
		subject_id,
		i.stay_id,
		starttime,
		endtime,
	ROW_NUMBER() OVER (PARTITION BY i.stay_id ORDER BY starttime) AS row_num
	FROM physionet-data.mimiciv_derived.ventilation as ventilation
	INNER JOIN physionet-data.mimiciv_icu.icustays as i
	ON ventilation.stay_id = i.stay_id
	WHERE TIMESTAMP_DIFF(endtime, starttime, SECOND) >= 24 * 3600
	AND ventilation_status = 'InvasiveVent'
),
ventilation_events_first_row AS (
	SELECT
		subject_id,
		stay_id,
		starttime,
		endtime
	FROM ventilation_events
	WHERE row_num = 1
),
stay_id_with_DNRDNI AS (
	SELECT DISTINCT c.stay_id,
	FROM `physionet-data.mimiciv_icu.chartevents` AS c
	INNER JOIN ventilation_events_first_row AS ventilation
	ON c.stay_id = ventilation.stay_id
	WHERE c.itemid = 223758 AND c.value IN 
	('DNR / DNI','DNI (do not intubate)', 'Comfort measures only', 'DNR (do not resuscitate)')
),
ventilation_events_exclude_DNRDNI AS (
	SELECT
		ventilation.subject_id,
		ventilation.stay_id,
		ventilation.starttime,
		ventilation.endtime
	FROM
		ventilation_events_first_row as ventilation
	LEFT JOIN stay_id_with_DNRDNI AS dnr
	ON ventilation.stay_id = dnr.stay_id
	WHERE dnr.stay_id IS NULL
),
ventilation_events_exclude_age AS (
	SELECT DISTINCT
		ventilation.subject_id,
		ventilation.stay_id,
		ventilation.starttime,
		ventilation.endtime
	FROM physionet-data.mimiciv_derived.age as a
	INNER JOIN ventilation_events_exclude_DNRDNI as ventilation
	ON a.subject_id = ventilation.subject_id
	WHERE age >= 20 AND age <= 100
),
Cohort AS (
	SELECT * FROM ventilation_events_exclude_age
),
-- baseline
baseline_gender_race AS (
  SELECT DISTINCT
		c.subject_id,
		c.stay_id,
		p.gender,
		a.race
  FROM Cohort as c
  JOIN physionet-data.mimiciv_hosp.patients as p
	ON p.subject_id = c.subject_id
  JOIN physionet-data.mimiciv_icu.icustays as icu
	ON icu.stay_id = c.stay_id
  JOIN physionet-data.mimiciv_hosp.admissions as a
	ON a.subject_id = c.subject_id
),
baseline_age_weight_height_tobacco AS (
  WITH all_weight_height_tobacco AS (
    SELECT
    c.stay_id,
		age.age,
    (CASE WHEN c_event.itemid = 226512 THEN c_event.value ELSE NULL END) AS weight_kg,
    (CASE WHEN c_event.itemid = 226730 THEN c_event.value ELSE NULL END) AS height_cm,
    (CASE WHEN c_event.itemid = 227687 THEN 1 ELSE 0 END) AS tobacco
    FROM Cohort AS c
    JOIN physionet-data.mimiciv_icu.chartevents AS c_event
		ON c.stay_id = c_event.stay_id
		JOIN physionet-data.mimiciv_derived.age AS age
		ON c.subject_id = age.subject_id
  )
  SELECT DISTINCT
	stay_id,
	MAX(age) AS age,
	MAX(weight_kg) AS weight_kg,
	MAX(height_cm) AS height_cm,
	MAX(tobacco) AS tobacco
  FROM all_weight_height_tobacco
  GROUP BY stay_id
),
baseline_sepsis AS (
    SELECT DISTINCT stay_id, 1 AS sepsis
    FROM physionet-data.mimiciv_derived.sepsis3
),
baseline_ards AS (
    SELECT DISTINCT d.subject_id, 1 AS ards
    FROM physionet-data.mimiciv_hosp.diagnoses_icd AS d
    WHERE d.icd_code IN ('51882', 'J80')
),
baseline_features AS (
    SELECT
        b_gender_race.subject_id,
        b_age_weight_height_tobacco.stay_id,
        gender,
        race,
        age,
        weight_kg AS weight,
        height_cm AS height,
        tobacco,
        COALESCE(sepsis.sepsis, 0) AS sepsis,
        COALESCE(ards.ards, 0) AS ards
    FROM baseline_gender_race AS b_gender_race
    JOIN baseline_age_weight_height_tobacco AS b_age_weight_height_tobacco
    ON b_gender_race.stay_id = b_age_weight_height_tobacco.stay_id
    LEFT JOIN baseline_sepsis AS sepsis
    ON b_gender_race.stay_id = sepsis.stay_id
    LEFT JOIN baseline_ards AS ards
    ON b_gender_race.subject_id = ards.subject_id
)
SELECT * FROM baseline_features;
```

- Charttime
	- State: HR, RR, sbp, dbp, mbp, spO2, GCS, tidal volume observed, RSBI, minute ventilation
	- Action: peep, fio2, respiratory rate set, tidal volume set, plateau pressure, (ventilator mode group)
	- stay_id
	- ICD code for each features
```sql=
WITH ventilation_events AS (
	SELECT
		subject_id,
		i.stay_id,
		starttime,
		endtime,
	ROW_NUMBER() OVER (PARTITION BY i.stay_id ORDER BY starttime) AS row_num
	FROM physionet-data.mimiciv_derived.ventilation as ventilation
	INNER JOIN physionet-data.mimiciv_icu.icustays as i
	ON ventilation.stay_id = i.stay_id
	WHERE TIMESTAMP_DIFF(endtime, starttime, SECOND) >= 24 * 3600
	AND ventilation_status = 'InvasiveVent'
),
ventilation_events_first_row AS (
	SELECT
		subject_id,
		stay_id,
		starttime,
		endtime
	FROM ventilation_events
	WHERE row_num = 1
),
stay_id_with_DNRDNI AS (
	SELECT DISTINCT c.stay_id,
	FROM `physionet-data.mimiciv_icu.chartevents` AS c
	INNER JOIN ventilation_events_first_row AS ventilation
	ON c.stay_id = ventilation.stay_id
	WHERE c.itemid = 223758 AND c.value IN 
	('DNR / DNI','DNI (do not intubate)', 'Comfort measures only', 'DNR (do not resuscitate)')
),
ventilation_events_exclude_DNRDNI AS (
	SELECT
		ventilation.subject_id,
		ventilation.stay_id,
		ventilation.starttime,
		ventilation.endtime
	FROM
		ventilation_events_first_row as ventilation
	LEFT JOIN stay_id_with_DNRDNI AS dnr
	ON ventilation.stay_id = dnr.stay_id
	WHERE dnr.stay_id IS NULL
),
ventilation_events_exclude_age AS (
	SELECT DISTINCT
		ventilation.subject_id,
		ventilation.stay_id,
		ventilation.starttime,
		ventilation.endtime
	FROM physionet-data.mimiciv_derived.age as a
	INNER JOIN ventilation_events_exclude_DNRDNI as ventilation
	ON a.subject_id = ventilation.subject_id
	WHERE age >= 20 AND age <= 100
),
Cohort AS (
	SELECT * FROM ventilation_events_exclude_age
),
-- vital sign
gcs_vitalsign AS (
	SELECT
	c.stay_id,
	c.subject_id,
	gcs.charttime,
	gcs.gcs
	FROM Cohort AS c
	JOIN physionet-data.mimiciv_derived.gcs AS gcs
	ON c.stay_id = gcs.stay_id
),
vitalsign AS (
  SELECT 
  c.subject_id,
  c.stay_id,
  vital.charttime,
  vital.heart_rate,
  vital.resp_rate,
  vital.sbp,
  vital.dbp,
  vital.mbp,
  vital.spo2
  FROM Cohort as c
  JOIN physionet-data.mimiciv_derived.vitalsign as vital
	ON vital.stay_id = c.stay_id
)
SELECT * FROM vitalsign
-- SELECT * FROM gcs_vitalsign -- TODO: need to run both SELECT
```
```sql=
WITH ventilation_events AS (
	SELECT
		subject_id,
		i.stay_id,
		starttime,
		endtime,
	ROW_NUMBER() OVER (PARTITION BY i.stay_id ORDER BY starttime) AS row_num
	FROM physionet-data.mimiciv_derived.ventilation as ventilation
	INNER JOIN physionet-data.mimiciv_icu.icustays as i
	ON ventilation.stay_id = i.stay_id
	WHERE TIMESTAMP_DIFF(endtime, starttime, SECOND) >= 24 * 3600
	AND ventilation_status = 'InvasiveVent'
),
ventilation_events_first_row AS (
	SELECT
		subject_id,
		stay_id,
		starttime,
		endtime
	FROM ventilation_events
	WHERE row_num = 1
),
stay_id_with_DNRDNI AS (
	SELECT DISTINCT c.stay_id,
	FROM `physionet-data.mimiciv_icu.chartevents` AS c
	INNER JOIN ventilation_events_first_row AS ventilation
	ON c.stay_id = ventilation.stay_id
	WHERE c.itemid = 223758 AND c.value IN 
	('DNR / DNI','DNI (do not intubate)', 'Comfort measures only', 'DNR (do not resuscitate)')
),
ventilation_events_exclude_DNRDNI AS (
	SELECT
		ventilation.subject_id,
		ventilation.stay_id,
		ventilation.starttime,
		ventilation.endtime
	FROM
		ventilation_events_first_row as ventilation
	LEFT JOIN stay_id_with_DNRDNI AS dnr
	ON ventilation.stay_id = dnr.stay_id
	WHERE dnr.stay_id IS NULL
),
ventilation_events_exclude_age AS (
	SELECT DISTINCT
		ventilation.subject_id,
		ventilation.stay_id,
		ventilation.starttime,
		ventilation.endtime
	FROM physionet-data.mimiciv_derived.age as a
	INNER JOIN ventilation_events_exclude_DNRDNI as ventilation
	ON a.subject_id = ventilation.subject_id
	WHERE age >= 20 AND age <= 100
),
Cohort AS (
	SELECT * FROM ventilation_events_exclude_age
),
-- ventilator_settings
ven_setting AS (
  SELECT
  c.stay_id,
  c.subject_id,
  charttime,
  peep,
  fio2,
  tidal_volume_observed,
  tidal_volume_set,
  respiratory_rate_set,
  plateau_pressure,
  ventilator_mode,
  FROM Cohort as c
  JOIN physionet-data.mimiciv_derived.ventilator_setting AS v
	ON c.stay_id = v.stay_id
)
SELECT * FROM ven_setting

```
## Ground Truth
- extubation outcome
	- stay_id
	- reintubation_flag
	- reintubation_time
	- expired_in_48hr_flag
	- expired_in_hospital_flag
	- expired_time
	- negative
		- reintubation_flag or expired_in_hospital_flag
	- positive
		- others
```sql=
WITH ventilation_events AS (
	SELECT
		subject_id,
		i.stay_id,
		starttime,
		endtime,
	ROW_NUMBER() OVER (PARTITION BY i.stay_id ORDER BY starttime) AS row_num
	FROM physionet-data.mimiciv_derived.ventilation as ventilation
	INNER JOIN physionet-data.mimiciv_icu.icustays as i
	ON ventilation.stay_id = i.stay_id
	WHERE TIMESTAMP_DIFF(endtime, starttime, SECOND) >= 24 * 3600
	AND ventilation_status = 'InvasiveVent'
),
ventilation_events_first_row AS (
	SELECT
		subject_id,
		stay_id,
		starttime,
		endtime
	FROM ventilation_events
	WHERE row_num = 1
),
stay_id_with_DNRDNI AS (
	SELECT DISTINCT c.stay_id,
	FROM `physionet-data.mimiciv_icu.chartevents` AS c
	INNER JOIN ventilation_events_first_row AS ventilation
	ON c.stay_id = ventilation.stay_id
	WHERE c.itemid = 223758 AND c.value IN 
	('DNR / DNI','DNI (do not intubate)', 'Comfort measures only', 'DNR (do not resuscitate)')
),
ventilation_events_exclude_DNRDNI AS (
	SELECT
		ventilation.subject_id,
		ventilation.stay_id,
		ventilation.starttime,
		ventilation.endtime
	FROM
		ventilation_events_first_row as ventilation
	LEFT JOIN stay_id_with_DNRDNI AS dnr
	ON ventilation.stay_id = dnr.stay_id
	WHERE dnr.stay_id IS NULL
),
ventilation_events_exclude_age AS (
	SELECT DISTINCT
		ventilation.subject_id,
		ventilation.stay_id,
		ventilation.starttime,
		ventilation.endtime
	FROM physionet-data.mimiciv_derived.age as a
	INNER JOIN ventilation_events_exclude_DNRDNI as ventilation
	ON a.subject_id = ventilation.subject_id
	WHERE age >= 20 AND age <= 100
),
Cohort AS (
	SELECT * FROM ventilation_events_exclude_age
),
reintubation AS (
    SELECT DISTINCT stay_id
    FROM ventilation_events
    WHERE row_num = 2
),
ventilation_with_gap AS (
    SELECT
        v.stay_id,
        v.starttime,
        v.endtime,
        LAG(v.endtime) OVER (PARTITION BY v.stay_id ORDER BY v.starttime) AS previous_endtime,
        v.row_num
    FROM
        ventilation_events AS v
    RIGHT JOIN reintubation AS m
    ON v.stay_id = m.stay_id
    WHERE v.row_num <= 2
),
reintubation_time_gap AS (
    SELECT
        stay_id,
        starttime,
        endtime,
        CASE 
            WHEN row_num = 2 THEN TIMESTAMP_DIFF(starttime, previous_endtime, HOUR)
            ELSE NULL
        END AS reintubation_time_gap_hr
    FROM ventilation_with_gap
    ORDER BY stay_id, starttime
),
reintubation_time_gap_one_row AS (
    SELECT *
    FROM reintubation_time_gap
    WHERE reintubation_time_gap_hr IS NOT NULL
),
deathRanked AS (
    SELECT
        stay_id,
        deathtime,
        ROW_NUMBER() OVER (PARTITION BY stay_id ORDER BY deathtime ASC) AS row_num
    FROM Cohort AS c
    LEFT JOIN physionet-data.mimiciv_hosp.admissions AS a
    ON c.subject_id = a.subject_id
    WHERE a.deathtime IS NOT NULL
),
death AS (
    SELECT
        stay_id,
        deathtime
    FROM DeathRanked
    WHERE row_num = 1
)

SELECT
	c.subject_id,
	c.stay_id,
	c.starttime,
	c.endtime,
	r.reintubation_time_gap_hr,
	d.deathtime
FROM Cohort AS c
LEFT JOIN reintubation_time_gap_one_row AS r
ON c.stay_id = r.stay_id
LEFT JOIN death AS d
ON c.stay_id = d.stay_id
ORDER BY c.stay_id

-- death AS (
--     SELECT
--         stay_id,
--         dod
--     FROM Cohort as c
--     JOIN physionet-data.mimiciv_hosp.patients as p
--     ON c.subject_id = p.subject_id
-- )
```
- Last state
	- positive
	- negative
- Pre-processing
	- outliers
# MIMIC-III SQL
## Cohort
- Primary Key for each Trajectory
	- condition
		- continuous intubation time >= 24hours: 4715
		- exclude the "icustay_id" which has DNR/DNI: 4715 - 472 = 4243
		- age >= 20 and <= 100: 4243 - 139 = 4104
		- included ventilation modes: Complete, Minimal support
```sql=
WITH ventilation_events AS (
    SELECT 
      subject_id,
      icustay_id,
      starttime,
      endtime,
      ROW_NUMBER() OVER (PARTITION BY icustay_id ORDER BY STARTTIME ASC) AS row_num
    FROM `physionet-data.mimiciii_clinical.procedureevents_mv`
    WHERE ITEMID = 225792 -- 225792: Invasive Ventilation
    AND TIMESTAMP_DIFF(ENDTIME, STARTTIME, SECOND) >= 24 * 3600 -- Ensure 24+ hours of ventilation
),
ventilation_events_first_row AS (
    SELECT
      subject_id,
      icustay_id,
      starttime,
      endtime
    FROM ventilation_events
    WHERE row_num = 1
), 
stay_id_with_DNRDNI AS (
    SELECT DISTINCT c.icustay_id
    FROM `physionet-data.mimiciii_derived.code_status` c
    INNER JOIN ventilation_events_first_row v ON c.icustay_id = v.icustay_id
    WHERE 
        c.dnr_first = 1  -- DNR at first recorded status
        OR c.dni_first = 1  -- DNI at first recorded status
        OR c.dnr_last = 1   -- DNR at last recorded status
        OR c.dni_last = 1   -- DNI at last recorded status
),
ventilation_events_exclude_DNRDNI AS (
    SELECT v.*
    FROM ventilation_events v
    LEFT JOIN stay_id_with_DNRDNI dnr ON v.icustay_id = dnr.icustay_id
    WHERE dnr.icustay_id IS NULL
)
, patient_age AS (
    SELECT p subject_id, i.icustay_id,
           TIMESTAMP_DIFF(i.intime, p.dob, YEAR) AS age
    FROM `physionet-data.mimiciii_clinical.patients` p
    INNER JOIN `physionet-data.mimiciii_clinical.icustays` i ON p.subject_id = i.subject_id
),
ventilation_events_exclude_age AS (
    SELECT DISTINCT v.*
    FROM ventilation_events_exclude_DNRDNI v
    INNER JOIN patient_age a ON v.icustay_id = a.icustay_id
    WHERE a.age >= 20 and a.age <= 100
),
Cohort AS (
	SELECT
    subject_id,
    icustay_id,
    starttime,
    endtime
  FROM ventilation_events_exclude_age
  WHERE row_num = 1
  ORDER BY icustay_id
)

SELECT *
FROM Cohort
ORDER BY icustay_id
```
## Features (Baseline & Charttime)
- Baseline
```sql=
WITH ventilation_events AS (
    SELECT 
      subject_id,
      icustay_id,
      starttime,
      endtime,
      ROW_NUMBER() OVER (PARTITION BY icustay_id ORDER BY STARTTIME ASC) AS row_num
    FROM `physionet-data.mimiciii_clinical.procedureevents_mv`
    WHERE ITEMID = 225792 -- 225792: Invasive Ventilation
    AND TIMESTAMP_DIFF(ENDTIME, STARTTIME, SECOND) >= 24 * 3600 -- Ensure 24+ hours of ventilation
),
ventilation_events_first_row AS (
    SELECT
      subject_id,
      icustay_id,
      starttime,
      endtime
    FROM ventilation_events
    WHERE row_num = 1
), 
stay_id_with_DNRDNI AS (
    SELECT DISTINCT c.icustay_id
    FROM `physionet-data.mimiciii_derived.code_status` c
    INNER JOIN ventilation_events_first_row v ON c.icustay_id = v.icustay_id
    WHERE 
        c.dnr_first = 1  -- DNR at first recorded status
        OR c.dni_first = 1  -- DNI at first recorded status
        OR c.dnr_last = 1   -- DNR at last recorded status
        OR c.dni_last = 1   -- DNI at last recorded status
),
ventilation_events_exclude_DNRDNI AS (
    SELECT v.*
    FROM ventilation_events v
    LEFT JOIN stay_id_with_DNRDNI dnr ON v.icustay_id = dnr.icustay_id
    WHERE dnr.icustay_id IS NULL
),
patient_age AS (
    SELECT p.subject_id, i.icustay_id,
           TIMESTAMP_DIFF(i.intime, p.dob, YEAR) AS age
    FROM `physionet-data.mimiciii_clinical.patients` p
    INNER JOIN `physionet-data.mimiciii_clinical.icustays` i ON p.subject_id = i.subject_id
),
ventilation_events_exclude_age AS (
    SELECT DISTINCT v.*
    FROM ventilation_events_exclude_DNRDNI v
    INNER JOIN patient_age a ON v.icustay_id = a.icustay_id
    WHERE a.age >= 20 AND a.age <= 100
),
Cohort AS (
    SELECT
        subject_id,
        icustay_id,
        starttime,
        endtime
    FROM ventilation_events_exclude_age
    WHERE row_num = 1
    ORDER BY icustay_id
),
baseline_gender_race AS (
    SELECT DISTINCT
        c.subject_id,
        c.icustay_id,
        p.gender,
        a.ethnicity AS race
    FROM Cohort c
    JOIN `physionet-data.mimiciii_clinical.patients` p
        ON p.subject_id = c.subject_id
    JOIN `physionet-data.mimiciii_clinical.icustays` icu
        ON icu.icustay_id = c.icustay_id
    JOIN `physionet-data.mimiciii_clinical.admissions` a
        ON a.hadm_id = icu.hadm_id AND a.subject_id = c.subject_id
),
baseline_weight_height_tobacco AS (
    WITH all_weight_height_tobacco AS (
        SELECT 
            c.icustay_id,
            (CASE WHEN c_event.itemid IN (224639, 226512) THEN c_event.valuenum ELSE NULL END) AS weight_kg,
            (CASE WHEN c_event.itemid IN (226707, 226730) THEN c_event.valuenum ELSE NULL END) AS height_cm,
            (CASE 
                WHEN c_event.itemid = 227687 AND c_event.value IN ('Current use or use within 1 month of admission', 
                                                                   'Former user - stopped more than 1 year ago',
                                                                   'Stopped more than 1 month ago, but less than 1 year ago') THEN 1
                WHEN c_event.itemid = 227687 THEN 0
                ELSE 0 
            END) AS tobacco
        FROM 
            Cohort c
        JOIN 
            `physionet-data.mimiciii_clinical.chartevents` c_event 
        ON 
            c.icustay_id = c_event.icustay_id
        WHERE 
            c_event.itemid IN (224639, 226512, 226707, 226730, 227687)
            AND (c_event.valuenum IS NOT NULL OR c_event.itemid = 227687)
            AND (c_event.valuenum > 0 OR c_event.itemid = 227687)
            AND c_event.error IS DISTINCT FROM 1
    )
    SELECT 
        DISTINCT icustay_id,
        MAX(weight_kg) AS weight_kg,
        MAX(height_cm) AS height_cm,
        MAX(tobacco) AS tobacco
    FROM 
        all_weight_height_tobacco
    GROUP BY 
        icustay_id
),
baseline_sepsis AS (
    SELECT DISTINCT subject_id, 1 AS sepsis
    FROM `physionet-data.mimiciii_clinical.diagnoses_icd`
    WHERE icd9_code LIKE '99591' OR icd9_code LIKE '99592' OR icd9_code LIKE '78552'
),
baseline_ards AS (
    SELECT DISTINCT subject_id, 1 AS ards
    FROM `physionet-data.mimiciii_clinical.diagnoses_icd`
    WHERE icd9_code LIKE '51882'
),
baseline_features AS (
    SELECT
        bgr.subject_id,
        bgr.icustay_id,
        bgr.gender,
        bgr.race,
        v.age,
        COALESCE(bwht.weight_kg, 0) AS weight,
        COALESCE(bwht.height_cm, 0) AS height,
        COALESCE(bwht.tobacco, 0) AS tobacco,
        COALESCE(sepsis.sepsis, 0) AS sepsis,
        COALESCE(ards.ards, 0) AS ards
    FROM baseline_gender_race bgr
    JOIN patient_age v
        ON bgr.icustay_id = v.icustay_id
    LEFT JOIN baseline_weight_height_tobacco bwht
        ON bgr.icustay_id = bwht.icustay_id
    LEFT JOIN baseline_sepsis sepsis
        ON bgr.subject_id = sepsis.subject_id
    LEFT JOIN baseline_ards ards
        ON bgr.subject_id = ards.subject_id
)
SELECT * FROM baseline_features
ORDER BY icustay_id;
```
- Charttime (havn't test)
```sql=
WITH ventilation_events AS (
    SELECT 
      subject_id,
      icustay_id,
      starttime,
      endtime,
      ROW_NUMBER() OVER (PARTITION BY icustay_id ORDER BY STARTTIME ASC) AS row_num
    FROM `physionet-data.mimiciii_clinical.procedureevents_mv`
    WHERE ITEMID = 225792 -- 225792: Invasive Ventilation
    AND TIMESTAMP_DIFF(ENDTIME, STARTTIME, SECOND) >= 24 * 3600 -- Ensure 24+ hours of ventilation
),
ventilation_events_first_row AS (
    SELECT
      subject_id,
      icustay_id,
      starttime,
      endtime
    FROM ventilation_events
    WHERE row_num = 1
), 
stay_id_with_DNRDNI AS (
    SELECT DISTINCT c.icustay_id
    FROM `physionet-data.mimiciii_derived.code_status` c
    INNER JOIN ventilation_events_first_row v ON c.icustay_id = v.icustay_id
    WHERE 
        c.dnr_first = 1  -- DNR at first recorded status
        OR c.dni_first = 1  -- DNI at first recorded status
        OR c.dnr_last = 1   -- DNR at last recorded status
        OR c.dni_last = 1   -- DNI at last recorded status
),
ventilation_events_exclude_DNRDNI AS (
    SELECT v.*
    FROM ventilation_events v
    LEFT JOIN stay_id_with_DNRDNI dnr ON v.icustay_id = dnr.icustay_id
    WHERE dnr.icustay_id IS NULL
)
, patient_age AS (
    SELECT p subject_id, i.icustay_id,
           TIMESTAMP_DIFF(i.intime, p.dob, YEAR) AS age
    FROM `physionet-data.mimiciii_clinical.patients` p
    INNER JOIN `physionet-data.mimiciii_clinical.icustays` i ON p.subject_id = i.subject_id
),
ventilation_events_exclude_age AS (
    SELECT DISTINCT v.*
    FROM ventilation_events_exclude_DNRDNI v
    INNER JOIN patient_age a ON v.icustay_id = a.icustay_id
    WHERE a.age >= 20 and a.age <= 100
),
Cohort AS (
	SELECT
    subject_id,
    icustay_id,
    starttime,
    endtime
  FROM ventilation_events_exclude_age
  WHERE row_num = 1
  ORDER BY icustay_id
)
  
-- GCS vital sign - first getting the components
, gcs_components AS (
    SELECT
        c.icustay_id,
        c.subject_id,
        ce.charttime,
        CASE WHEN ce.itemid = 220739 THEN ce.valuenum ELSE NULL END AS gcs_eye,
        CASE WHEN ce.itemid = 223900 THEN ce.valuenum ELSE NULL END AS gcs_verbal,
        CASE WHEN ce.itemid = 223901 THEN ce.valuenum ELSE NULL END AS gcs_motor,
        CASE WHEN ce.itemid = 198 THEN ce.valuenum ELSE NULL END AS gcs_total
    FROM Cohort AS c
    JOIN `physionet-data.mimiciii_clinical.chartevents` AS ce
        ON c.icustay_id = ce.icustay_id
    WHERE ce.itemid IN 
        (
            198,    -- GCS Total
            220739, -- GCS - Eye Opening
            223900, -- GCS - Verbal Response
            223901  -- GCS - Motor Response
        )
        AND ce.error IS DISTINCT FROM 1
),

-- Now aggregate the components
gcs_vitalsign AS (
    SELECT
        icustay_id,
        subject_id,
        charttime,
        MAX(gcs_eye) AS gcs_eye,
        MAX(gcs_verbal) AS gcs_verbal,
        MAX(gcs_motor) AS gcs_motor, 
        COALESCE(
            MAX(gcs_total),
            CASE 
                WHEN MAX(gcs_eye) IS NOT NULL AND MAX(gcs_verbal) IS NOT NULL AND MAX(gcs_motor) IS NOT NULL
                THEN MAX(gcs_eye) + MAX(gcs_verbal) + MAX(gcs_motor)
                ELSE NULL 
            END
        ) AS gcs
    FROM gcs_components
    GROUP BY icustay_id, subject_id, charttime
),

-- Other vital signs
vitalsign AS (
    SELECT 
        c.subject_id,
        c.icustay_id,
        ce.charttime,
        -- Heart rate
        MAX(CASE WHEN ce.itemid IN (211, 220045) THEN ce.valuenum ELSE NULL END) AS heart_rate,
        -- Respiratory rate
        MAX(CASE WHEN ce.itemid IN (618, 220210) THEN ce.valuenum ELSE NULL END) AS resp_rate,
        -- Systolic BP
        MAX(CASE WHEN ce.itemid IN (51, 442, 455, 6701, 220179, 220050) THEN ce.valuenum ELSE NULL END) AS sbp,
        -- Diastolic BP
        MAX(CASE WHEN ce.itemid IN (8368, 8440, 8441, 8555, 220180, 220051) THEN ce.valuenum ELSE NULL END) AS dbp,
        -- Mean BP
        MAX(CASE WHEN ce.itemid IN (456, 52, 6702, 443, 220052, 220181, 225312) THEN ce.valuenum ELSE NULL END) AS mbp,
        -- SpO2
        MAX(CASE WHEN ce.itemid IN (646, 220277) THEN ce.valuenum ELSE NULL END) AS spo2
    FROM Cohort as c
    JOIN `physionet-data.mimiciii_clinical.chartevents` as ce
        ON ce.icustay_id = c.icustay_id
    WHERE ce.itemid IN 
        (
            -- Heart rate
            211, 220045,
            -- Respiratory rate
            618, 220210,
            -- Systolic BP
            51, 442, 455, 6701, 220179, 220050,
            -- Diastolic BP
            8368, 8440, 8441, 8555, 220180, 220051,
            -- Mean BP
            456, 52, 6702, 443, 220052, 220181, 225312,
            -- SpO2
            646, 220277
        )
        AND ce.valuenum IS NOT NULL
        AND ce.error IS DISTINCT FROM 1
    GROUP BY c.subject_id, c.icustay_id, ce.charttime
)

-- Run both queries
SELECT * FROM vitalsign
-- SELECT * FROM gcs_vitalsign;

```
- ventilator settings
```sql=
WITH ventilation_events AS (
    SELECT 
      subject_id,
      icustay_id,
      starttime,
      endtime,
      ROW_NUMBER() OVER (PARTITION BY icustay_id ORDER BY STARTTIME ASC) AS row_num
    FROM `physionet-data.mimiciii_clinical.procedureevents_mv`
    WHERE ITEMID = 225792 -- 225792: Invasive Ventilation
    AND TIMESTAMP_DIFF(ENDTIME, STARTTIME, SECOND) >= 24 * 3600 -- Ensure 24+ hours of ventilation
),
ventilation_events_first_row AS (
    SELECT
      subject_id,
      icustay_id,
      starttime,
      endtime
    FROM ventilation_events
    WHERE row_num = 1
), 
stay_id_with_DNRDNI AS (
    SELECT DISTINCT c.icustay_id
    FROM `physionet-data.mimiciii_derived.code_status` c
    INNER JOIN ventilation_events_first_row v ON c.icustay_id = v.icustay_id
    WHERE 
        c.dnr_first = 1  -- DNR at first recorded status
        OR c.dni_first = 1  -- DNI at first recorded status
        OR c.dnr_last = 1   -- DNR at last recorded status
        OR c.dni_last = 1   -- DNI at last recorded status
),
ventilation_events_exclude_DNRDNI AS (
    SELECT v.*
    FROM ventilation_events v
    LEFT JOIN stay_id_with_DNRDNI dnr ON v.icustay_id = dnr.icustay_id
    WHERE dnr.icustay_id IS NULL
)
, patient_age AS (
    SELECT p subject_id, i.icustay_id,
           TIMESTAMP_DIFF(i.intime, p.dob, YEAR) AS age
    FROM `physionet-data.mimiciii_clinical.patients` p
    INNER JOIN `physionet-data.mimiciii_clinical.icustays` i ON p.subject_id = i.subject_id
),
ventilation_events_exclude_age AS (
    SELECT DISTINCT v.*
    FROM ventilation_events_exclude_DNRDNI v
    INNER JOIN patient_age a ON v.icustay_id = a.icustay_id
    WHERE a.age >= 20 and a.age <= 100
),
Cohort AS (
	SELECT
    subject_id,
    icustay_id,
    starttime,
    endtime
  FROM ventilation_events_exclude_age
  WHERE row_num = 1
  ORDER BY icustay_id
)
  
-- ventilator_settings
, ven_setting AS (
    SELECT
        c.icustay_id,
        c.subject_id,
        ce.charttime,
        -- PEEP
        MAX(CASE WHEN ce.itemid IN (505, 506, 220339) THEN ce.valuenum ELSE NULL END) AS peep,
        -- FiO2
        MAX(CASE 
            WHEN ce.itemid IN (190, 223835) AND ce.valuenum <= 1 THEN ce.valuenum * 100 -- Convert to percentage if stored as fraction
            WHEN ce.itemid IN (190, 223835) AND ce.valuenum > 1 THEN ce.valuenum 
            ELSE NULL END
        ) AS fio2,
        -- Tidal Volume (observed)
        MAX(CASE WHEN ce.itemid IN (639, 654, 681, 682, 683, 684, 224685, 224686) THEN ce.valuenum ELSE NULL END) AS tidal_volume_observed,
        -- Tidal Volume (set)
        MAX(CASE WHEN ce.itemid IN (639, 654, 681, 682, 683, 684, 224685, 224684) THEN ce.valuenum ELSE NULL END) AS tidal_volume_set,
        -- Respiratory Rate (set)
        MAX(CASE WHEN ce.itemid IN (615, 618, 619, 224688, 224689, 224690) THEN ce.valuenum ELSE NULL END) AS respiratory_rate_set,
        -- Plateau Pressure
        MAX(CASE WHEN ce.itemid IN (224696, 543) THEN ce.valuenum ELSE NULL END) AS plateau_pressure,
        -- Ventilator Mode
        MAX(CASE WHEN ce.itemid IN (720, 223849) THEN ce.value ELSE NULL END) AS ventilator_mode
    FROM Cohort AS c
    JOIN `physionet-data.mimiciii_clinical.chartevents` AS ce
        ON c.icustay_id = ce.icustay_id
    WHERE ce.itemid IN (
        -- PEEP
        505, 506, 220339,
        -- FiO2
        190, 223835,
        -- Tidal Volume
        639, 654, 681, 682, 683, 684, 224685, 224686, 224684,
        -- Respiratory Rate
        615, 618, 619, 224688, 224689, 224690,
        -- Plateau Pressure
        224696, 543,
        -- Ventilator Mode
        720, 223849
    )
    AND ce.error IS DISTINCT FROM 1
    GROUP BY c.icustay_id, c.subject_id, ce.charttime
)

SELECT * FROM ven_setting;

```
## Ground Truth
```sql=
WITH ventilation_events AS (
    SELECT 
      subject_id,
      icustay_id,
      starttime,
      endtime,
      ROW_NUMBER() OVER (PARTITION BY icustay_id ORDER BY STARTTIME ASC) AS row_num
    FROM `physionet-data.mimiciii_clinical.procedureevents_mv`
    WHERE ITEMID = 225792 -- 225792: Invasive Ventilation
    AND TIMESTAMP_DIFF(ENDTIME, STARTTIME, SECOND) >= 24 * 3600 -- Ensure 24+ hours of ventilation
),
ventilation_events_first_row AS (
    SELECT
      subject_id,
      icustay_id,
      starttime,
      endtime
    FROM ventilation_events
    WHERE row_num = 1
), 
stay_id_with_DNRDNI AS (
    SELECT DISTINCT c.icustay_id
    FROM `physionet-data.mimiciii_derived.code_status` c
    INNER JOIN ventilation_events_first_row v ON c.icustay_id = v.icustay_id
    WHERE 
        c.dnr_first = 1  -- DNR at first recorded status
        OR c.dni_first = 1  -- DNI at first recorded status
        OR c.dnr_last = 1   -- DNR at last recorded status
        OR c.dni_last = 1   -- DNI at last recorded status
),
ventilation_events_exclude_DNRDNI AS (
    SELECT v.*
    FROM ventilation_events v
    LEFT JOIN stay_id_with_DNRDNI dnr ON v.icustay_id = dnr.icustay_id
    WHERE dnr.icustay_id IS NULL
)
, patient_age AS (
    SELECT p subject_id, i.icustay_id,
           TIMESTAMP_DIFF(i.intime, p.dob, YEAR) AS age
    FROM `physionet-data.mimiciii_clinical.patients` p
    INNER JOIN `physionet-data.mimiciii_clinical.icustays` i ON p.subject_id = i.subject_id
),
ventilation_events_exclude_age AS (
    SELECT DISTINCT v.*
    FROM ventilation_events_exclude_DNRDNI v
    INNER JOIN patient_age a ON v.icustay_id = a.icustay_id
    WHERE a.age >= 20 and a.age <= 100
),
Cohort AS (
	SELECT
    subject_id,
    icustay_id,
    starttime,
    endtime
  FROM ventilation_events_exclude_age
  WHERE row_num = 1
  ORDER BY icustay_id
)
  
-- Identify patients with reintubation (second ventilation event)
, reintubation AS (
    SELECT DISTINCT icustay_id
    FROM ventilation_events
    WHERE row_num = 2
),

-- Get ventilation events with timing information
ventilation_with_gap AS (
    SELECT
        v.icustay_id,
        v.starttime,
        v.endtime,
        LAG(v.endtime) OVER (PARTITION BY v.icustay_id ORDER BY v.starttime) AS previous_endtime,
        v.row_num
    FROM
        ventilation_events AS v
    RIGHT JOIN reintubation AS r
    ON v.icustay_id = r.icustay_id
    WHERE v.row_num <= 2
),

-- Calculate the time gap between extubation and reintubation
reintubation_time_gap AS (
    SELECT
        icustay_id,
        starttime,
        endtime,
        CASE 
            WHEN row_num = 2 THEN TIMESTAMP_DIFF(starttime, previous_endtime, HOUR)
            ELSE NULL
        END AS reintubation_time_gap_hr
    FROM ventilation_with_gap
    ORDER BY icustay_id, starttime
),

-- Get only the rows with reintubation time gap
reintubation_time_gap_one_row AS (
    SELECT *
    FROM reintubation_time_gap
    WHERE reintubation_time_gap_hr IS NOT NULL
),

-- Identify death events
deathRanked AS (
    SELECT
        i.icustay_id,
        p.dod AS deathtime,
        ROW_NUMBER() OVER (PARTITION BY i.icustay_id ORDER BY p.dod ASC) AS row_num
    FROM Cohort AS c
    JOIN `physionet-data.mimiciii_clinical.icustays` AS i
    ON c.icustay_id = i.icustay_id
    JOIN `physionet-data.mimiciii_clinical.patients` AS p
    ON c.subject_id = p.subject_id
    WHERE p.dod IS NOT NULL
),

-- Get the first death event for each patient
death AS (
    SELECT
        icustay_id,
        deathtime
    FROM deathRanked
    WHERE row_num = 1
)

-- Final output
SELECT
    c.subject_id,
    c.icustay_id,
    c.starttime,
    c.endtime,
    r.reintubation_time_gap_hr,
    d.deathtime
FROM Cohort AS c
LEFT JOIN reintubation_time_gap_one_row AS r
ON c.icustay_id = r.icustay_id
LEFT JOIN death AS d
ON c.icustay_id = d.icustay_id
ORDER BY c.icustay_id;

-- death AS (
--     SELECT
--         c.icustay_id,
--         p.dod -- as deathtime, select part need to update as dod
--     FROM Cohort AS c
--     JOIN `physionet-data.mimiciii_clinical.patients` AS p
--     ON c.subject_id = p.subject_id
--     WHERE p.dod IS NOT NULL
-- )

```
# eICU
## Cohort
- condition
    - continuous intubation time >= 24hours: 13601
    - exclude the "patientunitstayid" which has DNR/DNI: 13601 - 3828 = 9773
    - age >= 20 and <= 100: 9773 - 178 = 9595
- [DNR/DNI](https://github.com/MIT-LCP/eicu-code/issues/90)
```sql=
-- Step 1: Collect relevant documentation times
WITH tm AS (
    SELECT patientunitstayid, respchartoffset AS charttime
    FROM `physionet-data.eicu_crd.respiratorycharting`
    WHERE respchartvalue IN (
        'Tracheostomy', 'Endotracheal tube', 'Bipap', 'CPAP', 'High flow nasal cannula',
        'Non-rebreather', 'Face tent', 'Nasal cannula', 'None'
    )
    UNION DISTINCT
    SELECT patientunitstayid, treatmentoffset AS charttime
    FROM `physionet-data.eicu_crd.treatment`
    WHERE treatmentstring LIKE '%mechanical ventilation%'
)

-- Step 2: Classify ventilation status
, vs AS (
    SELECT tm.patientunitstayid, tm.charttime,
        rc.respchartvalue AS o2_delivery_device,
        tr.treatmentstring AS procedure_event,
        
        -- Assign ventilation status
        CASE
            WHEN rc.respchartvalue = 'Tracheostomy' THEN 'Tracheostomy'
            WHEN rc.respchartvalue = 'Endotracheal tube' OR tr.treatmentstring LIKE '%mechanical ventilation%' THEN 'InvasiveVent'
            WHEN rc.respchartvalue IN ('Bipap', 'CPAP') THEN 'NonInvasiveVent'
            WHEN rc.respchartvalue = 'High flow nasal cannula' THEN 'HFNC'
            WHEN rc.respchartvalue IN ('Non-rebreather', 'Face tent', 'Nasal cannula') THEN 'SupplementalOxygen'
            WHEN rc.respchartvalue = 'None' THEN 'None'
            ELSE NULL 
        END AS ventilation_status
    FROM tm
    LEFT JOIN `physionet-data.eicu_crd.respiratorycharting` rc
        ON tm.patientunitstayid = rc.patientunitstayid
        AND tm.charttime = rc.respchartoffset
    LEFT JOIN `physionet-data.eicu_crd.treatment` tr
        ON tm.patientunitstayid = tr.patientunitstayid
        AND tm.charttime = tr.treatmentoffset
)

-- Step 3: Identify invasive ventilation events lasting ≥ 24 hours
, vd AS (
    SELECT patientunitstayid, charttime, ventilation_status,
        LAG(charttime) OVER (PARTITION BY patientunitstayid ORDER BY charttime) AS prev_time,
        LEAD(charttime) OVER (PARTITION BY patientunitstayid ORDER BY charttime) AS next_time
    FROM vs
    WHERE ventilation_status IS NOT NULL
)

, patient_admit_times AS (
    SELECT patientunitstayid, 
           TIMESTAMP_ADD(TIMESTAMP '1970-01-01 00:00:00 UTC', INTERVAL hospitaladmitoffset MINUTE) AS admit_dttm
    FROM `physionet-data.eicu_crd.patient`
)

, invasive_vent AS (
    SELECT vd.patientunitstayid, 
           MIN(TIMESTAMP_ADD(p.admit_dttm, INTERVAL vd.charttime MINUTE)) AS starttime,
           MAX(TIMESTAMP_ADD(p.admit_dttm, INTERVAL vd.charttime MINUTE)) AS endtime
    FROM vd
    JOIN patient_admit_times p 
        ON vd.patientunitstayid = p.patientunitstayid
    WHERE vd.ventilation_status = 'InvasiveVent'
    GROUP BY vd.patientunitstayid, p.admit_dttm
    HAVING TIMESTAMP_DIFF(endtime, starttime, SECOND) >= 24 * 3600
)

-- Step 4: Exclude DNR/DNI patients
, dnr_dni AS (
    SELECT DISTINCT patientunitstayid
    FROM `physionet-data.eicu_crd.careplangeneral`
    WHERE cplitemvalue like "Do not resuscitate" or cplitemvalue like "No intubation"
)

, invasive_vent_filtered AS (
    SELECT iv.*
    FROM invasive_vent iv
    LEFT JOIN dnr_dni dnr ON iv.patientunitstayid = dnr.patientunitstayid
    WHERE dnr.patientunitstayid IS NULL
)

-- Step 5: Exclude patients under 20 years old
, age_filtered AS (
    SELECT iv.*
    FROM invasive_vent_filtered iv
    INNER JOIN `physionet-data.eicu_crd.patient` p
    ON iv.patientunitstayid = p.patientunitstayid
    WHERE SAFE_CAST(p.age AS INT64) >= 20
    AND SAFE_CAST(p.age AS INT64) <= 100
)

, Cohort AS (
    SELECT *
    FROM age_filtered
)

-- Final selection
SELECT * FROM Cohort
ORDER BY patientunitstayid;

```
## Features (Baseline & Charttime)
- Baseline
- tobacco is wrong, but useless
```sql=
WITH tm AS (
    SELECT patientunitstayid, respchartoffset AS charttime
    FROM physionet-data.eicu_crd.respiratorycharting
    WHERE respchartvalue IN (
        'Tracheostomy', 'Endotracheal tube', 'Bipap', 'CPAP', 'High flow nasal cannula',
        'Non-rebreather', 'Face tent', 'Nasal cannula', 'None'
    )
    UNION DISTINCT
    SELECT patientunitstayid, treatmentoffset AS charttime
    FROM physionet-data.eicu_crd.treatment
    WHERE treatmentstring LIKE '%mechanical ventilation%'
)

-- Step 2: Classify ventilation status
, vs AS (
    SELECT tm.patientunitstayid, tm.charttime,
        rc.respchartvalue AS o2_delivery_device,
        tr.treatmentstring AS procedure_event,
        
        -- Assign ventilation status
        CASE
            WHEN rc.respchartvalue = 'Tracheostomy' THEN 'Tracheostomy'
            WHEN rc.respchartvalue = 'Endotracheal tube' OR tr.treatmentstring LIKE '%mechanical ventilation%' THEN 'InvasiveVent'
            WHEN rc.respchartvalue IN ('Bipap', 'CPAP') THEN 'NonInvasiveVent'
            WHEN rc.respchartvalue = 'High flow nasal cannula' THEN 'HFNC'
            WHEN rc.respchartvalue IN ('Non-rebreather', 'Face tent', 'Nasal cannula') THEN 'SupplementalOxygen'
            WHEN rc.respchartvalue = 'None' THEN 'None'
            ELSE NULL 
        END AS ventilation_status
    FROM tm
    LEFT JOIN physionet-data.eicu_crd.respiratorycharting rc
        ON tm.patientunitstayid = rc.patientunitstayid
        AND tm.charttime = rc.respchartoffset
    LEFT JOIN physionet-data.eicu_crd.treatment tr
        ON tm.patientunitstayid = tr.patientunitstayid
        AND tm.charttime = tr.treatmentoffset
)

-- Step 3: Identify invasive ventilation events lasting ≥ 24 hours
, vd AS (
    SELECT patientunitstayid, charttime, ventilation_status,
        LAG(charttime) OVER (PARTITION BY patientunitstayid ORDER BY charttime) AS prev_time,
        LEAD(charttime) OVER (PARTITION BY patientunitstayid ORDER BY charttime) AS next_time
    FROM vs
    WHERE ventilation_status IS NOT NULL
)

, patient_admit_times AS (
    SELECT patientunitstayid, 
           TIMESTAMP_ADD(TIMESTAMP '1970-01-01 00:00:00 UTC', INTERVAL hospitaladmitoffset MINUTE) AS admit_dttm
    FROM physionet-data.eicu_crd.patient
)

, invasive_vent AS (
    SELECT vd.patientunitstayid, 
           MIN(TIMESTAMP_ADD(p.admit_dttm, INTERVAL vd.charttime MINUTE)) AS starttime,
           MAX(TIMESTAMP_ADD(p.admit_dttm, INTERVAL vd.charttime MINUTE)) AS endtime
    FROM vd
    JOIN patient_admit_times p 
        ON vd.patientunitstayid = p.patientunitstayid
    WHERE vd.ventilation_status = 'InvasiveVent'
    GROUP BY vd.patientunitstayid, p.admit_dttm
    HAVING TIMESTAMP_DIFF(endtime, starttime, SECOND) >= 24 * 3600
)

-- Step 4: Exclude DNR/DNI patients
, dnr_dni AS (
    SELECT DISTINCT patientunitstayid
    FROM `physionet-data.eicu_crd.careplangeneral`
    WHERE cplitemvalue like "Do not resuscitate" or cplitemvalue like "No intubation"
)

, invasive_vent_filtered AS (
    SELECT iv.*
    FROM invasive_vent iv
    LEFT JOIN dnr_dni dnr ON iv.patientunitstayid = dnr.patientunitstayid
    WHERE dnr.patientunitstayid IS NULL
)

-- Step 5: Exclude patients under 20 years old
, age_filtered AS (
    SELECT iv.*
    FROM invasive_vent_filtered iv
    INNER JOIN physionet-data.eicu_crd.patient p
    ON iv.patientunitstayid = p.patientunitstayid
    WHERE SAFE_CAST(p.age AS INT64) >= 20
    AND SAFE_CAST(p.age AS INT64) <= 100
)

, Cohort AS (
    SELECT *
    FROM age_filtered
)

-- Now adding baseline features similar to the MIMIC query
, baseline_gender_race AS (
    SELECT DISTINCT
        c.patientunitstayid,
        p.gender,
        p.ethnicity AS race
    FROM Cohort AS c
    JOIN physionet-data.eicu_crd.patient AS p
    ON p.patientunitstayid = c.patientunitstayid
)

, baseline_age_weight_height AS (
    -- Get demographic and basic info in one efficient query
    SELECT 
        p.patientunitstayid,
        SAFE_CAST(p.age AS INT64) AS age,
        SAFE_CAST(p.admissionweight AS FLOAT64) AS weight_kg,
        SAFE_CAST(p.admissionheight AS FLOAT64) AS height_cm
    FROM Cohort AS c
    JOIN physionet-data.eicu_crd.patient AS p
        ON c.patientunitstayid = p.patientunitstayid
)

, tobacco_status AS (
    -- Separate tobacco status check for better performance
    SELECT 
        patientunitstayid,
        1 AS tobacco
    FROM physionet-data.eicu_crd.pasthistory
    WHERE (LOWER(pasthistorypath) LIKE '%tobacco%' OR LOWER(pasthistorypath) LIKE '%smok%')
    AND patientunitstayid IN (SELECT patientunitstayid FROM Cohort)
)

, baseline_age_weight_height_tobacco AS (
    -- Combine both with left join to maintain all cohort patients
    SELECT
        b.patientunitstayid,
        b.age,
        b.weight_kg AS weight_kg,
        b.height_cm AS height_cm,
        COALESCE(t.tobacco, 0) AS tobacco
    FROM baseline_age_weight_height b
    LEFT JOIN tobacco_status t
        ON b.patientunitstayid = t.patientunitstayid
)

, baseline_features AS (
    SELECT
        bgr.patientunitstayid,
        bgr.gender,
        bgr.race,
        bawht.age,
        bawht.weight_kg AS weight,
        bawht.height_cm AS height,
        bawht.tobacco
    FROM baseline_gender_race AS bgr
    JOIN baseline_age_weight_height_tobacco AS bawht
    ON bgr.patientunitstayid = bawht.patientunitstayid
)
-- Identify ARDS and Sepsis in eICU
, ards_patients AS (
    SELECT DISTINCT patientunitstayid
    FROM physionet-data.eicu_crd.diagnosis
    WHERE icd9code IN ('518.82') -- ICD-9 code for ARDS
),

sepsis_patients AS (
    SELECT DISTINCT patientunitstayid
    FROM physionet-data.eicu_crd_derived.sepsis_from_diagnosis
    WHERE sepsis = 1
)

SELECT 
    c.patientunitstayid,
    c.starttime,
    c.endtime,
    bf.gender,
    bf.race,
    bf.age,
    bf.weight,
    bf.height,
    bf.tobacco,
    CASE WHEN s.patientunitstayid IS NOT NULL THEN 1 ELSE 0 END AS sepsis,
    CASE WHEN a.patientunitstayid IS NOT NULL THEN 1 ELSE 0 END AS ards

FROM Cohort c
JOIN baseline_features bf ON c.patientunitstayid = bf.patientunitstayid
LEFT JOIN ards_patients a ON c.patientunitstayid = a.patientunitstayid
LEFT JOIN sepsis_patients s ON c.patientunitstayid = s.patientunitstayid
ORDER BY c.patientunitstayid;
```
- Charttime
```sql=
WITH tm AS (
    SELECT patientunitstayid, respchartoffset AS charttime
    FROM physionet-data.eicu_crd.respiratorycharting
    WHERE respchartvalue IN (
        'Tracheostomy', 'Endotracheal tube', 'Bipap', 'CPAP', 'High flow nasal cannula',
        'Non-rebreather', 'Face tent', 'Nasal cannula', 'None'
    )
    UNION DISTINCT
    SELECT patientunitstayid, treatmentoffset AS charttime
    FROM physionet-data.eicu_crd.treatment
    WHERE treatmentstring LIKE '%mechanical ventilation%'
)

-- Step 2: Classify ventilation status
, vs AS (
    SELECT tm.patientunitstayid, tm.charttime,
        rc.respchartvalue AS o2_delivery_device,
        tr.treatmentstring AS procedure_event,
        
        -- Assign ventilation status
        CASE
            WHEN rc.respchartvalue = 'Tracheostomy' THEN 'Tracheostomy'
            WHEN rc.respchartvalue = 'Endotracheal tube' OR tr.treatmentstring LIKE '%mechanical ventilation%' THEN 'InvasiveVent'
            WHEN rc.respchartvalue IN ('Bipap', 'CPAP') THEN 'NonInvasiveVent'
            WHEN rc.respchartvalue = 'High flow nasal cannula' THEN 'HFNC'
            WHEN rc.respchartvalue IN ('Non-rebreather', 'Face tent', 'Nasal cannula') THEN 'SupplementalOxygen'
            WHEN rc.respchartvalue = 'None' THEN 'None'
            ELSE NULL 
        END AS ventilation_status
    FROM tm
    LEFT JOIN physionet-data.eicu_crd.respiratorycharting rc
        ON tm.patientunitstayid = rc.patientunitstayid
        AND tm.charttime = rc.respchartoffset
    LEFT JOIN physionet-data.eicu_crd.treatment tr
        ON tm.patientunitstayid = tr.patientunitstayid
        AND tm.charttime = tr.treatmentoffset
)

-- Step 3: Identify invasive ventilation events lasting ≥ 48 hours (changed from 24 to match MIMIC query)
, vd AS (
    SELECT patientunitstayid, charttime, ventilation_status,
        LAG(charttime) OVER (PARTITION BY patientunitstayid ORDER BY charttime) AS prev_time,
        LEAD(charttime) OVER (PARTITION BY patientunitstayid ORDER BY charttime) AS next_time
    FROM vs
    WHERE ventilation_status IS NOT NULL
)

, patient_admit_times AS (
    SELECT patientunitstayid, 
           TIMESTAMP_ADD(TIMESTAMP '1970-01-01 00:00:00 UTC', INTERVAL hospitaladmitoffset MINUTE) AS admit_dttm
    FROM physionet-data.eicu_crd.patient
)

, invasive_vent AS (
    SELECT vd.patientunitstayid, 
           MIN(TIMESTAMP_ADD(p.admit_dttm, INTERVAL vd.charttime MINUTE)) AS starttime,
           MAX(TIMESTAMP_ADD(p.admit_dttm, INTERVAL vd.charttime MINUTE)) AS endtime
    FROM vd
    JOIN patient_admit_times p 
        ON vd.patientunitstayid = p.patientunitstayid
    WHERE vd.ventilation_status = 'InvasiveVent'
    GROUP BY vd.patientunitstayid, p.admit_dttm
    HAVING TIMESTAMP_DIFF(endtime, starttime, SECOND) >= 24 * 3600  -- Changed to 48 hours to match MIMIC
)

-- Step 4: Exclude DNR/DNI patients
, dnr_dni AS (
    SELECT DISTINCT patientunitstayid
    FROM `physionet-data.eicu_crd.careplangeneral`
    WHERE cplitemvalue like "Do not resuscitate" or cplitemvalue like "No intubation"
)

, invasive_vent_filtered AS (
    SELECT iv.*
    FROM invasive_vent iv
    LEFT JOIN dnr_dni dnr ON iv.patientunitstayid = dnr.patientunitstayid
    WHERE dnr.patientunitstayid IS NULL
)

-- Step 5: Exclude patients under 20 years old
, age_filtered AS (
    SELECT iv.*
    FROM invasive_vent_filtered iv
    INNER JOIN physionet-data.eicu_crd.patient p
    ON iv.patientunitstayid = p.patientunitstayid
    WHERE SAFE_CAST(p.age AS INT64) >= 20
    AND SAFE_CAST(p.age AS INT64) <= 100
)

, Cohort AS (
    SELECT *
    FROM age_filtered
)

-- Extract GCS values (equivalent to MIMIC's gcs table)
, gcs_vitalsign AS (
    SELECT
        c.patientunitstayid,
        pgcs.chartoffset AS charttime,
        pgcs.gcs AS gcs
    FROM Cohort AS c
    JOIN physionet-data.eicu_crd_derived.pivoted_gcs AS pgcs
        ON c.patientunitstayid = pgcs.patientunitstayid
)

-- Extract vital signs (equivalent to MIMIC's vitalsign table)
-- , vitalsign AS (
--     SELECT
--         c.patientunitstayid,
--         v.observationoffset AS charttime,
--         v.heartrate AS heart_rate,
--         v.respiration AS resp_rate,  -- Respiratory rate only from vitalperiodic table
--         v.systemicsystolic AS sbp,
--         v.systemicdiastolic AS dbp,
--         v.systemicmean AS mbp,
--         v.sao2 AS spo2
--     FROM Cohort AS c
--     JOIN physionet-data.eicu_crd.vitalperiodic AS v
--         ON c.patientunitstayid = v.patientunitstayid
-- )
, vitalsign AS (
    SELECT
        c.patientunitstayid,
        v.chartoffset AS charttime,
        v.heartrate AS heart_rate,
        v.respiratoryrate AS resp_rate,
        v.nibp_systolic AS sbp,
        v.nibp_diastolic AS dbp,
        v.nibp_mean AS mbp,
        v.spo2 AS spo2
    FROM Cohort AS c
    JOIN physionet-data.eicu_crd_derived.pivoted_vital AS v
        ON c.patientunitstayid = v.patientunitstayid
)

-- Return the vital signs data
SELECT * FROM vitalsign
-- WHERE patientunitstayid >= 3000000 -- for download
-- WHERE patientunitstayid >= 2000000 and patientunitstayid < 3000000
-- ORDER BY patientunitstayid
-- Uncomment to get GCS data instead
-- SELECT * FROM gcs_vitalsign
```
- ventilator setting
```sql=
-- for check the var name
SELECT DISTINCT respchartvaluelabel FROM `physionet-data.eicu_crd.respiratorycharting`
```
```sql=
WITH tm AS (
    SELECT patientunitstayid, respchartoffset AS charttime
    FROM physionet-data.eicu_crd.respiratorycharting
    WHERE respchartvalue IN (
        'Tracheostomy', 'Endotracheal tube', 'Bipap', 'CPAP', 'High flow nasal cannula',
        'Non-rebreather', 'Face tent', 'Nasal cannula', 'None'
    )
    UNION DISTINCT
    SELECT patientunitstayid, treatmentoffset AS charttime
    FROM physionet-data.eicu_crd.treatment
    WHERE treatmentstring LIKE '%mechanical ventilation%'
)

-- Step 2: Classify ventilation status
, vs AS (
    SELECT tm.patientunitstayid, tm.charttime,
        rc.respchartvalue AS o2_delivery_device,
        tr.treatmentstring AS procedure_event,
        
        -- Assign ventilation status
        CASE
            WHEN rc.respchartvalue = 'Tracheostomy' THEN 'Tracheostomy'
            WHEN rc.respchartvalue = 'Endotracheal tube' OR tr.treatmentstring LIKE '%mechanical ventilation%' THEN 'InvasiveVent'
            WHEN rc.respchartvalue IN ('Bipap', 'CPAP') THEN 'NonInvasiveVent'
            WHEN rc.respchartvalue = 'High flow nasal cannula' THEN 'HFNC'
            WHEN rc.respchartvalue IN ('Non-rebreather', 'Face tent', 'Nasal cannula') THEN 'SupplementalOxygen'
            WHEN rc.respchartvalue = 'None' THEN 'None'
            ELSE NULL 
        END AS ventilation_status
    FROM tm
    LEFT JOIN physionet-data.eicu_crd.respiratorycharting rc
        ON tm.patientunitstayid = rc.patientunitstayid
        AND tm.charttime = rc.respchartoffset
    LEFT JOIN physionet-data.eicu_crd.treatment tr
        ON tm.patientunitstayid = tr.patientunitstayid
        AND tm.charttime = tr.treatmentoffset
)

-- Step 3: Identify invasive ventilation events lasting ≥ 48 hours (changed from 24 to match MIMIC query)
, vd AS (
    SELECT patientunitstayid, charttime, ventilation_status,
        LAG(charttime) OVER (PARTITION BY patientunitstayid ORDER BY charttime) AS prev_time,
        LEAD(charttime) OVER (PARTITION BY patientunitstayid ORDER BY charttime) AS next_time
    FROM vs
    WHERE ventilation_status IS NOT NULL
)

, patient_admit_times AS (
    SELECT patientunitstayid, 
           TIMESTAMP_ADD(TIMESTAMP '1970-01-01 00:00:00 UTC', INTERVAL hospitaladmitoffset MINUTE) AS admit_dttm
    FROM physionet-data.eicu_crd.patient
)

, invasive_vent AS (
    SELECT vd.patientunitstayid, 
           MIN(TIMESTAMP_ADD(p.admit_dttm, INTERVAL vd.charttime MINUTE)) AS starttime,
           MAX(TIMESTAMP_ADD(p.admit_dttm, INTERVAL vd.charttime MINUTE)) AS endtime
    FROM vd
    JOIN patient_admit_times p 
        ON vd.patientunitstayid = p.patientunitstayid
    WHERE vd.ventilation_status = 'InvasiveVent'
    GROUP BY vd.patientunitstayid, p.admit_dttm
    HAVING TIMESTAMP_DIFF(endtime, starttime, SECOND) >= 24 * 3600  -- Changed to 48 hours to match MIMIC
)

-- Step 4: Exclude DNR/DNI patients
, dnr_dni AS (
    SELECT DISTINCT patientunitstayid
    FROM `physionet-data.eicu_crd.careplangeneral`
    WHERE cplitemvalue like "Do not resuscitate" or cplitemvalue like "No intubation"
)

, invasive_vent_filtered AS (
    SELECT iv.*
    FROM invasive_vent iv
    LEFT JOIN dnr_dni dnr ON iv.patientunitstayid = dnr.patientunitstayid
    WHERE dnr.patientunitstayid IS NULL
)

-- Step 5: Exclude patients under 20 years old
, age_filtered AS (
    SELECT iv.*
    FROM invasive_vent_filtered iv
    INNER JOIN physionet-data.eicu_crd.patient p
    ON iv.patientunitstayid = p.patientunitstayid
    WHERE SAFE_CAST(p.age AS INT64) >= 20
    AND SAFE_CAST(p.age AS INT64) <= 100
)

, Cohort AS (
    SELECT *
    FROM age_filtered
)

-- ventilator_settings for eICU
, ven_setting AS (
  SELECT
    c.patientunitstayid,
    rc.respchartoffset AS charttime,
    MAX(CASE 
        WHEN rc.respchartvaluelabel IN ('PEEP', 'PEEP/CPAP') THEN SAFE_CAST(rc.respchartvalue AS FLOAT64)
      END) AS peep,
    MAX(CASE 
        WHEN rc.respchartvaluelabel IN ('FiO2') -- FiO2, 'FIO2 (%)', 'Set Fraction of Inspired Oxygen (FIO2)' 
        THEN SAFE_CAST(REPLACE(rc.respchartvalue, '%', '') AS FLOAT64) 
      END) AS fio2,
    MAX(CASE 
        WHEN rc.respchartvaluelabel IN ('Tidal Volume (observed)', 'Vt', 'Tidal Volume Observed (VT)', 'Exhaled TV (patient)', 'Vt Spontaneous (mL)', 'Vti') 
        THEN SAFE_CAST(rc.respchartvalue AS FLOAT64) 
      END) AS tidal_volume_observed,
    MAX(CASE 
        WHEN rc.respchartvaluelabel IN ('Tidal Volume (set)', 'Set Vt', 'Set Vt (Servo,LTV)')  -- , 'Set Vt (Drager)'
        THEN SAFE_CAST(rc.respchartvalue AS FLOAT64) 
      END) AS tidal_volume_set,
    MAX(CASE 
        WHEN rc.respchartvaluelabel IN ('Respiratory Rate (set)', 'Set RR', 'Vent Rate') -- , 'Resp Rate Total', 'Total RR' 
        THEN SAFE_CAST(rc.respchartvalue AS FLOAT64) 
      END) AS respiratory_rate_set,
    MAX(CASE 
        WHEN rc.respchartvaluelabel IN ('Plateau Pressure', 'Pplat') 
        THEN SAFE_CAST(rc.respchartvalue AS FLOAT64) 
      END) AS plateau_pressure,
    MAX(CASE 
        WHEN rc.respchartvaluelabel IN ('Ventilator Mode', 'Mode', 'Mechanical Ventilator Mode', 'Non-invasive Ventilation Mode') 
        THEN rc.respchartvalue 
      END) AS ventilator_mode
  FROM Cohort AS c
  JOIN physionet-data.eicu_crd.respiratorycharting AS rc
    ON c.patientunitstayid = rc.patientunitstayid
  GROUP BY c.patientunitstayid, rc.respchartoffset
)
SELECT * FROM ven_setting
```
## Ground Truth
- only died_in_unit
```sql=
WITH tm AS (
    SELECT patientunitstayid, respchartoffset AS charttime
    FROM physionet-data.eicu_crd.respiratorycharting
    WHERE respchartvalue IN (
        'Tracheostomy', 'Endotracheal tube', 'Bipap', 'CPAP', 'High flow nasal cannula',
        'Non-rebreather', 'Face tent', 'Nasal cannula', 'None'
    )
    UNION DISTINCT
    SELECT patientunitstayid, treatmentoffset AS charttime
    FROM physionet-data.eicu_crd.treatment
    WHERE treatmentstring LIKE '%mechanical ventilation%'
)

-- Step 2: Classify ventilation status
, vs AS (
    SELECT tm.patientunitstayid, tm.charttime,
        rc.respchartvalue AS o2_delivery_device,
        tr.treatmentstring AS procedure_event,
        
        -- Assign ventilation status
        CASE
            WHEN rc.respchartvalue = 'Tracheostomy' THEN 'Tracheostomy'
            WHEN rc.respchartvalue = 'Endotracheal tube' OR tr.treatmentstring LIKE '%mechanical ventilation%' THEN 'InvasiveVent'
            WHEN rc.respchartvalue IN ('Bipap', 'CPAP') THEN 'NonInvasiveVent'
            WHEN rc.respchartvalue = 'High flow nasal cannula' THEN 'HFNC'
            WHEN rc.respchartvalue IN ('Non-rebreather', 'Face tent', 'Nasal cannula') THEN 'SupplementalOxygen'
            WHEN rc.respchartvalue = 'None' THEN 'None'
            ELSE NULL 
        END AS ventilation_status
    FROM tm
    LEFT JOIN physionet-data.eicu_crd.respiratorycharting rc
        ON tm.patientunitstayid = rc.patientunitstayid
        AND tm.charttime = rc.respchartoffset
    LEFT JOIN physionet-data.eicu_crd.treatment tr
        ON tm.patientunitstayid = tr.patientunitstayid
        AND tm.charttime = tr.treatmentoffset
)

-- Step 3: Identify invasive ventilation events lasting ≥ 48 hours (changed from 24 to match MIMIC query)
, vd AS (
    SELECT patientunitstayid, charttime, ventilation_status,
        LAG(charttime) OVER (PARTITION BY patientunitstayid ORDER BY charttime) AS prev_time,
        LEAD(charttime) OVER (PARTITION BY patientunitstayid ORDER BY charttime) AS next_time
    FROM vs
    WHERE ventilation_status IS NOT NULL
)

, patient_admit_times AS (
    SELECT patientunitstayid, 
           TIMESTAMP_ADD(TIMESTAMP '1970-01-01 00:00:00 UTC', INTERVAL hospitaladmitoffset MINUTE) AS admit_dttm
    FROM physionet-data.eicu_crd.patient
)

, invasive_vent AS (
    SELECT vd.patientunitstayid, 
           MIN(TIMESTAMP_ADD(p.admit_dttm, INTERVAL vd.charttime MINUTE)) AS starttime,
           MAX(TIMESTAMP_ADD(p.admit_dttm, INTERVAL vd.charttime MINUTE)) AS endtime
    FROM vd
    JOIN patient_admit_times p 
        ON vd.patientunitstayid = p.patientunitstayid
    WHERE vd.ventilation_status = 'InvasiveVent'
    GROUP BY vd.patientunitstayid, p.admit_dttm
    HAVING TIMESTAMP_DIFF(endtime, starttime, SECOND) >= 24 * 3600  -- Changed to 48 hours to match MIMIC
)

-- Step 4: Exclude DNR/DNI patients
, dnr_dni AS (
    SELECT DISTINCT patientunitstayid
    FROM `physionet-data.eicu_crd.careplangeneral`
    WHERE cplitemvalue like "Do not resuscitate" or cplitemvalue like "No intubation"
)

, invasive_vent_filtered AS (
    SELECT iv.*
    FROM invasive_vent iv
    LEFT JOIN dnr_dni dnr ON iv.patientunitstayid = dnr.patientunitstayid
    WHERE dnr.patientunitstayid IS NULL
)

-- Step 5: Exclude patients under 20 years old
, age_filtered AS (
    SELECT iv.*
    FROM invasive_vent_filtered iv
    INNER JOIN physionet-data.eicu_crd.patient p
    ON iv.patientunitstayid = p.patientunitstayid
    WHERE SAFE_CAST(p.age AS INT64) >= 20
    AND SAFE_CAST(p.age AS INT64) <= 100
)

, Cohort AS (
    SELECT *
    FROM age_filtered
)

, patient_death AS (
    SELECT 
        patientunitstayid,
        CASE 
            WHEN unitdischargestatus = 'Expired' THEN TRUE
            ELSE FALSE
        END AS died_in_unit
    FROM physionet-data.eicu_crd.patient
)

-- Final cohort with ventilation, mortality data
SELECT 
    c.patientunitstayid,
    c.starttime,
    c.endtime,
    pd.died_in_unit
FROM cohort AS c
LEFT JOIN patient_death AS pd ON c.patientunitstayid = pd.patientunitstayid
ORDER BY c.patientunitstayid;
```