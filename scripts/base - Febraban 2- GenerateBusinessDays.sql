CREATE OR REPLACE PROCEDURE `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.GenerateBusinessDays`(IN start_date DATE, IN end_date DATE)
BEGIN
  
  DECLARE TABLE_EXISTS BOOL;
  
  SET TABLE_EXISTS = (
    SELECT COUNT(1) > 0
    FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.INFORMATION_SCHEMA.TABLES`
    WHERE table_name = 'BusinessDays'
  );
  
  IF NOT TABLE_EXISTS THEN
    CREATE TABLE `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.BusinessDays` (
      date DATE,
      is_business_day BOOL,
      previous_business_day DATE,
      next_business_day DATE
    );
  END IF;
  
  CREATE TEMP TABLE TempBusinessDays AS
  WITH dates AS (
    SELECT
      DATE_ADD(start_date, INTERVAL day DAY) AS date
    FROM
      UNNEST(GENERATE_ARRAY(0, DATE_DIFF(end_date, start_date, DAY))) AS day
  ),

  possible_next_days AS (
    SELECT
      d.date AS original_date,
      DATE_ADD(d.date, INTERVAL day_offset DAY) AS possible_next_date
    FROM
      dates d,
      UNNEST(GENERATE_ARRAY(1, 30)) AS day_offset
  ),

  filtered_next_days AS (
    SELECT
      original_date,
      possible_next_date
    FROM
      possible_next_days
    WHERE
      `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.IsBusinessDay`(possible_next_date)
  ),

  next_business_days AS (
    SELECT
      original_date AS date,
      CASE
        WHEN `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.IsBusinessDay`(original_date)
          THEN true
        ELSE false
      END AS is_business_day,      
      MIN(possible_next_date) AS next_business_day
    FROM
      filtered_next_days
    GROUP BY
      original_date,
      is_business_day
  ),

  previous_business_days AS (
    SELECT
      date,
      is_business_day,
      LAG(date) OVER (ORDER BY date) AS previous_business_day,
      next_business_day
    FROM
      next_business_days
  )
  SELECT
    date,
    is_business_day,
    previous_business_day,
    next_business_day
  FROM
    previous_business_days;

  MERGE INTO `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.BusinessDays` T
  USING TempBusinessDays S
  ON T.date = S.date
  WHEN MATCHED THEN
    UPDATE SET 
      T.is_business_day = S.is_business_day,
      T.previous_business_day = S.previous_business_day,
      T.next_business_day = S.next_business_day
  WHEN NOT MATCHED THEN
    INSERT (date, is_business_day, previous_business_day, next_business_day) 
    VALUES (S.date, S.is_business_day, S.previous_business_day, S.next_business_day);

END;