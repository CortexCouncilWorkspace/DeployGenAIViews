CREATE OR REPLACE FUNCTION `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.PreviousBusinessDay`(entry_date DATE) RETURNS DATE AS (
(
    SELECT
      previous_business_day
    FROM
      `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.BusinessDays` 
   WHERE date = entry_date
   
   )
);