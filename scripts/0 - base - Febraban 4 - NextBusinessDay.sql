CREATE OR REPLACE FUNCTION `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.NextBusinessDay`(entry_date DATE) RETURNS DATE AS (
(
    SELECT
      CASE
        WHEN is_business_day IS TRUE THEN date
        ELSE next_business_day
    END
      next_business_day
    FROM
      `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.BusinessDays` 
   WHERE date = entry_date
   
   )
);