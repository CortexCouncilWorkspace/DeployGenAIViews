CREATE OR REPLACE FUNCTION `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.IsBusinessDay`(date DATE) RETURNS BOOL AS (
(SELECT
    CASE
      WHEN EXTRACT(DAYOFWEEK FROM date) IN (1, 7) THEN FALSE -- Weekend
      WHEN (SELECT COUNT(1) FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.FebrabanHoliday` WHERE HolidayDate = date) > 0 THEN FALSE -- Holiday
      ELSE TRUE
    END
  )
);