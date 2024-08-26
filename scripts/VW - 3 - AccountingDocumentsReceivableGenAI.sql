create or replace view {{ project_id_tgt }}.{{ dataset_reporting_tgt }}.AccountingDocumentsReceivableGenAI as
WITH Tbl_AccountingDocuments as (
SELECT
      AccountingDocuments.Client_MANDT,
      AccountingDocuments.ExchangeRateType_KURST,
      AccountingDocuments.CompanyCode_BUKRS,
      CompaniesMD.CompanyText_BUTXT,
      AccountingDocuments.CustomerNumber_KUNNR,
      AccountingDocuments.FiscalYear_GJAHR,
      AccountingDocuments.SpecialGLIndicator_UMSKZ, --##CORTEX-CUSTOMER Insert field Special G/L Indicator
      GlIndicator.ktext as SpecialGLIndicatorLine_ktext, --##CORTEX-CUSTOMER Special GL Indicator Line
      GlIndicator.ltext as SpecialGLIndicatorEvent_ltext, --##CORTEX-CUSTOMER Special GL Indicator Event      
      AccountingDocuments.UserName_USNAM, --##CORTEX-CUSTOMER Insert field User name
      AccountingDocuments.AssignmentNumber_ZUONR, --##CORTEX-CUSTOMER Assignment Number
      AccountingDocuments.DocumentType_BLART, --##CORTEX-CUSTOMER Document Type
      DocTypeText.ltext AS DocumentTypeText_LTEXT, --##CORTEX-CUSTOMER Document Type
      AccountingDocuments.BusinessPlace_BUPLA, --##CORTEX-CUSTOMER Business Place
      AccountingDocuments.PostingKey_BSCHL, --##CORTEX-CUSTOMER Business Place
      AccountingDocuments.ItemText_SGTXT, --##CORTEX-CUSTOMER Item Text
      CustomersMD.NAME1_NAME1,
      CompaniesMD.Country_LAND1 AS Company_Country,
      CompaniesMD.CityName_ORT01 AS Company_City,
      CustomersMD.CountryKey_LAND1,
      CustomersMD.City_ORT01,
      AccountingDocuments.AccountingDocumentNumber_BELNR,
      AccountingDocuments.NumberOfLineItemWithinAccountingDocument_BUZEI,
      AccountingDocuments.CurrencyKey_WAERS,
      AccountingDocuments.LocalCurrency_HWAER,
      CompaniesMD.FiscalyearVariant_PERIV,
      IF(`{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.Fiscal_Period`(AccountingDocuments.Client_MANDT,
          CompaniesMD.FiscalyearVariant_PERIV,
          AccountingDocuments.PostingDateInTheDocument_BUDAT) = 'CASE1',
        `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.Fiscal_Case1`(AccountingDocuments.Client_MANDT,
          CompaniesMD.FiscalyearVariant_PERIV,
          AccountingDocuments.PostingDateInTheDocument_BUDAT),
        IF(`{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.Fiscal_Period`(AccountingDocuments.Client_MANDT,
            CompaniesMD.FiscalyearVariant_PERIV,
            AccountingDocuments.PostingDateInTheDocument_BUDAT) = 'CASE2',
          `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.Fiscal_Case2`(AccountingDocuments.Client_MANDT,
            CompaniesMD.FiscalyearVariant_PERIV,
            AccountingDocuments.PostingDateInTheDocument_BUDAT),
          IF(`{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.Fiscal_Period`(AccountingDocuments.Client_MANDT,
              CompaniesMD.FiscalyearVariant_PERIV,
              AccountingDocuments.PostingDateInTheDocument_BUDAT) = 'CASE3',
            `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.Fiscal_Case3`(AccountingDocuments.Client_MANDT,
              CompaniesMD.FiscalyearVariant_PERIV,
              AccountingDocuments.PostingDateInTheDocument_BUDAT),
            'DATA ISSUE'))) AS Period,
      IF(`{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.Fiscal_Period`(AccountingDocuments.Client_MANDT,
          CompaniesMD.FiscalyearVariant_PERIV,
          CURRENT_DATE()) = 'CASE1',
        `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.Fiscal_Case1`(AccountingDocuments.Client_MANDT,
          CompaniesMD.FiscalyearVariant_PERIV,
          CURRENT_DATE()),
        IF(`{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.Fiscal_Period`(AccountingDocuments.Client_MANDT,
            CompaniesMD.FiscalyearVariant_PERIV,
            CURRENT_DATE()) = 'CASE2',
          `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.Fiscal_Case2`(AccountingDocuments.Client_MANDT,
            CompaniesMD.FiscalyearVariant_PERIV,
            CURRENT_DATE()),
          IF(`{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.Fiscal_Period`(AccountingDocuments.Client_MANDT,
              CompaniesMD.FiscalyearVariant_PERIV,
              CURRENT_DATE()) = 'CASE3',
            `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.Fiscal_Case3`(AccountingDocuments.Client_MANDT,
              CompaniesMD.FiscalyearVariant_PERIV,
              CURRENT_DATE()),
            'DATA ISSUE'))) AS Current_Period,
      AccountingDocuments.AccountType_KOART,
      AccountingDocuments.PostingDateInTheDocument_BUDAT,
      AccountingDocuments.DocumentDateInDocument_BLDAT,
      AccountingDocuments.InvoiceToWhichTheTransactionBelongs_REBZG,
      AccountingDocuments.BillingDocument_VBELN,
      AccountingDocuments.WrittenOffAmount_DMBTR,
      AccountingDocuments.BadDebt_DMBTR,
      `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.NextBusinessDay`(
      AccountingDocuments.netDueDateCalc) AS NetDueDate, --##CORTEX-CUSTOMER Insert business day rule
      -- AccountingDocuments.netDueDateCalc AS NetDueDate, --##CORTEX-CUSTOMER Insert business day rule      
      AccountingDocuments.ClearingDate_AUGDT, --##CORTEX-CUSTOMER Insert Clearing Date 
      if(ifnull(AccountingDocuments.ClearingDate_AUGDT,'0001-01-01') <= '1900-01-01',current_date(),AccountingDocuments.ClearingDate_AUGDT) 
      as ClearingDateOrCurrentDate, --##CORTEX-CUSTOMER Clearing Date or Current date
      if(ifnull(AssignmentNumber_ZUONR,"") = "",False,True) as IsAccountsPayableCompensation, --##CORTEX-CUSTOMER Accounts Payable Compensation
      if(UserName_USNAM = "IBMTWS",True,False) as IsAutomaticPosting,	--##CORTEX-CUSTOMER Automatic Posting
      AmountInLocalCurrency_DMBTR, --##CORTEX-CUSTOMER Amount in local currency
      AccountingDocuments.sk2dtCalc AS CashDiscountDate1,
      AccountingDocuments.sk1dtCalc AS CashDiscountDate2,
      AccountingDocuments.OpenAndNotDue,
      AccountingDocuments.ClearedAfterDueDate,
      AccountingDocuments.ClearedOnOrBeforeDueDate,
      AccountingDocuments.OpenAndOverDue,
      AccountingDocuments.DoubtfulReceivables,
      AccountingDocuments.DaysInArrear,
      AccountingDocuments.AccountsReceivable,
      AccountingDocuments.Sales
    FROM
      `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.AccountingDocuments` AS AccountingDocuments
    LEFT JOIN
      `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.CustomersMD` AS CustomersMD
      ON
        AccountingDocuments.Client_MANDT = CustomersMD.Client_MANDT
        AND AccountingDocuments.CustomerNumber_KUNNR = CustomersMD.CustomerNumber_KUNNR
    LEFT JOIN
      `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.CompaniesMD` AS CompaniesMD
      ON
        AccountingDocuments.Client_MANDT = CompaniesMD.Client_MANDT
        AND AccountingDocuments.CompanyCode_BUKRS = CompaniesMD.CompanyCode_BUKRS
    LEFT JOIN `{{ project_id_tgt }}.{{ dataset_cdc_processed }}.t003t` AS DocTypeText --##CORTEX-CUSTOMER Document Type Text
      ON AccountingDocuments.DocumentType_BLART = DocTypeText.blart 
     AND DocTypeText.spras = 'E' --##CORTEX-CUSTOMER Document Type Text, select language
    LEFT JOIN `{{ project_id_tgt }}.{{ dataset_cdc_processed }}.t074t` AS GlIndicator --##CORTEX-CUSTOMER Gl Indicator Text
      ON AccountingDocuments.SpecialGLIndicator_UMSKZ = GlIndicator.SHBKZ 
     AND AccountingDocuments.DocumentType_BLART = GlIndicator.koart    
     AND GlIndicator.spras = 'E' --##CORTEX-CUSTOMER Gl Indicator Text, select language 
    WHERE AccountingDocuments.AccountType_KOART = "D" ) 
    SELECT  * except(ClearingDateOrCurrentDate,DaysInArrear),                          
            DATE_DIFF(NetDueDate, ClearingDateOrCurrentDate, day) as DaysInArrear,     --##CORTEX-CUSTOMER Insert Aging 
            CASE
              WHEN DATE_DIFF(NetDueDate, ClearingDateOrCurrentDate, day) between 31 and 60 THEN '31 a 60 Dias'
              WHEN DATE_DIFF(NetDueDate, ClearingDateOrCurrentDate, day) between 61 and 180 THEN '61 a 180 Dias'
              WHEN DATE_DIFF(NetDueDate, ClearingDateOrCurrentDate, day) > 180 THEN 'Acima de 180 Dias'
              WHEN DATE_DIFF(NetDueDate, ClearingDateOrCurrentDate, day) between 0 and 30 THEN '0 a 30 Dias'
              WHEN DATE_DIFF(NetDueDate, ClearingDateOrCurrentDate, day) between -30 and 0 THEN '0 a 30 Dias'
              WHEN DATE_DIFF(NetDueDate, ClearingDateOrCurrentDate, day) between -60 and -31 THEN '31 a 60 Dias'
              WHEN DATE_DIFF(NetDueDate, ClearingDateOrCurrentDate, day) between -180 and -61 THEN '61 a 180 Dias'
              WHEN DATE_DIFF(NetDueDate, ClearingDateOrCurrentDate, day) < -180 THEN 'Acima de 180 Dias'    
          END AS AgingRange,                                             --##CORTEX-CUSTOMER Insert Aging 
            CASE
              WHEN DATE_DIFF(NetDueDate, ClearingDateOrCurrentDate, day) >= 0 then 'A Vencer' 
              ELSE 'Vencidos'
            END AS AgingList                                             --##CORTEX-CUSTOMER Insert Aging 
      from Tbl_AccountingDocuments