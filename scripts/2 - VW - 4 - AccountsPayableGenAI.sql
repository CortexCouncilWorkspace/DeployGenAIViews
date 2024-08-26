create or replace view {{ project_id_tgt }}.{{ dataset_reporting_tgt }}.AccountsPayableGenAI as
WITH CurrencyConversion AS (
      SELECT
        Client_MANDT, FromCurrency_FCURR, ToCurrency_TCURR, ConvDate, ExchangeRate_UKURS
      FROM
        `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.CurrencyConversion`
      WHERE
        ToCurrency_TCURR = 'BRL'
        --##CORTEX-CUSTOMER Modify the exchange rate type based on your requirement
        AND ExchangeRateType_KURST = 'B' -- ## COPEL-CUSTOMIZATION 'B'
    ),

    AccountingInvoices AS (
      SELECT
        AccountingDocuments.Client_MANDT,
        AccountingDocuments.CompanyCode_BUKRS,
        AccountingDocuments.AccountingDocumentNumber_BELNR,
        AccountingDocuments.FiscalYear_GJAHR,
        AccountingDocuments.Documenttype_BLART AS AccountingDocumenttype_BLART,
        InvoiceDocuments.Documenttype_BLART AS InvoiceDocumenttype_BLART,
        AccountingDocuments.DocumentDateInDocument_BLDAT,
        AccountingDocuments.PostingDateInTheDocument_BUDAT,
        (`{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.PreviousBusinessDay`(AccountingDocuments.PostingDateInTheDocument_BUDAT)) AS PreviousBusinessDay_BUDAT,
        InvoiceDocuments.PostingDate_BUDAT,
        AccountingDocuments.FiscalPeriod_MONAT,
        AccountingDocuments.PurchasingDocumentNumber_EBELN,
        AccountingDocuments.NumberOfLineItemWithinAccountingDocument_BUZEI,
        AccountingDocuments.ClearingDate_AUGDT,
        COALESCE(AccountingDocuments.NetPaymentAmount_NEBTR, 0) AS NetPaymentAmount_NEBTR,
        COALESCE(AccountingDocuments.AmountInLocalCurrency_DMBTR, 0) AS AmountInLocalCurrency_DMBTR,
        COALESCE(AccountingDocuments.AmountinDocumentCurrency_WRBTR, 0) AS AmountinDocumentCurrency_WRBTR, ## COPEL-CUSTOMIZATION
        AccountingDocuments.AccountType_KOART,
        AccountingDocuments.TransactionKey_KTOSL,
        AccountingDocuments.PostingKey_BSCHL,
        AccountingDocuments.CashDiscountDays1_ZBD1T,
        AccountingDocuments.BaselineDateForDueDateCalculation_ZFBDT,
        COALESCE(AccountingDocuments.AmountEligibleForCashDiscountInDocumentCurrency_SKFBT, 0) AS AmountEligibleForCashDiscountInDocumentCurrency_SKFBT,
        AccountingDocuments.AccountNumberOfVendorOrCreditor_LIFNR,
        AccountingDocuments.PaymentBlockKey_ZLSPR,
        AccountingDocuments.SpecialGlIndicator_UMSKZ,
        AccountingDocuments.ItemNumberOfPurchasingDocument_EBELP,
        AccountingDocuments.FollowOnDocumentType_REBZT,
        AccountingDocuments.DocumentNumberOfTheClearingDocument_AUGBL,
        AccountingDocuments.TermsOfPaymentKey_ZTERM,
        AccountingDocuments.ReasonCodeForPayments_RSTGR,
        AccountingDocuments.CashDiscountPercentage1_ZBD1P,
        AccountingDocuments.NetPaymentTermsPeriod_ZBD3T,
        AccountingDocuments.CashDiscountDays2_ZBD2T,
        AccountingDocuments.DebitcreditIndicator_SHKZG,
        AccountingDocuments.InvoiceToWhichTheTransactionBelongs_REBZG,
        AccountingDocuments.CurrencyKey_WAERS,
        AccountingDocuments.SupplyingCountry_LANDL,
        AccountingDocuments.ObjectKey_AWKEY,
        NULL AS InvStatus_RBSTAT,
        AccountingDocuments.YearOfPostingDateInTheDocument_BUDAT,
        AccountingDocuments.MonthOfPostingDateInTheDocument_BUDAT,
        AccountingDocuments.WeekOfPostingDateInTheDocument_BUDAT,
        AccountingDocuments.QuarterOfPostingDateInTheDocument_BUDAT
      FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.AccountingDocuments` AS AccountingDocuments
      LEFT OUTER JOIN `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.InvoiceDocuments_Flow` AS InvoiceDocuments
        ON
          AccountingDocuments.Client_MANDT = InvoiceDocuments.Client_MANDT
          AND AccountingDocuments.CompanyCode_BUKRS = InvoiceDocuments.CompanyCode_BUKRS
          AND LEFT(AccountingDocuments.ObjectKey_AWKEY, 10) = InvoiceDocuments.InvoiceDocNum_BELNR
          AND AccountingDocuments.FiscalYear_GJAHR = InvoiceDocuments.FiscalYear_GJAHR
          AND LTRIM(AccountingDocuments.NumberOfLineItemWithinAccountingDocument_BUZEI, '0') = LTRIM(InvoiceDocuments.InvoiceDocLineNum_BUZEI, '0')
      WHERE
        AccountingDocuments.AccountType_KOART = 'K' ## CORTEX-CUSTOMER: Please add relevant Account Type. Value 'K' represents 'Vendor'
        ## COPEL-CUSTOMIZATION does not consider the block below, only AccountingDocuments.AccountType_KOART = 'K' 
        -- OR AccountingDocuments.PurchasingDocumentNumber_EBELN IS NOT NULL
        -- OR AccountingDocuments.Documenttype_BLART IN ('KZ', 'ZP') ## CORTEX-CUSTOMER: Please add relevant Document Type. Value 'KZ' represents 'Vendor Payment' and 'ZP' represents 'Payment Posting'
        -- OR AccountingDocuments.TransactionKey_KTOSL = 'SKE' ## CORTEX-CUSTOMER: Please add relevant Transaction Key. Value 'SKE' represents 'Cash Discount Received'
        -- OR AccountingDocuments.PostingKey_BSCHL = '31' ## CORTEX-CUSTOMER: Please add relevant Posting Key. Value '31' represents 'Vendor Invoice'

      UNION ALL

      /* Select 'Parked Invoices' from Invoice Documents */
      SELECT
        InvoiceDocuments.Client_MANDT,
        InvoiceDocuments.CompanyCode_BUKRS,
        InvoiceDocuments.InvoiceDocNum_BELNR AS AccountingDocumentNumber_BELNR,
        InvoiceDocuments.FiscalYear_GJAHR,
        CAST(NULL AS STRING) AS AccountingDocumenttype_BLART,
        CAST(NULL AS STRING) AS InvoiceDocumenttype_BLART,
        InvoiceDocuments.DocumentDate_BLDAT AS DocumentDateInDocument_BLDAT,
        InvoiceDocuments.PostingDate_BUDAT AS PostingDateInTheDocument_BUDAT,
        (`{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.PreviousBusinessDay`(InvoiceDocuments.PostingDate_BUDAT)) AS PreviousBusinessDay_BUDAT,
        InvoiceDocuments.PostingDate_BUDAT,
        CAST(NULL AS STRING) AS FiscalPeriod_MONAT,
        CAST(NULL AS STRING) AS PurchasingDocumentNumber_EBELN,
        CAST(NULL AS STRING) AS NumberOfLineItemWithinAccountingDocument_BUZEI,
        CAST(NULL AS DATE) AS ClearingDate_AUGDT,
        NULL AS NetPaymentAmount_NEBTR,
        InvoiceDocuments.GrossInvAmnt_RMWWR AS AmountInLocalCurrency_DMBTR,
        InvoiceDocuments.AmountInDocumentCurrency_WRBTR AS AmountInDocumentCurrency_WRBTR, ## COPEL-CUSTOMIZATION
        CAST(NULL AS STRING) AS AccountType_KOART,
        CAST(NULL AS STRING) AS TransactionKey_KTOSL,
        CAST(NULL AS STRING) AS PostingKey_BSCHL,
        CAST(NULL AS NUMERIC) AS CashDiscountDays1_ZBD1T,
        CAST(NULL AS DATE) AS BaselineDateForDueDateCalculation_ZFBDT,
        CAST(NULL AS NUMERIC) AS AmountEligibleForCashDiscountInDocumentCurrency_SKFBT,
        InvoiceDocuments.InvoicingParty_LIFNR AS AccountNumberOfVendorOrCreditor_LIFNR,
        CAST(NULL AS STRING) AS PaymentBlockKey_ZLSPR,
        CAST(NULL AS STRING) AS SpecialGlIndicator_UMSKZ,
        CAST(NULL AS STRING) AS ItemNumberOfPurchasingDocument_EBELP,
        CAST(NULL AS STRING) AS FollowOnDocumentType_REBZT,
        CAST(NULL AS STRING) AS DocumentNumberOfTheClearingDocument_AUGBL,
        CAST(NULL AS STRING) AS TermsOfPaymentKey_ZTERM,
        CAST(NULL AS STRING) AS ReasonCodeForPayments_RSTGR,
        CAST(NULL AS NUMERIC) AS CashDiscountPercentage1_ZBD1P,
        CAST(NULL AS NUMERIC) AS NetPaymentTermsPeriod_ZBD3T,
        CAST(NULL AS NUMERIC) AS CashDiscountDays2_ZBD2T,
        CAST(NULL AS STRING) AS DebitcreditIndicator_SHKZG,
        CAST(NULL AS STRING) AS InvoiceToWhichTheTransactionBelongs_REBZG,
        Currency_WAERS AS CurrencyKey_WAERS,
        CAST(NULL AS STRING) AS SupplyingCountry_LANDL,
        CAST(NULL AS STRING) AS ObjectKey_AWKEY,
        InvoiceDocuments.InvStatus_RBSTAT,
        InvoiceDocuments.YearOfPostingDate_BUDAT,
        InvoiceDocuments.MonthOfPostingDate_BUDAT,
        InvoiceDocuments.WeekOfPostingDate_BUDAT,
        InvoiceDocuments.QuarterOfPostingDate_BUDAT
      FROM
        `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.InvoiceDocuments_Flow` AS InvoiceDocuments
      WHERE
        ## CORTEX-CUSTOMER: Please add relevant Invoice Status. Value 'A' represents that the document is Parked and not posted
        InvoiceDocuments.Invstatus_RBSTAT = 'A'
      QUALIFY RANK() OVER (
        PARTITION BY InvoiceDocuments.Client_MANDT, InvoiceDocuments.CompanyCode_BUKRS, InvoiceDocuments.InvoiceDocNum_BELNR
        ORDER BY InvoiceDocuments.InvoiceDocLineNum_BUZEI) = 1
    ),

    AccountingInvoicesKPI AS (
      SELECT
        AccountingInvoices.*,
        CompaniesMD.CompanyText_BUTXT,

        CASE `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.Fiscal_Period`(
          AccountingInvoices.Client_MANDT,
          CompaniesMD.FiscalyearVariant_PERIV,
          AccountingInvoices.PostingDateInTheDocument_BUDAT
        )
          WHEN 'CASE1' THEN
            `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.Fiscal_Case1`(
              AccountingInvoices.Client_MANDT,
              CompaniesMD.FiscalyearVariant_PERIV,
              AccountingInvoices.PostingDateInTheDocument_BUDAT
            )
          WHEN 'CASE2' THEN
            `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.Fiscal_Case2`(
              AccountingInvoices.Client_MANDT,
              CompaniesMD.FiscalyearVariant_PERIV,
              AccountingInvoices.PostingDateInTheDocument_BUDAT
            )
          WHEN 'CASE3' THEN
            `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.Fiscal_Case3`(
              AccountingInvoices.Client_MANDT,
              CompaniesMD.FiscalyearVariant_PERIV,
              AccountingInvoices.PostingDateInTheDocument_BUDAT
            )
        END AS DocFiscPeriod,

        CASE `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.Fiscal_Period`(
          AccountingInvoices.Client_MANDT,
          CompaniesMD.FiscalyearVariant_PERIV,
          DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 DAY)
        )
          WHEN 'CASE1' THEN
            `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.Fiscal_Case1`(
              AccountingInvoices.Client_MANDT,
              CompaniesMD.FiscalyearVariant_PERIV,
              DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 DAY)
            )
          WHEN 'CASE2' THEN
            `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.Fiscal_Case2`(
              AccountingInvoices.Client_MANDT,
              CompaniesMD.FiscalyearVariant_PERIV,
              DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 DAY)
            )
          WHEN 'CASE3' THEN
            `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.Fiscal_Case3`(
              AccountingInvoices.Client_MANDT,
              CompaniesMD.FiscalyearVariant_PERIV,
              DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 DAY)
            )
        END AS KeyFiscPeriod,

        -- COPEL-CUSTOMIZATION Change to consider NetDueDate for the next business day
        `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.NextBusinessDay`(
        DATE_ADD(
          IF(
            ## CORTEX-CUSTOMER: Please add relevant Account Type. Value 'K' represents 'Vendor'
            AccountingInvoices.AccountType_KOART = 'K' AND AccountingInvoices.BaselineDateForDueDateCalculation_ZFBDT IS NULL,
            AccountingInvoices.DocumentDateInDocument_BLDAT,
            AccountingInvoices.BaselineDateForDueDateCalculation_ZFBDT
          ),
          INTERVAL CAST(
            CASE           
              WHEN AccountingInvoices.CashDiscountDays1_ZBD1T IS NOT NULL
                THEN AccountingInvoices.CashDiscountDays1_ZBD1T
              WHEN AccountingInvoices.CashDiscountDays1_ZBD1T IS NULL
                THEN 0
              ## CORTEX-CUSTOMER: Please add relevant Debit Credit Indicator. Value 'H' represents 'Credit' ('S' represents 'Debit'))
              WHEN AccountingInvoices.AccountType_KOART = 'K' AND AccountingInvoices.DebitcreditIndicator_SHKZG = 'H'
                AND AccountingInvoices.InvoiceToWhichTheTransactionBelongs_REBZG IS NULL
                THEN 0
              ELSE 0
            END
            AS INT64
          ) DAY
        ) ) AS NetDueDate,

      FROM AccountingInvoices
      INNER JOIN
        `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.CompaniesMD` AS CompaniesMD
        ON
          AccountingInvoices.Client_MANDT = CompaniesMD.Client_MANDT
          AND AccountingInvoices.CompanyCode_BUKRS = CompaniesMD.CompanyCode_BUKRS
    )

    SELECT
      AccountingInvoicesKPI.Client_MANDT,
      AccountingInvoicesKPI.CompanyCode_BUKRS,
      AccountingInvoicesKPI.CompanyText_BUTXT,
      AccountingInvoicesKPI.AccountNumberOfVendorOrCreditor_LIFNR,
      VendorsMD.NAME1,
      VendorsMD.VendorAccountGroup_KTOKK, -- COPEL-CUSTOMIZATION insert field
      AccountingInvoicesKPI.AmountInLocalCurrency_DMBTR,
      AccountingInvoicesKPI.AmountInDocumentCurrency_WRBTR, -- COPEL-CUSTOMIZATION insert field
      AccountingInvoicesKPI.AccountingDocumentNumber_BELNR,
      CASE WHEN left(AccountingInvoicesKPI.AccountingDocumentNumber_BELNR,2) IN ('15', '20', '17', '12') 
           THEN false 
           ELSE true 
      END as IsInvoice, -- COPEL-CUSTOMIZATION insert field
	  
      CASE WHEN left(AccountingInvoicesKPI.AccountingDocumentNumber_BELNR,2) IN ('15') then true
           WHEN left(AccountingInvoicesKPI.AccountingDocumentNumber_BELNR,2) IN ('20') then false
           ELSE null
      END as IsManualPayment, -- COPEL-CUSTOMIZATION insert field
      CASE WHEN left(AccountingInvoicesKPI.AccountingDocumentNumber_BELNR,2) IN ('20') then true
           WHEN left(AccountingInvoicesKPI.AccountingDocumentNumber_BELNR,2) IN ('15') then false
           ELSE null
      END as IsAutomaticPayment, -- COPEL-CUSTOMIZATION insert field

      IF (AccountingInvoicesKPI.AccountingDocumenttype_BLART in ('RE', 'KR') and VendorAccountGroup_KTOKK in ('1000', '2000'),
            true, false ) as IsInvoiceMaterialService, 
      -- COPEL-CUSTOMIZATION insert field	

      CASE WHEN AccountingInvoicesKPI.PaymentBlockKey_ZLSPR IN ( 'P' ) 
           THEN true
           ELSE false 
      END as IsHardBlocked, -- COPEL-CUSTOMIZATION insert field	  
      CASE WHEN VendorAccountGroup_KTOKK IN ( '4000' ) 
           THEN true
           ELSE false 
      END as IsLegal, -- COPEL-CUSTOMIZATION insert field	  	  
	  
      AccountingInvoicesKPI.NumberOfLineItemWithinAccountingDocument_BUZEI,
      AccountingInvoicesKPI.DocumentNumberOfTheClearingDocument_AUGBL,
      AccountingInvoicesKPI.TermsOfPaymentKey_ZTERM,
      AccountingInvoicesKPI.AccountType_KOART,
      AccountingInvoicesKPI.ReasonCodeForPayments_RSTGR,
      AccountingInvoicesKPI.PaymentBlockKey_ZLSPR,
      AccountingInvoicesKPI.ClearingDate_AUGDT,
      AccountingInvoicesKPI.PostingDateInTheDocument_BUDAT,      
      AccountingInvoicesKPI.FiscalYear_GJAHR,
      AccountingInvoicesKPI.FiscalPeriod_MONAT,
      AccountingInvoicesKPI.DocFiscPeriod,
      AccountingInvoicesKPI.KeyFiscPeriod,
      AccountingInvoicesKPI.NetDueDate,
      AccountingInvoicesKPI.InvStatus_RBSTAT,
      AccountingInvoicesKPI.PostingDate_BUDAT,
      AccountingInvoicesKPI.PurchasingDocumentNumber_EBELN,
      AccountingInvoicesKPI.CurrencyKey_WAERS,
      AccountingInvoicesKPI.SupplyingCountry_LANDL,
      AccountingInvoicesKPI.AccountingDocumenttype_BLART,
      AccountingInvoicesKPI.InvoiceDocumenttype_BLART,
      POOrderHistory.MovementType__inventoryManagement___BWART,
      POOrderHistory.AmountInLocalCurrency_DMBTR AS POOrderHistory_AmountInLocalCurrency_DMBTR,
      POOrderHistory.AmountInDocumentCurrency_WRBTR AS POOrderHistory_AmountInDocumentCurrency_WRBTR, -- COPEL-CUSTOMIZATION
      POOrderHistory.AmountInDocumentCurrency_WRBTR * CurrencyConversion.ExchangeRate_UKURS AS POOrderHistory_AmountInTargetCurrency_WRBTR, -- COPEL-CUSTOMIZATION CHANGE FROM DMBTR TO WRBTR
      AccountingInvoicesKPI.YearOfPostingDateInTheDocument_BUDAT,
      AccountingInvoicesKPI.MonthOfPostingDateInTheDocument_BUDAT,
      AccountingInvoicesKPI.WeekOfPostingDateInTheDocument_BUDAT,
      AccountingInvoicesKPI.QuarterOfPostingDateInTheDocument_BUDAT,
      AccountingInvoicesKPI.AmountInDocumentCurrency_WRBTR * CurrencyConversion.ExchangeRate_UKURS AS AmountInTargetCurrency_WRBTR, -- COPEL-CUSTOMIZATION CHANGE FROM DMBTR TO WRBTR
      CurrencyConversion.ExchangeRate_UKURS,
      CurrencyConversion.ToCurrency_TCURR AS TargetCurrency_TCURR,

      /* Overdue Amount */
      IF(
        ## CORTEX-CUSTOMER: Please add relevant Account Type. Value 'K' represents 'Vendor'
        AccountingInvoicesKPI.AccountType_KOART = 'K' AND CURRENT_DATE() > AccountingInvoicesKPI.NetDueDate and 
    		AccountingInvoicesKPI.ClearingDate_AUGDT < '1900-01-01',
        AccountingInvoicesKPI.AmountInLocalCurrency_DMBTR,
        0
      ) AS OverdueAmountInLocalCurrency, -- COPEL-CUSTOMIZATION CHANGE FROM OverdueAmountInSourceCurrency to OverdueAmountInLocalCurrency
      
      IF(
        -- COPEL-CUSTOMIZATION Inclusion of the OverdueAmountInDocumentCurrency field
        AccountingInvoicesKPI.AccountType_KOART = 'K' AND CURRENT_DATE() > AccountingInvoicesKPI.NetDueDate and 
		    AccountingInvoicesKPI.ClearingDate_AUGDT < '1900-01-01',   
        AccountingInvoicesKPI.AmountInDocumentCurrency_WRBTR,
        0
      ) AS OverdueAmountInDocumentCurrency, 

      IF(
        ## CORTEX-CUSTOMER: Please add relevant Account Type. Value 'K' represents 'Vendor'
        AccountingInvoicesKPI.AccountType_KOART = 'K' AND CURRENT_DATE() > AccountingInvoicesKPI.NetDueDate and 
		    AccountingInvoicesKPI.ClearingDate_AUGDT < '1900-01-01',  
        AccountingInvoicesKPI.AmountInDocumentCurrency_WRBTR * CurrencyConversion.ExchangeRate_UKURS,
        0
      ) AS OverdueAmountInTargetCurrency, -- COPEL-CUSTOMIZATION CHANGE FROM DMBTR TO WRBTR

      /* Outstanding But Not Overdue */
      IF(
        ## CORTEX-CUSTOMER: Please add relevant Account Type. Value 'K' represents 'Vendor'
        AccountingInvoicesKPI.AccountType_KOART = 'K' AND CURRENT_DATE() <= AccountingInvoicesKPI.NetDueDate  and
		    AccountingInvoicesKPI.ClearingDate_AUGDT < '1900-01-01',   
        AccountingInvoicesKPI.AmountInLocalCurrency_DMBTR,
        0
      ) AS OutstandingButNotOverdueInLocalCurrency, -- COPEL-CUSTOMIZATION CHANGE FROM OutstandingButNotOverdueInSourceCurrency to OutstandingButNotOverdueInLocalCurrency

      IF(
        -- COPEL-CUSTOMIZATION Inclusion of the OutstandingButNotOverdueInDocumentCurrency field
        AccountingInvoicesKPI.AccountType_KOART = 'K' AND CURRENT_DATE() <= AccountingInvoicesKPI.NetDueDate and 
		    AccountingInvoicesKPI.ClearingDate_AUGDT < '1900-01-01',   
        AccountingInvoicesKPI.AmountInDocumentCurrency_WRBTR,
        0
      ) AS OutstandingButNotOverdueInDocumentCurrency, 

      IF(
        ## CORTEX-CUSTOMER: Please add relevant Account Type. Value 'K' represents 'Vendor'
        AccountingInvoicesKPI.AccountType_KOART = 'K' AND CURRENT_DATE() <= AccountingInvoicesKPI.NetDueDate and
		    AccountingInvoicesKPI.ClearingDate_AUGDT < '1900-01-01',   
        AccountingInvoicesKPI.AmountInDocumentCurrency_WRBTR * CurrencyConversion.ExchangeRate_UKURS,
        0
      ) AS OutstandingButNotOverdueInTargetCurrency, -- COPEL-CUSTOMIZATION CHANGE FROM DMBTR TO WRBTR

      /* Overdue On Past Date */
      IF(
        ## CORTEX-CUSTOMER: Please add relevant Account Type. Value 'K' represents 'Vendor'
        AccountingInvoicesKPI.AccountType_KOART = 'K'
        AND AccountingInvoicesKPI.PostingDateInTheDocument_BUDAT < CURRENT_DATE()
        AND AccountingInvoicesKPI.NetDueDate < CURRENT_DATE()   
        AND AccountingInvoicesKPI.ClearingDate_AUGDT < '1900-01-01',
        AccountingInvoicesKPI.AmountInLocalCurrency_DMBTR, 
        0
      )
     AS OverdueOnPastDateInLocalCurrency, -- COPEL-CUSTOMIZATION CHANGE FROM OverdueOnPastDateInSourceCurrency to OverdueOnPastDateInLocalCurrency

      IF(
        -- COPEL-CUSTOMIZATION Inclusion of the OverdueOnPastDateInDocumentCurrency field
        ## CORTEX-CUSTOMER: Please add relevant Account Type. Value 'K' represents 'Vendor'
        AccountingInvoicesKPI.AccountType_KOART = 'K'
        AND AccountingInvoicesKPI.PostingDateInTheDocument_BUDAT < CURRENT_DATE()
        AND AccountingInvoicesKPI.NetDueDate < CURRENT_DATE()
        AND AccountingInvoicesKPI.ClearingDate_AUGDT < '1900-01-01',
        AccountingInvoicesKPI.AmountInDocumentCurrency_WRBTR, 
        0
      )
     AS OverdueOnPastDateInDocumentCurrency, 

      IF(
        ## CORTEX-CUSTOMER: Please add relevant Account Type. Value 'K' represents 'Vendor'
        AccountingInvoicesKPI.AccountType_KOART = 'K'
        AND AccountingInvoicesKPI.PostingDateInTheDocument_BUDAT < CURRENT_DATE()
        AND AccountingInvoicesKPI.NetDueDate < CURRENT_DATE()
        AND AccountingInvoicesKPI.ClearingDate_AUGDT < '1900-01-01',
        AccountingInvoicesKPI.AmountInDocumentCurrency_WRBTR * CurrencyConversion.ExchangeRate_UKURS,
        0
      ) AS OverdueOnPastDateInTargetCurrency, -- COPEL-CUSTOMIZATION CHANGE FROM DMBTR TO WRBTR

      /* Partial Payments */
      IF(
        ## CORTEX-CUSTOMER: Please add relevant Follow On Document Type. Value 'Z' represents Partial Payment against an open invoice
        AccountingInvoicesKPI.FollowOnDocumentType_REBZT = 'Z',
        AccountingInvoicesKPI.AmountInLocalCurrency_DMBTR,
        0
      ) AS PartialPaymentsInLocalCurrency, -- COPEL-CUSTOMIZATION CHANGE FROM PartialPaymentsInSourceCurrency to PartialPaymentsInLocalCurrency

      IF(
        -- COPEL-CUSTOMIZATION Inclusion of the PartialPaymentsInDocumentCurrency field
        ## CORTEX-CUSTOMER: Please add relevant Follow On Document Type. Value 'Z' represents Partial Payment against an open invoice
        AccountingInvoicesKPI.FollowOnDocumentType_REBZT = 'Z',
        AccountingInvoicesKPI.AmountInDocumentCurrency_WRBTR,
        0
      ) AS PartialPaymentsInDocumentCurrency, 

      IF(
        ## CORTEX-CUSTOMER: Please add relevant Follow On Document Type. Value 'Z' represents Partial Payment against an open invoice
        AccountingInvoicesKPI.FollowOnDocumentType_REBZT = 'Z',
        AccountingInvoicesKPI.AmountInDocumentCurrency_WRBTR * CurrencyConversion.ExchangeRate_UKURS,
        0
      ) AS PartialPaymentsInTargetCurrency, -- COPEL-CUSTOMIZATION CHANGE FROM DMBTR TO WRBTR

      /* Late Payments */
      IF(
        ## CORTEX-CUSTOMER: Please add relevant Account Type. Value 'K' represents 'Vendor'
        AccountingInvoicesKPI.AccountType_KOART = 'K' AND AccountingInvoicesKPI.ClearingDate_AUGDT > AccountingInvoicesKPI.NetDueDate,
        AccountingInvoicesKPI.AmountInLocalCurrency_DMBTR,
        0
      ) AS LatePaymentsInLocalCurrency, -- COPEL-CUSTOMIZATION CHANGE FROM LatePaymentsInSourceCurrency to LatePaymentsInLocalCurrency

      IF(
        -- COPEL-CUSTOMIZATION Inclusion of the LatePaymentsInDocumentCurrency field        
        ## CORTEX-CUSTOMER: Please add relevant Account Type. Value 'K' represents 'Vendor'
        AccountingInvoicesKPI.AccountType_KOART = 'K' AND AccountingInvoicesKPI.ClearingDate_AUGDT > AccountingInvoicesKPI.NetDueDate,
        AccountingInvoicesKPI.AmountInDocumentCurrency_WRBTR,
        0
      ) AS LatePaymentsInDocumentCurrency, 

      IF(
        ## CORTEX-CUSTOMER: Please add relevant Account Type. Value 'K' represents 'Vendor'
        AccountingInvoicesKPI.AccountType_KOART = 'K' AND AccountingInvoicesKPI.ClearingDate_AUGDT > AccountingInvoicesKPI.NetDueDate,
        AccountingInvoicesKPI.AmountInDocumentCurrency_WRBTR * CurrencyConversion.ExchangeRate_UKURS,
        0
      ) AS LatePaymentsInTargetCurrency, -- COPEL-CUSTOMIZATION CHANGE FROM DMBTR TO WRBTR

      /* Upcoming Payments */
      IF(
        ## CORTEX-CUSTOMER: Please add relevant Account Type. Value 'K' represents 'Vendor'
        ## CORTEX-CUSTOMER: Please adjust the number of days for upcoming payments
        AccountingInvoicesKPI.AccountType_KOART = 'K'
        AND AccountingInvoicesKPI.NetDueDate BETWEEN CURRENT_DATE() AND DATE_ADD(CURRENT_DATE(), INTERVAL 14 DAY)
        AND AccountingInvoicesKPI.ClearingDate_AUGDT < '1900-01-01',
        AccountingInvoicesKPI.AmountInLocalCurrency_DMBTR,
        0
      ) AS UpcomingPaymentsInLocalCurrency, -- COPEL-CUSTOMIZATION CHANGE FROM UpcomingPaymentsInSourceCurrency to UpcomingPaymentsInLocalCurrency

      IF(
        -- COPEL-CUSTOMIZATION Inclusion of the UpcomingPaymentsInDocumentCurrency field      
        ## CORTEX-CUSTOMER: Please add relevant Account Type. Value 'K' represents 'Vendor'
        ## CORTEX-CUSTOMER: Please adjust the number of days for upcoming payments
        AccountingInvoicesKPI.AccountType_KOART = 'K'
        AND AccountingInvoicesKPI.NetDueDate BETWEEN CURRENT_DATE() AND DATE_ADD(CURRENT_DATE(), INTERVAL 14 DAY)
        AND AccountingInvoicesKPI.ClearingDate_AUGDT < '1900-01-01',
        AccountingInvoicesKPI.AmountInDocumentCurrency_WRBTR,
        0
      ) AS UpcomingPaymentsInDocumentCurrency, 

      IF(
        ## CORTEX-CUSTOMER: Please add relevant Account Type. Value 'K' represents 'Vendor'
        ## CORTEX-CUSTOMER: Please adjust the number of days for upcoming payments
        AccountingInvoicesKPI.AccountType_KOART = 'K'
        AND AccountingInvoicesKPI.NetDueDate BETWEEN CURRENT_DATE() AND DATE_ADD(CURRENT_DATE(), INTERVAL 14 DAY)
        AND AccountingInvoicesKPI.ClearingDate_AUGDT < '1900-01-01',
        AccountingInvoicesKPI.AmountInDocumentCurrency_WRBTR * CurrencyConversion.ExchangeRate_UKURS,
        0
      ) AS UpcomingPaymentsInTargetCurrency, -- COPEL-CUSTOMIZATION CHANGE FROM DMBTR TO WRBTR

      /* Potential Penalty */
      IF(
        ## CORTEX-CUSTOMER: Please add relevant Account Type. Value 'K' represents 'Vendor'
        AccountingInvoicesKPI.AccountType_KOART = 'K' AND AccountingInvoicesKPI.ClearingDate_AUGDT > AccountingInvoicesKPI.NetDueDate,
        AccountingInvoicesKPI.AmountInLocalCurrency_DMBTR,
        0
      ) * COALESCE(SAFE_CAST(VendorConfig.LowField_LOW AS INT64), 0) AS PotentialPenaltyInLocalCurrency, -- COPEL-CUSTOMIZATION CHANGE FROM PotentialPenaltyInSourceCurrency to PotentialPenaltyInLocalCurrency

      IF(
        -- COPEL-CUSTOMIZATION Inclusion of the PotentialPenaltyInDocumentCurrency field   
        ## CORTEX-CUSTOMER: Please add relevant Account Type. Value 'K' represents 'Vendor'
        AccountingInvoicesKPI.AccountType_KOART = 'K' AND AccountingInvoicesKPI.ClearingDate_AUGDT > AccountingInvoicesKPI.NetDueDate,
        AccountingInvoicesKPI.AmountInDocumentCurrency_WRBTR,
        0
      ) * COALESCE(SAFE_CAST(VendorConfig.LowField_LOW AS INT64), 0) AS PotentialPenaltyInDocumentCurrency, 

      IF(
        ## CORTEX-CUSTOMER: Please add relevant Account Type. Value 'K' represents 'Vendor'
        AccountingInvoicesKPI.AccountType_KOART = 'K' AND AccountingInvoicesKPI.ClearingDate_AUGDT > AccountingInvoicesKPI.NetDueDate,
        AccountingInvoicesKPI.AmountInDocumentCurrency_WRBTR * CurrencyConversion.ExchangeRate_UKURS,
        0
      ) * COALESCE(SAFE_CAST(VendorConfig.LowField_LOW AS INT64), 0) AS PotentialPenaltyInTargetCurrency, -- COPEL-CUSTOMIZATION CHANGE FROM DMBTR TO WRBTR

      /* Purchase */
      IF(
        ## CORTEX-CUSTOMER: Please add relevant Movement Types.
        -- Value '101' represents 'GR Goods Receipt' and '501' represents 'Receipt w/o PO'
        AccountingInvoicesKPI.AccountType_KOART = 'M' AND POOrderHistory.MovementType__inventoryManagement___BWART IN ('101', '501'),
        AccountingInvoicesKPI.AmountInLocalCurrency_DMBTR,
        IF(
          ## CORTEX-CUSTOMER: Please add relevant Movement Types.
          -- Value '102' represents 'GR for PO reversal (full or any one of the line item)'
          -- Value '502' represents 'Return Receipt w/o PO' (Receipt made against movement type 501 document is cancelled.)
          AccountingInvoicesKPI.AccountType_KOART = 'M' AND POOrderHistory.MovementType__inventoryManagement___BWART IN ('102', '502'),
          AccountingInvoicesKPI.AmountInLocalCurrency_DMBTR * -1, 0
        )
      ) AS PurchaseInLocalCurrency, -- COPEL-CUSTOMIZATION CHANGE FROM PurchaseInSourceCurrency to PurchaseInLocalCurrency

      IF(
        -- COPEL-CUSTOMIZATION Inclusion of the PurchaseInDocumentCurrency field   
        ## CORTEX-CUSTOMER: Please add relevant Movement Types.
        -- Value '101' represents 'GR Goods Receipt' and '501' represents 'Receipt w/o PO'
        AccountingInvoicesKPI.AccountType_KOART = 'M' AND POOrderHistory.MovementType__inventoryManagement___BWART IN ('101', '501'),
        AccountingInvoicesKPI.AmountInDocumentCurrency_WRBTR,
        IF(
          ## CORTEX-CUSTOMER: Please add relevant Movement Types.
          -- Value '102' represents 'GR for PO reversal (full or any one of the line item)'
          -- Value '502' represents 'Return Receipt w/o PO' (Receipt made against movement type 501 document is cancelled.)
          AccountingInvoicesKPI.AccountType_KOART = 'M' AND POOrderHistory.MovementType__inventoryManagement___BWART IN ('102', '502'),
          AccountingInvoicesKPI.AmountInDocumentCurrency_WRBTR * -1, 0
        )
      ) AS PurchaseInDocumentCurrency,

      IF(
        ## CORTEX-CUSTOMER: Please add relevant Movement Types.
        -- Value '101' represents 'GR Goods Receipt' and '501' represents 'Receipt w/o PO'
        AccountingInvoicesKPI.AccountType_KOART = 'M' AND POOrderHistory.MovementType__inventoryManagement___BWART IN ('101', '501'),
        AccountingInvoicesKPI.AmountInDocumentCurrency_WRBTR * CurrencyConversion.ExchangeRate_UKURS,
        IF(
          ## CORTEX-CUSTOMER: Please add relevant Movement Types.
          -- Value '102' represents 'GR for PO reversal (full or any one of the line item)'
          -- Value '502' represents 'Return Receipt w/o PO' (Receipt made against movement type 501 document is cancelled.)
          AccountingInvoicesKPI.AccountType_KOART = 'M' AND POOrderHistory.MovementType__inventoryManagement___BWART IN ('102', '502'),
          AccountingInvoicesKPI.AmountInDocumentCurrency_WRBTR * CurrencyConversion.ExchangeRate_UKURS * -1, 0
        )
      ) AS PurchaseInTargetCurrency, -- COPEL-CUSTOMIZATION CHANGE FROM DMBTR TO WRBTR

      /* Parked Invoices */
      ## CORTEX-CUSTOMER: Please add relevant Invoice Status. Value 'A' represents that the document is Parked and not posted
      IF(AccountingInvoicesKPI.Invstatus_RBSTAT = 'A', TRUE, FALSE) AS IsParkedInvoice,

      /* Blocked Invoices */
      ## CORTEX-CUSTOMER: Please add relevant Payment Block Keys. Value 'A' represents 'Locked for Payment' and 'B' represents 'Blocked for Payment'
      IF(AccountingInvoicesKPI.PaymentBlockKey_ZLSPR > ' ', TRUE, FALSE) AS IsBlockedInvoice,

      /* Cash Discount Received */
      IF(
        ## CORTEX-CUSTOMER: Please add relevant Document Types. Value 'KZ' represents ' Vendor Payment' and 'ZP' represents 'Payment Posting'
        ## CORTEX-CUSTOMER: Please add relevant Transaction Key. Value 'SKE' represents ' Cash Discount Received'
        AccountingInvoicesKPI.AccountingDocumenttype_BLART IN ('KZ', 'ZP') AND AccountingInvoicesKPI.TransactionKey_KTOSL = 'SKE',
        AccountingInvoicesKPI.AmountInLocalCurrency_DMBTR,
        0
      ) AS CashDiscountReceivedInLocalCurrency, -- COPEL-CUSTOMIZATION CHANGE FROM CashDiscountReceivedInSourceCurrency to CashDiscountReceivedInLocalCurrency

      IF(
        -- COPEL-CUSTOMIZATION Inclusion of the CashDiscountReceivedInDocumentCurrency field  
        ## CORTEX-CUSTOMER: Please add relevant Document Types. Value 'KZ' represents ' Vendor Payment' and 'ZP' represents 'Payment Posting'
        ## CORTEX-CUSTOMER: Please add relevant Transaction Key. Value 'SKE' represents ' Cash Discount Received'
        AccountingInvoicesKPI.AccountingDocumenttype_BLART IN ('KZ', 'ZP') AND AccountingInvoicesKPI.TransactionKey_KTOSL = 'SKE',
        AccountingInvoicesKPI.AmountInDocumentCurrency_WRBTR,
        0
      ) AS CashDiscountReceivedInDocumentCurrency,

      IF(
        ## CORTEX-CUSTOMER: Please add relevant Document Types. Value 'KZ' represents ' Vendor Payment' and 'ZP' represents 'Payment Posting'
        ## CORTEX-CUSTOMER: Please add relevant Transaction Key. Value 'SKE' represents ' Cash Discount Received'
        AccountingInvoicesKPI.AccountingDocumenttype_BLART IN ('KZ', 'ZP') AND AccountingInvoicesKPI.TransactionKey_KTOSL = 'SKE',
        AccountingInvoicesKPI.AmountInDocumentCurrency_WRBTR * CurrencyConversion.ExchangeRate_UKURS,
        0
      ) AS CashDiscountReceivedInTargetCurrency, -- COPEL-CUSTOMIZATION CHANGE FROM DMBTR TO WRBTR

      /* Target Cash Discount */
      IF(
        ## CORTEX-CUSTOMER: Please add relevant Posting Key. Value '31' represents 'Vendor Invoice'
        AccountingInvoicesKPI.PostingKey_BSCHL = '31',
        (AccountingInvoicesKPI.AmountEligibleForCashDiscountInDocumentCurrency_SKFBT * AccountingInvoicesKPI.CashDiscountPercentage1_ZBD1P) / 100,
        0
      ) AS TargetCashDiscountInSourceCurrency, 

      IF(
        ## CORTEX-CUSTOMER: Please add relevant Posting Key. Value '31' represents 'Vendor Invoice'
        AccountingInvoicesKPI.PostingKey_BSCHL = '31',
        (AccountingInvoicesKPI.AmountEligibleForCashDiscountInDocumentCurrency_SKFBT * CurrencyConversion.ExchangeRate_UKURS * AccountingInvoicesKPI.CashDiscountPercentage1_ZBD1P) / 100,
        0
      ) AS TargetCashDiscountInTargetCurrency,

      /* Amount Of Open Debit Items */
      IF(
        ## CORTEX-CUSTOMER: Please add relevant Account Type. Value 'K' represents 'Vendor'
        ## CORTEX-CUSTOMER: Please add relevant Special GI Indicator. Value 'A' represents 'Down Payment'
        AccountingInvoicesKPI.Accounttype_KOART = 'K' AND AccountingInvoicesKPI.ClearingDate_AUGDT < '1900-01-01' and 
        left(AccountingInvoicesKPI.AccountingDocumentNumber_BELNR,2) not IN ('15', '20', '17', '12'),
        AccountingInvoicesKPI.AmountInLocalCurrency_DMBTR,
        0
      ) AS AmountOfOpenDebitItemsInLocalCurrency, -- COPEL-CUSTOMIZATION CHANGE FROM AmountOfOpenDebitItemsInSourceCurrency to AmountOfOpenDebitItemsInLocalCurrency

      IF(
        -- COPEL-CUSTOMIZATION Inclusion of the AmountOfOpenDebitItemsInDocumentCurrency field  
        ## CORTEX-CUSTOMER: Please add relevant Account Type. Value 'K' represents 'Vendor'
        ## CORTEX-CUSTOMER: Please add relevant Special GI Indicator. Value 'A' represents 'Down Payment'
        AccountingInvoicesKPI.Accounttype_KOART = 'K' AND AccountingInvoicesKPI.ClearingDate_AUGDT < '1900-01-01' and 
        left(AccountingInvoicesKPI.AccountingDocumentNumber_BELNR,2) not IN ('15', '20', '17', '12'),
        AccountingInvoicesKPI.AmountInDocumentCurrency_WRBTR,
        0
      ) AS AmountOfOpenDebitItemsInDocumentCurrency, -- COPEL-CUSTOMIZATION CHANGE FROM AmountOfOpenDebitItemsInSourceCurrency to AmountOfOpenDebitItemsInLocalCurrency

      IF(
        ## CORTEX-CUSTOMER: Please add relevant Account Type. Value 'K' represents 'Vendor'
        ## CORTEX-CUSTOMER: Please add relevant Special GI Indicator. Value 'A' represents 'Down Payment'
        AccountingInvoicesKPI.Accounttype_KOART = 'K' AND AccountingInvoicesKPI.ClearingDate_AUGDT < '1900-01-01' and 
        left(AccountingInvoicesKPI.AccountingDocumentNumber_BELNR,2) not IN ('15', '20', '17', '12'),
        AccountingInvoicesKPI.AmountInDocumentCurrency_WRBTR * CurrencyConversion.ExchangeRate_UKURS,
        0
      ) AS AmountOfOpenDebitItemsInTargetCurrency, -- COPEL-CUSTOMIZATION CHANGE FROM DMBTR TO WRBTR

      /* Amount Of Return */
      IF(
        ## CORTEX-CUSTOMER: Please add relevant Movement Type. Value '122' represents 'RE return to Vendor'
        POOrderHistory.MovementType__inventoryManagement___BWART = '122',
        POOrderHistory.AmountInLocalCurrency_DMBTR * POOrderHistory.Quantity_MENGE,
        0
      ) AS AmountOfReturnInLocalCurrency, -- COPEL-CUSTOMIZATION CHANGE FROM AmountOfReturnInSourceCurrency to AmountOfReturnInLocalCurrency

      IF(
        -- COPEL-CUSTOMIZATION Inclusion of the AmountOfReturnInDocumentCurrency field  
        ## CORTEX-CUSTOMER: Please add relevant Movement Type. Value '122' represents 'RE return to Vendor'
        POOrderHistory.MovementType__inventoryManagement___BWART = '122',
        POOrderHistory.AmountInDocumentCurrency_WRBTR * POOrderHistory.Quantity_MENGE,
        0
      ) AS AmountOfReturnInDocumentCurrency, 

      IF(
        ## CORTEX-CUSTOMER: Please add relevant Movement Type. Value '122' represents 'RE return to Vendor'
        POOrderHistory.MovementType__inventoryManagement___BWART = '122',
        POOrderHistory.AmountInDocumentCurrency_WRBTR * CurrencyConversion.ExchangeRate_UKURS * POOrderHistory.Quantity_MENGE,
        0
      ) AS AmountOfReturnInTargetCurrency -- COPEL-CUSTOMIZATION CHANGE FROM DMBTR TO WRBTR

    FROM AccountingInvoicesKPI
    LEFT OUTER JOIN (
      /* Vendors may contain multiple addresses that may produce multiple VendorsMD records, pick the name agaist latest entry */
      SELECT Client_MANDT, AccountNumberOfVendorOrCreditor_LIFNR, NAME1, VendorAccountGroup_KTOKK -- COPEL-CUSTOMIZATION insert VendorAccountGroup_KTOKK
      FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.VendorsMD`
      WHERE ValidToDate_DATE_TO = '9999-12-31'
    ) AS VendorsMD
    ON AccountingInvoicesKPI.Client_MANDT = VendorsMD.Client_MANDT
      AND AccountingInvoicesKPI.AccountNumberOfVendorOrCreditor_LIFNR = VendorsMD.AccountNumberOfVendorOrCreditor_LIFNR
    LEFT OUTER JOIN
      `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.VendorConfig` AS VendorConfig
      ## CORTEX-CUSTOMER Vendor Name in the config follows the format 'Z_VENDOR_{VendorId}'. Please change the logic if the name follows a different format.
      ON VendorsMD.AccountNumberOfVendorOrCreditor_LIFNR = ARRAY_REVERSE(SPLIT(VendorConfig.NameOfVariantVariable_NAME, '_'))[SAFE_OFFSET(0)]
    LEFT JOIN `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.PurchaseDocumentsHistory` AS POOrderHistory
      ON AccountingInvoicesKPI.Client_MANDT = POOrderHistory.Client_MANDT
        AND AccountingInvoicesKPI.PurchasingDocumentNumber_EBELN = POOrderHistory.PurchasingDocumentNumber_EBELN
        AND AccountingInvoicesKPI.ItemNumberOfPurchasingDocument_EBELP = POOrderHistory.ItemNumberOfPurchasingDocument_EBELP
        AND AccountingInvoicesKPI.FiscalYear_GJAHR = POOrderHistory.MaterialDocumentYear_GJAHR
        AND AccountingInvoicesKPI.ObjectKey_AWKEY = CONCAT(POOrderHistory.NumberOfMaterialDocument_BELNR, POOrderHistory.MaterialDocumentYear_GJAHR)
    LEFT JOIN CurrencyConversion
      ON
        AccountingInvoicesKPI.Client_MANDT = CurrencyConversion.Client_MANDT
        AND AccountingInvoicesKPI.CurrencyKey_WAERS = CurrencyConversion.FromCurrency_FCURR
        -- AND AccountingInvoicesKPI.PostingDateInTheDocument_BUDAT = CurrencyConversion.ConvDate
        AND AccountingInvoicesKPI.PreviousBusinessDay_BUDAT = CurrencyConversion.ConvDate