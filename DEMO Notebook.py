from pyspark.sql.functions import *
from pyspark.sql.types import DoubleType,IntegerType,StringType
import pyspark.sql.functions as F
from pyspark.sql.window import Window

df_lead_report_tab= spark.sql("""select cast(LEAD_ID as int) LEAD_ID_L,
                   FIRST_NAME_AR,
                   LAST_NAME_AR,
                   MANUAL_LEAD,
                   C_MANUFACTURER,
                   C_MODEL,
                   GF_SESSION_COUNT,
                   GF_PAGE_VIEW,
                   GF_CLIENT_ID,
                   BDC_NEXT_ACTION_20,
                   LAST_MANAGEMENT_NOTE,
                   PROCESS_TYPE,
                   EMPLOYEE_ID,
                   STATUS,
                   ORIGIN,
                   CREATED_BY,
                   CREATED_ON,
                   ENQUIRY_ID as ENQUIRY_ID_L,
                   LEAD_SOURCE,
                   LANGUAGE,
                   FIRST_NAME_EN,
                   LAST_NAME_EN,
                   CLOSED_DATE,
                   MEDIUM,
                   SOURCE,
                   GF_CAMPAIGN,
                   TRADE_IN_FLAG,
                   GF_REC_DATE,
                   GF_MAKE_AR,
                   GF_MODEL_AR,
                   NOTES_EN,
                   GF_URL,
                   GF_KEYWORD,
                   REVIEWED_USER,
                   REVIEWED_DATE,
                   CURRENT_VEHICLE_DRI_MODEL,
                   CURRENT_VEHICLE_DRI_MAKE,
                   LEAD_PASSED_ON,
                   LEAD_PASSED_BY,
                   LEAD_PASSED_ON_TIM,
                   NEXT_ACTION_DATE,
                   FIRST_MANAGEMENT_ACTION_DATE,
                   SALES_EXECUTIVE_ASSIGNED_DATE,
                   CLOSE_REASON_DESCRIPTION,
                   MANAGEMENT_NEXT_ACTION_DATE,
                   LAST_MANAGEMENT_ACTION_DATE,
                   FIRST_MANAGEMENT_NOTE,
                   CAMPAIGN
                   from silver.lead_data""")
df_lead_report_tab.createOrReplaceTempView('lead_report_tab')



df_enquiry_report_tab=spark.sql("""select cast(ENQUIRY_ID as int) ENQUIRY_ID_E,
                      FIRST_ACTION_DATE,
                      cast(LEAD_ID as int) LEAD_ID_E,
                      ENQUIRY_TYPE,
                      AGG_APPOINTMENT_NUMBER,
                      AGG_FOLL_NUMBER,
                      APPOINTMENT_DATE_FIRST,
                      APPOINTMENT_DATE_LAST,
                      CREATED_ON,
                      BE_BACKS,
                      APPOINTMENT_SHOWS,
                      STATUS,
                      APPOINTMENT_START_DATE,
                      APPOINTMENT_START_TIME,
                      HYBRIS_CAMPAIGN
                      from silver.enquiry_data""")
df_enquiry_report_tab.createOrReplaceTempView('enquiry_report_tab')

#ZIF_FFT_BI_ACTVT
df_ft_bi_interface=spark.sql("""select ENQUIRY_NUMBER,
                                       TEST_DRIVE_PRINT,
                                       LAST_STATUS_UPDATE,
                                       SAP_ORDER_NUMBER,
                                       MAKE,
                                       FIRST_ORDERED_DATE,
                                       FIRST_ORDERED,
                                       BUSINESS_MANAGER,
                                       FASTRACK_INTERFACE_FINANCIAL_SCHEME_NAME,
                                       TEST_DRIVE_PRINT,
                                       RETURN_DATE,
                                       LOCATOR_DATE,
                                       OFFER_DATE,
                                       CREATE_SALES_ORDER,
                                       PURCHASE_AGREEMENT,
                                       DELIVERED,
                                       CREATE_SALES_ORDER_TIME
                                   from silver.bi_interface""")
df_ft_bi_interface.createOrReplaceTempView('ft_bi_interface')

#ZIF_FFT_BI_TRADE
df_ft_bi_trade_in=spark.sql("""select cast(ENQUIRY_NUMBER as int) as ENQUIRY_NUMBER_T,
                                CREATION_DATE,APPRAISAL_COMPLETED_DATE,
                      TI_FIRST_VALUE,
                      TI_FIRST_VALUE_DATE,
                      TI_FIRST_VALUE_USER,
                      TI_LAST_VALUE,
                      TI_LAST_VALUE_DATE,
                      TI_LAST_VLAUE_USER,
                      TI_ALLOWANCE,
                      TI_ALLOWANCE_DATE,
                      TI_ALLOWANCE_USER_NAME,
                      TI_REMOVED_DATE,
                      TI_REMOVED_USER from silver.trade_in""")
df_ft_bi_trade_in.createOrReplaceTempView('ft_bi_trade_in')


# ZIF_FFT_BI_SALES
df_ft_sales_attributes = spark.sql("""select cast(ENQUIRY_NUMBER as int) ENQUIRY_NUMBER_S,
                      CUSTOMER_ID as CUSTOMER_ID_S,
                      SOURCE_CONTACT_TYPE,
                      ENQUIRY_SOURCE,
                      PREVIOUS_VEHICLE_MAKE,
                      PREVIOUS_VEHICLE_MODEL,
                      EXCHANGE_TAKEN,
                      DOWNPAYMENT,
                      SALES_RETURNED,
                      MATERIAL_DESCRIPTION,
                      VARIANT,
                      MATERIAL_NUMBER,
                      VARIANT_CODE,
                      VIN_NO,
                      FRANCHISE,
                      VEHICLE_OF_INTEREST,
                      LOST_REASON,
                      SALES_TYPE,
                      ---CUSTOMER_FULL_NAME,
                      SALES_OFFICE,
                      BRANCE_MANAGER_NAME,
                      SALES_EXECUTIVE_NAME,
                      LOST_TO_REASON,
                      TRADEIN_FLAG,
                      CURRENT_MAKE,
                      CURRENT_MODEL,
                      ENQUIRY_CREATED_DATE,
                      STATUS,
                      NEW_USED,
                      EXPECTED_DELIVERY_DATE,
                      ACTUAL_DELIVERY_DATE,
                      DEMO_TAKEN,
                      TRADEINMATFLG,
                      TRADEIN_VIN,
                      BRANCH_ID,
                      REJ_REASON,
                      TRPOVAL,
                      TRSTATUSDD,
                      REJ_REASON_TR,
                      AEDAT,
                      BLDAT
                      --ENQUIRY_CREATED_TIME
                      from silver.sales""")
df_ft_sales_attributes.createOrReplaceTempView('ft_sales_attributes')

# ZIF_FFT_BI_CUST
df_ft_customer_attributes=spark.sql("""select
                                       CUSTOMER_ID,
                                       cast(CUSTOMER_ID as int) CUSTOMER_ID_C,
                                       CUSTOMER_NAME,
                                       EID_NUMBER_SINGLE,
                                       EID_NUMBER,
                                       GENDER,
                                       NATIONALITY,
                                       DATE_OF_BIRTH,
                                       PO_BOX,
                                       CITY,
                                       HOUSE_NUMBER,
                                       BUILDING_NAME,
                                       STREET_NAME_NUMBER,
                                       AREA,
                                       CITY_1,
                                       CITY_POSTAL_CODE,
                                       EXISTING_CUSTOMER,
                                       FIRST_NAME,
                                       LAST_NAME,
                                       EMAIL_ADDRESS,
                                       TELEPHONE_NO,
                                       NEAREST_LAND_MARK
                                    from silver.customer_master""")
df_ft_customer_attributes.createOrReplaceTempView('ft_customer_attributes')

df_final_1=spark.sql(""" select e.ENQUIRY_ID_E as EnquiryNo,
                      e.FIRST_ACTION_DATE as FirstActionDate,
                      e.LEAD_ID_E,
                      e.ENQUIRY_TYPE as EnquiryType,
                      e.AGG_APPOINTMENT_NUMBER as ApptCount,
                      e.APPOINTMENT_DATE_FIRST as FirstVisit,
                      e.APPOINTMENT_DATE_LAST as LastVisit,
                      e.BE_BACKS as BeBacks,
                      e.APPOINTMENT_SHOWS as ApptShows,
                      e.STATUS as AppointmentStatusName,
                      e.APPOINTMENT_START_DATE as AppointmentDate,
                      e.APPOINTMENT_START_TIME,
                      e.CREATED_ON as EnquiryCreated,
                      i.TEST_DRIVE_PRINT as Demo,
                      i.LAST_STATUS_UPDATE as LastStatusUpdate,
                      i.SAP_ORDER_NUMBER as SAPOrderNumber,
                      i.MAKE as Make,
                      i.FIRST_ORDERED_DATE as FirstOrdered,
                      i.FIRST_ORDERED as 1stOrder,
                      i.BUSINESS_MANAGER as BusinessManager,
                      i.FASTRACK_INTERFACE_FINANCIAL_SCHEME_NAME as CashFinance,
                      i.TEST_DRIVE_PRINT as TestDrivePrint,
                      i.RETURN_DATE as ReturntoEnquiry,
                      i.LOCATOR_DATE as Locator,
                      i.OFFER_DATE as Offer,
                      i.CREATE_SALES_ORDER as CreateSalesOrder,
                      i.PURCHASE_AGREEMENT as PurchaseAgreement,
                      i.DELIVERED as Delivered,
                      i.CREATE_SALES_ORDER_TIME,
                      --t.CREATION_DATE as DateCreated,
                      t.APPRAISAL_COMPLETED_DATE as AppraisalCompleted,
                      t.TI_FIRST_VALUE as TI1stValue,
                      t.TI_FIRST_VALUE_DATE as TI1stValued,
                      t.TI_FIRST_VALUE_USER as TI1stValueUser,
                      t.TI_LAST_VALUE as TILastValue,
                      t.TI_LAST_VALUE_DATE as TILastValued,
                      t.TI_LAST_VLAUE_USER as TILastValueUser,
                      t.TI_ALLOWANCE as TIAllowance,
                      t.TI_ALLOWANCE_DATE as TIAllowanceSet,
                      t.TI_ALLOWANCE_USER_NAME as TIAllowanceUserName,
                      t.TI_REMOVED_DATE as TIRemoved,
                      t.TI_REMOVED_USER as TIRemovedUser,
                      s.ENQUIRY_NUMBER_S,
                      s.CUSTOMER_ID_S,
                      s.ENQUIRY_SOURCE as SourceOfEnquiry,
                      s.PREVIOUS_VEHICLE_MODEL as Model,
                      s.DOWNPAYMENT as DownPayment,
                      s.MATERIAL_DESCRIPTION as Derivative,
                      s.VARIANT as Variant,
                      s.MATERIAL_NUMBER as ArticleCode,
                      s.VARIANT_CODE as VariantCode,
                      s.VIN_NO as `VIN_NO-VIN_Current`,
                      s.FRANCHISE as Franchise,
                      s.VEHICLE_OF_INTEREST as VehicleofInterest,
                      s.LOST_REASON as LostReason,
                      s.SALES_TYPE as SalesType,
                      --s.CUSTOMER_FULL_NAME as Customer,
                      s.SALES_OFFICE as Branch,
                      s.SALES_EXECUTIVE_NAME as SalesExec,
                      s.TRADEIN_FLAG as TradeInFlag,
                      s.CURRENT_MAKE as CurrentMake,
                      s.CURRENT_MODEL as CurrentModel,
                      s.ENQUIRY_CREATED_DATE as DateCreated,
                      s.STATUS as Status,
                      s.NEW_USED as NewUsed,
                      s.EXPECTED_DELIVERY_DATE as ExpDeliveryDate,
                      s.ACTUAL_DELIVERY_DATE as ActDeliveryDate,
                      s.DEMO_TAKEN as DemoTaken,
                      s.TRADEINMATFLG as TradeInMatFlg,
                      s.TRADEIN_VIN as TradeIn_VIN,
                      s.BRANCH_ID as BranchId,
                      s.REJ_REASON,
                      s.TRPOVAL,
                      s.TRSTATUSDD,
                      s.REJ_REASON_TR,
                      s.AEDAT,
                      s.BLDAT
                      --s.ENQUIRY_CREATED_TIME
                      from enquiry_report_tab e
                      left join ft_bi_interface i on e.ENQUIRY_ID_E = i.ENQUIRY_NUMBER
                      left join ft_bi_trade_in t on e.ENQUIRY_ID_E = t.ENQUIRY_NUMBER_T
                      left join ft_sales_attributes s on e.ENQUIRY_ID_E = s.ENQUIRY_NUMBER_S
                       """)
df_final_1.createOrReplaceTempView('final_1')

df_final=spark.sql("""
                   select f.*,
                   l.LEAD_ID_L,
                   l.MANUAL_LEAD as ManualLead,
                   l.C_MODEL,
                   l.GF_SESSION_COUNT as SessionCount,
                   l.GF_PAGE_VIEW as PageViewCount,
                   l.GF_CLIENT_ID as ClientID,
                   l.BDC_NEXT_ACTION_20 as BDCNextAction,
                   l.LAST_MANAGEMENT_NOTE as LastManagementNotes,
                   l.EMPLOYEE_ID as SalesExecID,
                   l.ORIGIN as LeadChannel,
                   l.CREATED_BY as ManualLeadCreatedBy,
                   l.CREATED_ON as LeadCreated,
                   l.LEAD_SOURCE as LeadSource,
                   l.LANGUAGE as LanguageName,
                   l.FIRST_NAME_EN as EnglishForename,
                   l.CLOSED_DATE as ClosedDate,
                   l.MEDIUM as Medium,
                   l.SOURCE as LeadType,
                   l.GF_REC_DATE as ReceivedDate,
                   l.GF_MAKE_AR as ArabicMake,
                   l.GF_MODEL_AR as ArabicModel,
                   l.NOTES_EN as EnglishComment,
                   l.GF_URL as LeadURL,
                   l.GF_KEYWORD as KeywordsTerm,
                   l.REVIEWED_USER as ReviewedUser,
                   l.REVIEWED_DATE as ReviewedDate,
                   l.CURRENT_VEHICLE_DRI_MODEL as EnglishModel,
                   l.CURRENT_VEHICLE_DRI_MAKE as EnglishMake,
                   l.LEAD_PASSED_ON as PassToBranchDate,
                   l.LEAD_PASSED_BY as PassToBranchUser,
                   l.LEAD_PASSED_ON_TIM ,
                   l.NEXT_ACTION_DATE as NextActionDate,
                   l.FIRST_MANAGEMENT_ACTION_DATE as FirstManagementActionDate,
                   c.CUSTOMER_ID as Customer_ID_Connect,
                   --c.CUSTOMER_ID_C as Customer_ID_Connect,
                   c.CUSTOMER_NAME as Customer,
                   c.EID_NUMBER_SINGLE as EIDNumber,
                   c.GENDER as Gender,
                   c.NATIONALITY as Nationality,
                   c.DATE_OF_BIRTH as DOB,
                   c.PO_BOX as POBox,
                   c.CITY as Emirate,
                   c.HOUSE_NUMBER as FlatVillaNo,
                   c.BUILDING_NAME as BuildingName,
                   c.STREET_NAME_NUMBER as StreetNameNumber,
                   c.AREA as Area,
                   c.CITY_1 as Emirate2,
                   c.EXISTING_CUSTOMER as ExistingCustomer,
                   c.EMAIL_ADDRESS as EmailAddress,
                   c.TELEPHONE_NO as PhoneMobile
                   from final_1 f
                   left join lead_report_tab l on f.LEAD_ID_E == l.LEAD_ID_L
                   left join ft_customer_attributes c on f.customer_id_S == c.customer_id_C
                   """)
#df_final.createOrReplaceTempView('final')

df_final=df_final.filter("!IsNull(EnquiryNo) and !IsNull(EnquiryCreated) and !IsNull(EnquiryType)")
#df_final=df_final.filter("ENQUIRY_NUMBER_S != '334095' and ENQUIRY_NUMBER_S != '412601'")
df_final=df_final.withColumn('RowLastUpdated',lit(''))
df_final=df_final.withColumn('SourceType',lit('SAP'))

df_final=df_final.filter("!IsNull(EnquiryNo) and !IsNull(EnquiryCreated) and !IsNull(EnquiryType)")
#df_final=df_final.filter("ENQUIRY_NUMBER_S != '334095' and ENQUIRY_NUMBER_S != '412601'")
df_final=df_final.withColumn('RowLastUpdated',lit(''))
df_final=df_final.withColumn('SourceType',lit('SAP'))


reg_exp="\s+"
for colname in df_final.columns:
  data_type_list=df_final.select(colname).dtypes[0][1]
  if data_type_list=='string':
    df_final = df_final.withColumn(colname,trim(regexp_replace(col(colname), reg_exp," ")))
    df_final = df_final.withColumn(colname,regexp_replace(col(colname),'\n|\t',""))


NULL_COLUMN_LIST=['RowLastUpdated','FirstOrderedPAPrint','HybrisCampaign','1stContact','HUBSubmit','1stApprovedStatus','Invoiced','WayNumber','PC','NearestLandmark','Employer','Salary','LostTo','BankingWith','Leadtatus','ArabicForename','EnglishSurname','ArabicSurname','ArabicComment','UniqueLead','AssignedToSalesExecDate','CloseReasonDescription','ManagementNextActionDate','LastManagementActionDate','FirstManagementNotes','Campaign','Content']
for J in NULL_COLUMN_LIST:
    df_final=df_final.withColumn(J,lit(None))


FORMAT_CHANGE_List=['Demo','ReturntoEnquiry','Locator','Offer','PurchaseAgreement','1stOrder','CreateSalesOrder','AppointmentDate','DateCreated','AppraisalCompleted','TI1stValued','TILastValued','TIAllowanceSet','TIRemoved','PassToBranchDate','DOB','FirstOrderedPAPrint','DateCreated']

for f in FORMAT_CHANGE_List:
    df_final=df_final.withColumn(f,regexp_replace(col(f), "-", ""))
    df_final=df_final.withColumn(f,when(col(f).isNull(),lit(''))
                                   .when((col(f)=='0') | (col(f)=='00') | (col(f) == '') | (col(f) == '00000000'),lit(''))
                                   .otherwise(concat(substring(col(f),1,4),lit('-'),substring(col(f),5,2),lit('-'),substring(col(f),7,2),lit(' 00:00:00'))))

FORMAT_CHANGE_List_2=['Invoiced','FirstOrdered','TestDrivePrint','LastStatusUpdate','FirstVisit','LastVisit','FirstActionDate','ActDeliveryDate','ExpDeliveryDate','LeadCreated','ClosedDate','ReceivedDate','ReviewedDate','AssignedToSalesExecDate','NextActionDate','FirstManagementActionDate','ManagementNextActionDate','EnquiryCreated','LastManagementActionDate']

for i in FORMAT_CHANGE_List_2:
    df_final = df_final.withColumn(i, regexp_replace(col(i), "-", ""))
    df_final = df_final.withColumn(i, when(col(i).isNull(), lit(''))
                                   .when((col(i) == '0') | (col(i) == '00') | (col(i) == '') | (col(i) == '00000000'),
                                         lit(''))
                                   .otherwise(
        concat(substring(col(i), 1, 4), lit('-'), substring(col(i), 5, 2), lit('-'), substring(col(i), 7, 2), lit(' '),
               substring(col(i), 9, 2), lit(':'), substring(col(i), 11, 2), lit(':'), substring(col(i), 13, 2))))


FORMAT_CHANGE_List_3=['Delivered']
for i in FORMAT_CHANGE_List_3:
    df_final = df_final.withColumn(i, regexp_replace(col(i), "-", ""))
    df_final = df_final.withColumn(i, when(col(i).isNull(), lit(''))
                                   .when((col(i) == '0') | (col(i) == '00') | (col(i) == '') | (col(i) == '00000000'),
                                         lit(''))
                                   .when(length(col(i)) == 8,
                                         concat(substring(col(i), 1, 4), lit('-'), substring(col(i), 5, 2), lit('-'),
                                                substring(col(i), 7, 2), lit(' 00:00:00')))
                                   .otherwise(
        concat(substring(col(i), 1, 4), lit('-'), substring(col(i), 5, 2), lit('-'), substring(col(i), 7, 2), lit(' '),
               substring(col(i), 9, 2), lit(':'), substring(col(i), 11, 2), lit(':'), substring(col(i), 13, 2))))




df_final=df_final.withColumn('1stVisit',col('FirstVisit')).withColumn('Source',col('LeadType')).withColumn('Emirate2',col('Emirate'))
df_final=df_final.withColumn('1stOrder',col('FirstOrdered'))
df_final = df_final.withColumn('LeadChannel', when(substring(col('LeadType'), 1, 3) == 'Web', lit('Web'))
                               .when(substring(col('LeadType'), 1, 3) == 'Tel', lit('Telephone'))
                               .when(substring(col('LeadType'), 1, 3) == 'Sho', lit('Showroom')).otherwise(lit(None)))


df_final=df_final.withColumn('EnquiryType',when((col('EnquiryType') == 'Showroom') | (col('EnquiryType') == 'Walk-In'),lit('Showroom/Walk-In'))
                                           .otherwise((col('EnquiryType'))))

df_final=df_final.withColumn('TradeInFlag',when((col('TradeInFlag') == '') | (col('TradeInFlag') == 'No') |  col('TradeInFlag').isNull(),lit('N')).otherwise((lit('Y'))))

df_final=df_final.withColumn('AppointmentStatusName',initcap(col('AppointmentStatusName'))).withColumn('Franchise',initcap(col('Franchise')))
df_final=df_final.withColumn('EIDNumber',when((col('EIDNumber') == '0'),lit(None)).otherwise(col('EIDNumber')))
df_final=df_final.withColumn('EmailAddress',when((col('EmailAddress')).isNull() | (col('EmailAddress') == ''),lit('No')).otherwise(lit('Yes')))

df_final=df_final.withColumn('PhoneMobile',when((col('PhoneMobile')).isNull() | (col('PhoneMobile') == ''),lit('No')).otherwise(lit('Yes')))
# df_final=df_final.withColumn('PhoneMobile',when((col('PhoneMobile')).isNotNull(),lit('Yes')).otherwise(lit('No')))


df_final = df_final.withColumn('CREATE_SALES_ORDER_TIME',
                               concat(substring(col('CREATE_SALES_ORDER_TIME'), 1, 2), lit(':'),
                                      substring(col('CREATE_SALES_ORDER_TIME'), 3, 2), lit(':'),
                                      substring(col('CREATE_SALES_ORDER_TIME'), 5, 2)))

df_final = df_final.withColumn('CreateSalesOrder', concat(substring(col('CreateSalesOrder'), 1, 10), lit(' '),
                                                          col('CREATE_SALES_ORDER_TIME')))


df_final=df_final.select('RowLastUpdated','EnquiryNo','Branch','EnquiryCreated','EnquiryType','LeadCreated','LeadType','SourceOfEnquiry','FirstVisit','LastVisit','Demo','FirstOrderedPAPrint','Status','LastStatusUpdate','SAPOrderNumber','NewUsed','SalesExec','Customer','EIDNumber','Make','Model','Derivative','Variant','ArticleCode','VariantCode','HybrisCampaign','ExpDeliveryDate','ActDeliveryDate','SalesType','FirstOrdered','VIN_No-VIN_Current','DateCreated','AppraisalCompleted','TI1stValue','TI1stValued','TILastValue','TILastValued','TILastValueUser','TIAllowance','TIAllowanceSet','TIAllowanceUserName','TIRemoved','TIRemovedUser','1stOrder','TI1stValueUser','BusinessManager','1stContact','1stVisit','CashFinance','TestDrivePrint','ReturntoEnquiry','Locator','Offer','CreateSalesOrder','DownPayment','PurchaseAgreement','HUBSubmit','1stApprovedStatus','Invoiced','Delivered','Franchise','Gender','Nationality','DOB','POBox','Emirate','FlatVillaNo','WayNumber','BuildingName','StreetNameNumber','Area','Emirate2','PC','NearestLandmark','VehicleofInterest','Employer','Salary','LostTo','LostReason','BankingWith','ReceivedDate','LeadChannel','LeadSource','Leadtatus','LanguageName','EnglishForename','ArabicForename','EnglishSurname','ArabicSurname','EmailAddress','PhoneMobile','EnglishMake','ArabicMake','EnglishModel','ArabicModel','EnglishComment','ArabicComment','ExistingCustomer','UniqueLead','ReviewedUser','ReviewedDate','PassToBranchUser','PassToBranchDate','AssignedToSalesExecDate','AppointmentDate','AppointmentStatusName','DemoTaken','CloseReasonDescription','ClosedDate','BDCNextAction','NextActionDate','FirstManagementActionDate','ManagementNextActionDate','LastManagementActionDate','FirstManagementNotes','LastManagementNotes','FirstActionDate','ManualLead','ManualLeadCreatedBy','LeadURL','Medium','Source','Campaign','Content','KeywordsTerm','SessionCount','PageViewCount','ClientID','BeBacks','CurrentMake','CurrentModel','TradeInFlag','ApptCount','ApptShows','Customer_ID_Connect','SourceType','BranchId','TradeInMatFlg','TradeIn_VIN','REJ_REASON','TRPOVAL','TRSTATUSDD','REJ_REASON_TR','AEDAT','BLDAT')


update_sub_statement = ','.join(spark.sparkContext.parallelize(df_final_format.columns).map(lambda x: '`'+x+'`'+'=SOURCE.'+'`'+x+'`').collect())
final_update_statement='UPDATE SET ' + update_sub_statement

final_update_statement=final_update_statement + " ,LOAD_DATE = current_date()"

print(final_update_statement)

merge_statement = """MERGE INTO gold.enquiry TARGET
                      USING df_final_format SOURCE
                      ON  TARGET.EnquiryNo = SOURCE.EnquiryNo and TARGET.SourceType = SOURCE.SourceType
                      WHEN MATCHED THEN {final_update_statement} 
                     WHEN NOT MATCHED THEN {final_insert_statement}""".format \
    (final_update_statement=final_update_statement, final_insert_statement=final_insert_statement)

print(merge_statement)



spark.sql(merge_statement)

