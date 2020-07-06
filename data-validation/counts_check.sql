
--------------Dim counts--------------
select count(*) from dim_futurecart_calendar;
21553


select count(*) from dim_futurecart_call_center;
25

select count(*) from dim_futurecart_case_category;
27

select count(*) from dim_futurecart_case_priority;
25

select count(*) from dim_futurecart_country;
193

select count(*) from dim_futurecart_employee;
300024

select count(*) from dim_futurecart_product;
92353

select count(*) from dim_futurecart_questions;
5

-----------fact counts-------------------

select count(*) from futurecart_case_dly;
808264

select count(*) from futurecart_case_dly where status = 'Open';
498000

select count(*) from futurecart_case_dly where status = 'Closed';
310264

select count(*) from futurecart_survey_dly;
399000

select count(*) from futurecart_dw.fact_futurecart_case_survey_dly;
498000

select count(*) from futurecart_dw.pivot_fact_futurecart_case_survey_dly;
498000

