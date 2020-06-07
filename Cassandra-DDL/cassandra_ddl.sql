create table fc_survey_event(
key int primary key,
Q1 int,
Q3 int,
Q2 int,
Q5 int,
Q4 text,
case_no text,
survey_timestamp text,
survey_id text);


create table fc_case_event(
key int primary key,
status text,
category text,
sub_category text,
last_modified_timestamp text,
case_no text,
create_timestamp text,
created_employee_key text,
call_center_id text,
product_code text,
country_cd text,
communication_mode text);