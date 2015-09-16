/*
    Description:
        Retrieve the staging table of Nautilus Visits for upntb
    Target:
        Oracle
    Author:
        Alex Felmeister <felmeistera@email.chop.edu>
*/
select
s.sample_id as "visit_id",
sdg.name as "sample_subject_id",
sdg.sdg_id as "sdg_id",
s.received_on as "received_on",
us.u_sd_group_name as "sd_group_name",
us.u_sd_visit_name as "visit_name",
s.description as "visit_description",
us.u_visit_time_date as "visit_time_date"
/*sample_id as "sample_id"*/
from sample s,
sample_user us,
sdg sdg
where s.sample_id=us.sample_id
and sdg.sdg_id=s.sdg_id
and s.sdg_id in
(select
sdg.sdg_id
from sdg sdg, sdg_user usdg
where sdg.sdg_id = usdg.sdg_id
and usdg.u_sd_study_name = '7316'
and sdg.status != 'X'
and sdg.status != 'U'
and sdg.status != 'S')
and s.status != 'X'
and s.status != 'U'
and s.status != 'S'
