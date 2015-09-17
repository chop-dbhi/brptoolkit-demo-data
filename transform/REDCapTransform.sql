/* Create Subject Table */
drop table if exists subject CASCADE;
create table subject as
select distinct
    d.study_id,
    d.date_enrolled,
    d.ethnicity,
    d.race,
    d.sex,
    d.given_birth,
    d.num_children,
    ehb.ehb_id,
    ehb.ehb_id_int
from
    demographics d,
    ehb_link ehb
where
    ehb.external_id = d.study_id and
    d.form_status is not null;

/* Create Meal Table */
drop table if exists meal CASCADE;
create table meal as
select distinct
    m.study_id as visit_id,
    m.redcap_event_name as meal_type,
    m.meal_description,
    m.healthy,
    ehb.ehb_id
from
    meal_description_form as m,
    ehb_link ehb
where
    ehb.external_id = m.study_id and
    m.form_status is not null;


/* Create Visit Table */
drop table if exists visit CASCADE;
create table visit as
select distinct
    row_number() over()::int as id,
    v.redcap_event_name as visit_type,
    cast(v.height as integer) as height,
    cast(v.weight as integer) as weight,
    cast(v.prealb_b as float) as prealbumin,
    cast(v.creat_b as float) as creatine,
    cast(v.chol_b as float) as total_chol,
    cast(v.transferrin_b as float) as transferrin,
    /* Does patient have an IBD diag at time of visit */
    case when v.ibd_flag = 'No' then
        True
    else
        false end as ibd_flag,
    ehb.ehb_id
    /* Does not include date of diag info */
from
    baseline_visit_data as v,
    ehb_link ehb
where
    ehb.external_id = v.study_id and
    v.form_status is not null;

/* Visit Meds */
drop table if exists visit_medications CASCADE;
create table visit_medications as
select distinct
    row_number() over()::int as id,
    m.study_id as visit_id,
    m.redcap_event_name as visit_type,
    m.med_type as med_type,
    ehb.ehb_id
from
    ehb_link as ehb,
    (
        select
            study_id,
            redcap_event_name,
            meds___1 as med_type
        from
            baseline_visit_data
        where
            meds___1 is not null
        union all
        select
            study_id,
            redcap_event_name,
            meds___2 as med_type
        from
            baseline_visit_data
        where
            meds___2 is not null
        union all
        select
            study_id,
            redcap_event_name,
            meds___3 as med_type
        from
            baseline_visit_data
        where
            meds___3 is not null
        union all
        select
            study_id,
            redcap_event_name,
            meds___4 as med_type
        from
            baseline_visit_data
        where
            meds___4 is not null
        union all
        select
            study_id,
            redcap_event_name,
            meds___5 as med_type
        from
            baseline_visit_data
        where
            meds___5 is not null
    ) m
where
    ehb.external_id = m.study_id;
