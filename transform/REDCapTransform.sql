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
    d.date_enrolled is not null and
    d.ethnicity  is not null and
    race is not null;

/* Create Meal Table */
drop table if exists meal CASCADE;
create table meal as
select distinct
    m.study_id as meal_id,
    m.redcap_event_name as meal_type,
    m.meal_description,
    m.healthy,
    ehb.ehb_id
from
    meal_description_form as m,
    ehb_link ehb
where
    ehb.external_id = m.study_id;


/* Create Visit Table */
drop table if exists visit CASCADE;
create table visit as
select distinct
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
    ehb.external_id = v.study_id;



/* Visit Meds */
