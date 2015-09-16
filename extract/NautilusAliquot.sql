/*
    Description:
        Retrieve the staging table of Nautilus aliquots for upntb
    Target:
        Oracle
    Author:
        Alex Felmeister <felmeistera@email.chop.edu>
*/
select distinct
sdg.sdg_id as "sdg_id",
sdg.name as "sample_subject_name",
sdgu.u_collection_site as "collection_site",
case
when sdg.external_reference = '0' then sdg.name
else lpad(sdg.external_reference, 16)
end as "potential_universal_id",
us.u_sd_visit_name as "visit_name",
a.aliquot_id as "aliquot_id",
a.name as "aliquot_name",
af.parent_aliquot_id as "parent_aliquot_id",
a.sample_id as "visit_id",
a.received_on as "received_on",
au.u_sample_type as "sample_type_code",
au.u_secondary_sample_type  as "secondary_sample_code",
p.phrase_description as "sample_type",
p1.phrase_description as "secondary_sample_type",
p.phrase_description || ' ' || p1.phrase_description as "full_sample_type_desc",
au.u_sd_collect_event_name as "collection_event_name",
au.u_drw_alq_note as "draw_note",
au.u_tissue_type as "tissue_type",
au.u_specimen_category as "specimen_category",
au.u_collect_method as "collect_method",
au.u_received_date_time as "received_date_time",
cast(au.u_volume_received as float)  as "volume_received",
cast(au.u_vol_remain as float) as "volume_remaining",
u.name as "vol_units",
cast(au.u_conc as float) as "concentration",
au.u_units as "conc_units",
a.unit_id as "unit_id",
case when a.location_id = '426' then 'Y'
else null
end as "disposed_flag",
case when au.u_alq_status is not null or a.location_id = '426' then null
else 'Y'
end as "available_flag",
au.u_disposed as "disposed",
a.location_id as "location_id",
to_date(au.u_collect_date_time, 'dd-mon-RR') as "collect_date_time"
from
sdg sdg,
sdg_user sdgu,
sample s,
sample_user us,
aliquot_formulation af,
aliquot a,
aliquot_user au,
unit u,
phrase_entry p,
phrase_entry p1
where a.aliquot_id = au.aliquot_id
and s.sdg_id = sdg.sdg_id
and sdg.sdg_id=sdgu.sdg_id
and s.sample_id = a. sample_id
and s.sample_id = us.sample_id
and a.aliquot_id = af.child_aliquot_id(+)
and u.unit_id(+)=a.unit_id
/*left join phrase_entry table two times for sample type and secondary sample type*/
and au.u_sample_type = p.phrase_name(+)
and au.u_secondary_sample_type = p1.phrase_name(+)
/*hot fix to account for duplicate FRZM phrase types*/
and (p1.phrase_description != 'Frozen Media'
or p1.phrase_description is null)
and (p1.phrase_description not like 'EDTA P%%'
or p1.phrase_description is null)
and (p1.phrase_description not like '%%PENN%%'
or p1.phrase_description is null)
and (p.phrase_description not like '%%PENN%%'
or p.phrase_description is null)
/*hot fix to remove TISS-Flash Frozen*/
and p.phrase_description NOT LIKE 'TISS'
/*hot fix for removal of CELN CSF BLD FFRZ FFZM as sample type descriptions because of lack of real phrase list*/
and p.phrase_description NOT LIKE 'CELN'
and p.phrase_description NOT LIKE 'CSF'
and p.phrase_description NOT LIKE 'BLD'
and (p1.phrase_description != 'FFRZ'
or p1.phrase_description is null)
and (p1.phrase_description != 'FFZM'
or p1.phrase_description is null)
and (p1.phrase_description != 'FFZM'
or p1.phrase_description is null)
and a.sample_id in
  (select s.sample_id
  from sample s
  where s.sdg_id in
    (select
    sdg.sdg_id
    from sdg sdg, sdg_user usdg
    where sdg.sdg_id = usdg.sdg_id
    and usdg.u_sd_study_name = '7316'
    and sdg.status != 'X'
    and sdg.status != 'U'
    and sdg.status != 'S'))
and a.status != 'X'
and a.status != 'U'
and a.status != 'S'
order by a.aliquot_id, a.received_on
