/* Intermediate ehb_link table to create hashed id */
drop table if exists ehb_link_md5;
create table ehb_link_md5 as

select
    id,
    md5(ehb_id::text) as ehb_id_md5,
    ehb_id,
    external_system_id,
    external_id,
    organization_id,
    organization_subject_id,
    created,
    dob
from ehb_link;

/* Portal Subject Table Creation */
drop table if exists portal_subject;
create table portal_subject as

select
    distinct e.ehb_id_md5 as ehb_id,
    'Z'||cast(r.research_id as varchar(255)) as research_id
from
(
    select
        distinct ehb_id,
        round((CAST(ehb_id as integer)*1.77)*100, 0) as research_id
    from ehb_link_md5
    group by ehb_id
    order by ehb_id
) r,
ehb_link_md5 e
where e.ehb_id=r.ehb_id;


/* Final ehb_link table */
drop table if exists ehb_link;
create table ehb_link as

select
    id,
    ehb_id_md5 as ehb_id,
    ehb_id as ehb_id_int,
    external_system_id,
    external_id,
    organization_id,
    organization_subject_id,
    created,
    dob
from ehb_link_md5;

insert into etl_stats ( etl_datetime ) values ( now() );
