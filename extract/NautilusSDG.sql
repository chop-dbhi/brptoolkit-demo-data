/*
    Description:
        Retrieve the staging table of Nautilus SDGs for upntb
    Target:
        Oracle
    Author:
        Alex Felmeister <felmeistera@email.chop.edu>
*/

select
    sdg.sdg_id as "sdg_id",
    sdg.name as "sample_subject_id",
    sdg.external_reference as "external_reference",
    usdg.u_collection_site as "collection_site",
    case
        when sdg.external_reference = '0'
        then
            sdg.name
        else
            lpad(sdg.external_reference, 16)
    end as "potential_universal_id"
from
    sdg sdg,
    sdg_user usdg
where sdg.sdg_id = usdg.sdg_id and
    usdg.u_sd_study_name = '7316' and
    sdg.status != 'X' and
    sdg.status != 'U' and
    sdg.status != 'S' and
    sdg.external_reference != '0' and
    sdg.external_reference != '00'
