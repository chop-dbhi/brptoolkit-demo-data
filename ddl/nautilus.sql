DROP TABLE IF EXISTS nautilus_sdg_staging;
DROP TABLE IF EXISTS nautilus_visit_staging;
DROP TABLE IF EXISTS nautilus_aliquot_staging;


CREATE TABLE nautilus_sdg_staging
(
  sdg_id numeric(16,0),
  sample_subject_id character varying(255),
  external_reference character varying(255),
  collection_site character varying(30),
  potential_universal_id character varying(255)
);

CREATE TABLE nautilus_visit_staging
(
  visit_id numeric(16,0),
  sample_subject_id character varying(255),
  sdg_id numeric(16,0),
  received_on date,
  sd_group_name character varying(2000),
  visit_name character varying(2000),
  visit_description character varying(4000),
  visit_time_date date
);

CREATE TABLE nautilus_aliquot_staging
(
  sdg_id numeric(16,0),
  sample_subject_name character varying(255),
  collection_site character varying(30),
  potential_universal_id character varying(255),
  visit_name character varying(2000),
  aliquot_id numeric(16,0),
  aliquot_name character varying(255),
  parent_aliquot_id numeric(16,0),
  visit_id numeric(16,0),
  received_on timestamp with time zone,
  sample_type_code character varying(2000),
  secondary_sample_code character varying(2000),
  sample_type character varying(4000),
  secondary_sample_type character varying(4000),
  full_sample_type_desc character varying(4000),
  collection_event_name character varying(2000),
  draw_note character varying(2000),
  tissue_type character varying(30),
  specimen_category character varying(2000),
  collect_method character varying(2000),
  received_date_time timestamp with time zone,
  volume_received double precision,
  volume_remaining double precision,
  vol_units character varying(255),
  concentration double precision,
  conc_units character varying(30),
  unit_id numeric(16,0),
  disposed_flag character(1),
  available_flag character(1),
  disposed character(1),
  location_id numeric(16,0),
  collect_date_time timestamp with time zone,
  primary key (aliquot_name)
);

/* Create the cBio table if its not available as genomic linking to the sample relies on it */
create table if not exists cbio_sample (
    internal_id integer,
    stable_id text,
    sdg_id text,
    sample_type text,
    patient_id integer,
    type_of_cancer text,
    cancer_study_identifier text
);
