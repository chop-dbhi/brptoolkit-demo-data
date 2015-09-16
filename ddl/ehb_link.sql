drop table if exists ehb_link;

create table ehb_link (
    ID SERIAL,
    ehb_id int NOT NULL,
    external_system_id int NOT NULL,
    external_id varchar(30),
    organization_id varchar(30),
    organization_subject_id varchar(30),
    dob date,
    created timestamp
);

CREATE TABLE if not exists etl_stats
(
  etl_datetime timestamp without time zone
);
