
-- drop table if exists public.tbldaily_ph_earthquake_data


create table public.tbldaily_ph_earthquake_data (
    date date,
    time time,
    geo_lat double precision,
    geo_long double precision,
    geo_point varchar,
    location varchar,
    depth_km double precision,
    magnitude double precision,
    info_no int,
    depth_of_focus_km int,
    origin varchar,
    expecting_damage varchar,
    expecting_aftershocks varchar,
    page_link varchar
)