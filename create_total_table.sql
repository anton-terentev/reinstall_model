drop table if exists ma_data.terenev_reinstalls_total;
CREATE TABLE ma_data.terenev_reinstalls_total (
    app_short varchar(256),
    user_id varchar(256),
    install_date date,
    install_time timestamp,
    country_code varchar(2),
    media_source varchar(256),
    last_active timestamp,
    switch_date timestamp,
    reinstall_type varchar (256)
)
DISTKEY (user_id)
SORTKEY (app_short, user_id);

insert into ma_data.terenev_reinstalls_total (
select
    app_short,
    user_id,
    install_date,
    install_time,
    country_code,
    media_source,
    last_active,
    switch_date,
    reinstall_type
from (
select distinct
    *,
    nvl(lead(install_time) over (partition by user_id, app_short order by install_time), current_date) as switch_date,
    row_number() over (partition by app_short, user_id, install_date order by install_time) num
from(
select
    app_short,
    user_id,
    install_date,
    install_time,
    country_code,
    media_source,
    last_active,
    'normal' as reinstall_type
from
    ma_data.terentev_reinstalls_last_active
union all
select
    *,
    'hidden' as reinstall_type
from
    ma_data.magazov_hidden_reinstalls_valid
union all
select
    *,
    'lost' as reinstall_type
from
    ma_data.terentev_reinstalls_lost) as x) as y
where
    num = 1);




