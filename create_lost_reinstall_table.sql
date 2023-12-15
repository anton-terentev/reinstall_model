drop table if exists ma_data.terentev_reinstalls_lost;
create table ma_data.terentev_reinstalls_lost (
    app_short varchar(256),
    user_id varchar(256),
    install_date date,
    install_time timestamp,
    country_code varchar(2),
    media_source varchar(256),
    last_active timestamp);

insert into ma_data.terentev_reinstalls_lost (
with reinstalls as (
    select
        app_short,
        customer_user_id,
        country_code,
        install_time::DATETIME,
        media_source
    from
        plr.af_raw_data.reinstalls u
    where
        month between '2021-01-01' and '2023-10-01'
        and date(event_time) between '2021-01-01' and '2023-10-31'
        and app_short in ('gs_as', 'gs_gp', 'hs_as', 'hs_gp', 'fd_as', 'fd_gp', 'ts_as', 'ts_gp')
    union all
    select
        app_short,
        customer_user_id,
        country_code,
        install_time::DATETIME,
        media_source
    from
        plr.af_raw_data.organic_reinstalls u
    where
        month between '2021-01-01' and '2023-10-01'
        and date(event_time) between '2021-01-01' and '2023-10-31'
        and app_short in ('gs_as', 'gs_gp', 'hs_as', 'hs_gp', 'fd_as', 'fd_gp', 'ts_as', 'ts_gp')
    union all
    select
        app_short,
        customer_user_id,
        country_code,
        event_time::DATETIME as install_time,
        media_source
    from
        plr.af_raw_data.conversions_retargeting
    where
        month between '2021-01-01' and '2023-10-01'
        and date(event_time) between '2021-01-01' and '2023-10-31'
        and app_short in ('gs_as', 'gs_gp', 'hs_as', 'hs_gp', 'fd_as', 'fd_gp', 'ts_as', 'ts_gp')
        and retargeting_conversion_type = 'reattribution'
        and event_name = 'install'),

reinstalls_unique as (
    select
        *
    from (
    select
       app_short,
       customer_user_id,
       install_time::date as install_date,
       install_time,
       country_code,
       media_source,
       row_number() over (partition by customer_user_id, install_date order by install_time) temp_interday_reinstall
    from
        reinstalls) as t
    where
        temp_interday_reinstall = 1
)

select
    r.app_short,
    r.customer_user_id as user_id,
    r.install_date,
    r.install_time,
    r.country_code,
    r.media_source,
    date_add('days', -181, r.install_date) as last_active -- pseudo last active
from
    reinstalls_unique r
join
    plr.public.users_ids i on (r.customer_user_id = i.id)
left join
    ma_data.terentev_reinstalls_last_active l on (r.customer_user_id = l.user_id and r.app_short = l.app_short)
left join
    plr.public.users_all u on (u.swrve_id = r.customer_user_id)
left join
    plr.public.users_all uu on (uu.plr_id = i.plr_id)
where
    l.user_id is null
    and u.swrve_id is null
    and uu.plr_id is null);