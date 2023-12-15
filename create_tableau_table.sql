drop table ma_data.terentev_reinstalls_model;
create table ma_data.terentev_reinstalls_model (
        app_short varchar(256),
        install_month date,
        first_install_month date,
        country_code varchar(8),
        media_source varchar(256),
        install_type varchar(256),
        reinstall_type varchar(256),
        installs integer,
        payers_7 integer,
        payers_30 integer,
        payers_90 integer,
        payers_180 integer,
        payers_365 integer,
        payers_all integer,
        revenue_7 float,
        revenue_30 float,
        revenue_90 float,
        revenue_180 float,
        revenue_365 float,
        revenue_all float,
        payers_90_minus integer,
        payers_180_minus integer,
        payers_365_minus integer,
        payers_all_minus integer,
        revenue_90_after_r_minus float,
        revenue_180_after_r_minus float,
        revenue_365_after_r_minus float,
        revenue_all_after_r_minus float
)

insert into ma_data.terentev_reinstalls_model
with installs as (
    select
        app_short,
        swrve_id,
        install_date,
        country_code,
        media_source,
        install_type,
        reinstall_type,
        revenue_7,
        revenue_30,
        revenue_90,
        revenue_180,
        revenue_365,
        revenue_all
    from (
    select
        u.app_short,
        u.swrve_id,
        u.install_datetime::date as install_date,
        u.country as country_code,
        p.media_source as media_source,
        'install' as install_type,
        'install' as reinstall_type,
        row_number() over (partition by u.swrve_id, u.app_short order by u.install_datetime) num,
        sum(case when datediff(days, u.install_datetime::timestamp, pp.created::timestamp) < 7  then revenue_usd else null end) revenue_7,
        sum(case when datediff(days, u.install_datetime::timestamp, pp.created::timestamp) < 30 then revenue_usd else null end) revenue_30,
        sum(case when datediff(days, u.install_datetime::timestamp, pp.created::timestamp) < 90 then revenue_usd else null end) revenue_90,
        sum(case when datediff(days, u.install_datetime::timestamp, pp.created::timestamp) < 180 then revenue_usd else null end) revenue_180,
        sum(case when datediff(days, u.install_datetime::timestamp, pp.created::timestamp) < 365 then revenue_usd else null end) revenue_365,
        sum(revenue_usd) revenue_all
    from
        plr.public.users_all u
    join
        plr.public.partners p on (u.partner_id = p.partner_id)
    left join
        plr.public.purchases_all pp on (u.app_short = pp.app_short and u.plr_id = pp.plr_id)
    where
        u.app_short in ('gs_as', 'gs_gp', 'hs_as', 'hs_gp', 'fd_as', 'fd_gp', 'ts_as', 'ts_gp')
        and install_datetime::date between '2021-01-01' and '2023-10-31'
    group by
        1, 2, 3, 4, 5, 6, 7, u.install_datetime) as x
    where
        num = 1
),

first_install as (
    select
        *
    from (
    select
        u.app_short,
        u.swrve_id,
        u.plr_id,
        u.install_datetime,
        p.media_source,
        row_number() over (partition by u.app_short, u.swrve_id order by u.install_datetime) num
    from
        plr.public.users_all u
    join
        plr.public.partners p on (u.partner_id = p.partner_id)
    where
        u.app_short in ('gs_as', 'gs_gp', 'hs_as', 'hs_gp','fd_as', 'fd_gp', 'ts_as', 'ts_gp')) as x
    where
        num = 1
),

reinstall as (
    select
        r.app_short,
        r.user_id,
        u.install_datetime::date as install_date,
        r.install_date as r_install_date,
        r.country_code as r_country,
        case
           when r.media_source = 'restricted' then 'Facebook Ads' else r.media_source
        end as r_media_source,
        u.media_source as media_source,
        case
            when datediff(days, last_active, r.install_time) between 30 and 59 then '30-59'
            when datediff(days, last_active, r.install_time) between 60 and 89 then '60-89'
            when datediff(days, last_active, r.install_time) between 90 and 179 then '90-179'
            when datediff(days, last_active, r.install_time) >= 180 then '180+'
        end as install_type,
        reinstall_type,
        switch_date,
        row_number() over (partition by r.app_short, r.user_id order by r_install_date) as num,
        sum(case when datediff(days, r.install_time, pp.created::timestamp) between 0 and 7 and pp.created::timestamp < switch_date then revenue_usd else null end) revenue_7_after_r,
        sum(case when datediff(days, r.install_time, pp.created::timestamp) between 0 and 30 and pp.created::timestamp < switch_date then revenue_usd else null end) revenue_30_after_r,
        sum(case when datediff(days, r.install_time, pp.created::timestamp) between 0 and 90 and pp.created::timestamp < switch_date then revenue_usd else null end) revenue_90_after_r,
        sum(case when datediff(days, r.install_time, pp.created::timestamp) between 0 and 180 and pp.created::timestamp < switch_date then revenue_usd else null end) revenue_180_after_r,
        sum(case when datediff(days, r.install_time, pp.created::timestamp) between 0 and 365 and pp.created::timestamp < switch_date then revenue_usd else null end) revenue_365_after_r,
        sum(case when pp.created::timestamp between r.install_time and switch_date then revenue_usd else null end) revenue_all_after_r
    from
        ma_data.terenev_reinstalls_total r
    join
        plr.public.users_ids i on (r.user_id = i.id and r.app_short = i.app)
    left join
        first_install u on (r.app_short = u.app_short and r.user_id = u.swrve_id)
    left join
        plr.public.purchases_all pp on (nvl(u.app_short, i.app) = pp.app_short and nvl(u.plr_id, i.plr_id) = pp.plr_id)
    where
        (datediff(days, last_active, r.install_time) >= 30 and reinstall_type in ('hidden', 'normal') and u.install_datetime <= r.install_time)
       or (reinstall_type = 'lost' and u.swrve_id is null)
    group by
        1, 2, 3, 4, 5, 6, 7, 8, 9, 10
),

total as (select distinct
    u.*,
    date_trunc('month', u.install_date)::date as first_install_month,
    r.install_date as switch_date,
    revenue_90_after_r as revenue_90_after_r_minus,
    revenue_180_after_r as revenue_180_after_r_minus,
    revenue_365_after_r as revenue_365_after_r_minus,
    revenue_all_after_r as revenue_all_after_r_minus
from
    installs u
left join
    reinstall r on (r.app_short = u.app_short and r.user_id = u.swrve_id and r.num = 1)
union all
select distinct
    app_short,
    user_id,
    r_install_date,
    r_country,
    r_media_source,
    install_type,
    reinstall_type,
    revenue_7_after_r,
    revenue_30_after_r,
    revenue_90_after_r,
    revenue_180_after_r,
    revenue_365_after_r,
    revenue_all_after_r,
    date_trunc('month', install_date)::date as install_month,
    switch_date,
    0 as revenue_90_after_r_minus,
    0 as revenue_180_after_r_minus,
    0 as revenue_365_after_r_minus,
    0 as revenue_all_after_r_minus
from
    reinstall r)

select
    app_short,
    date_trunc('month', install_date) install_month,
    first_install_month,
    case
        when country_code in ('US', 'JP', 'UK', 'DE', 'KR', 'CA', 'AU', 'FR', 'CN', 'IT', 'BR') then country_code
        else 'Other'
    end as country_code,
    case
        when media_source in ('googleadwords_int', 'vungle_int', 'applovin_int', 'unityads_int', 'ironsource_int',
                              'Facebook Ads', 'liftoff_int', 'aura_int', 'moloco_int', 'digitalturbine_int', 'mistplay_int', 'organic') then media_source
        else 'Other'
    end as media_source,
    install_type,
    reinstall_type,
    count(distinct case when reinstall_type = 'hidden' then null else swrve_id end) as installs,
    count(distinct case when revenue_7 > 0 then swrve_id else null end) as payers_7,
    count(distinct case when revenue_30 > 0 then swrve_id else null end) as payers_30,
    count(distinct case when revenue_90 > 0 then swrve_id else null end) as payers_90,
    count(distinct case when revenue_180 > 0 then swrve_id else null end) as payers_180,
    count(distinct case when revenue_365 > 0 then swrve_id else null end) as payers_365,
    count(distinct case when revenue_all > 0 then swrve_id else null end) as payers_all,
    SUM(COALESCE(revenue_7, 0)) AS revenue_7,
    SUM(COALESCE(revenue_30, 0)) AS revenue_30,
    SUM(COALESCE(revenue_90, 0)) AS revenue_90,
    SUM(COALESCE(revenue_180, 0)) AS revenue_180,
    SUM(COALESCE(revenue_365, 0)) AS revenue_365,
    SUM(COALESCE(revenue_all, 0)) AS revenue_all,
    count(distinct case when revenue_90_after_r_minus > 0 then swrve_id else null end) as payers_90_minus,
    count(distinct case when revenue_180_after_r_minus > 0 then swrve_id else null end) as payers_180_minus,
    count(distinct case when revenue_365_after_r_minus > 0 then swrve_id else null end) as payers_365_minus,
    count(distinct case when revenue_all_after_r_minus > 0 then swrve_id else null end) as payers_all_minus,
    SUM(COALESCE(revenue_90_after_r_minus, 0)) AS revenue_90_after_r_minus,
    SUM(COALESCE(revenue_180_after_r_minus, 0)) AS revenue_180_after_r_minus,
    SUM(COALESCE(revenue_365_after_r_minus, 0)) AS revenue_365_after_r_minus,
    SUM(COALESCE(revenue_all_after_r_minus, 0)) AS revenue_all_after_r_minus
from
    total
group by
    1, 2, 3, 4, 5, 6, 7;