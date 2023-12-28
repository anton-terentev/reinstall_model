DROP table if exists ma_data.terenev_temp_table;
CREATE TABLE ma_data.terenev_temp_table (
    app_short varchar(256),
    user_id varchar(256),
    install_date date,
    install_time timestamp,
    country_code varchar(2),
    media_source varchar(256),
    last_active timestamp,
    switch_date timestamp
)
DISTKEY (user_id)
SORTKEY (app_short, user_id);

insert into ma_data.terenev_temp_table
SELECT distinct
    app_short,
    user_id,
    install_date,
    install_time,
    country_code,
    media_source,
    last_active,
    nvl(lead(install_time) over (partition by user_id, app_short order by install_time), current_date) as switch_date
FROM ma_data.terentev_reengagement_fb_gp;

-- Удалите исходную таблицу
DROP TABLE ma_data.terentev_reengagement_fb_gp;

-- Переименуйте новую таблицу в имя исходной таблицы
ALTER TABLE ma_data.terenev_temp_table RENAME TO terentev_reengagement_fb_gp;

vacuum ma_data.terentev_reengagement_fb_gp;
