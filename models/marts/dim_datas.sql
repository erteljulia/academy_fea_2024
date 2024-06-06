with recursive date_series as (
    select 
        cast('2011-01-01' as date) as date_value
    union all
    select dateadd(day, 1, date_value)
    from date_series
    where date_value < '2014-12-31'
    )
    select
        date_value as date
        , extract(year from date_value) as year
        , extract(month from date_value) as month
        , extract(day from date_value) as day
        , extract(dayofweek from date_value) as day_of_week
        , case 
            when extract (dayofweek from date_value) in (1, 7) then 'Weekend'
            else 'Weekday'
        end as weekend_weekday
        , to_char(date_value, 'YYYY-MM-DD') as iso_date
    from date_series
