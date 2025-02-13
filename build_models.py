from dependencies import start_db, close_db, execute_query

# start analysis
conn = start_db()

# analysis goes here
# harlequin "/Users/willdecesare/Documents/GitHub/subway-safe/subway.db"
query = """

create or replace table analytics.fct_felonies as (
    select
        make_date(
            extract('year' from Month),
            extract('day' from Month),
            extract('month' from Month)
        ) as date_month
    , 'Subway' as mode
    , sum("Felony Count") as felonies
from "raw"."MTAMajorFelonies_20250210"
where Month >= '2023-01-01'
    and Month < '2025-01-01'
    and Agency = 'NYCT'
group by 1,2
order by 1,2
);

create or replace table analytics.fct_ridership as
(
    with base as (
        SELECT 
            date_trunc('Month', Date) as date_month
            , Mode as mode
            , sum(Count) as trips
        FROM "raw"."MTADailyRidership_20250210"
        where Date >= '2023-01-01'
            and Date < '2025-01-01'
            and Mode in ('Subway', 'Bus')
        group by 1,2
    )

    , base_aggregated as (
        select
            date_month
            , 'Bus + Subway total' as mode
            , sum(trips) as trips
        from base
        group by 1,2
    )

    , base_aggregated_yearly_sum as (
        select
            extract(year from date_month) as date_year
            , sum(trips) as trips_year
        from base_aggregated
        group by 1
    )

    , base_aggregated_distribution as (
        select
            a.date_month
            , b.trips_year
            , (a.trips / b.trips_year) as monthly_distribution
            , (2*(1/12) - (a.trips / b.trips_year)) as inverse_monthly_distribution
        from base_aggregated a
        left join base_aggregated_yearly_sum b on extract(year from a.date_month) = b.date_year
    )

    , new_modes as (
        SELECT 
            date_month
            , unnest(['Pedestrian', 'Car', 'Bike']) as mode
            , 0 as trips
        FROM base_aggregated
    )

    , update_ridership as (
        select
            new_modes.date_month
            , new_modes.mode
            , case 
                when new_modes.mode = 'Pedestrian' then b.trips_year * (45/17) * b.inverse_monthly_distribution
                when new_modes.mode = 'Car' then b.trips_year * (34/17) * b.monthly_distribution
                when new_modes.mode = 'Bike' then b.trips_year * (3/17) * b.inverse_monthly_distribution
                else null 
            end as riders
        from new_modes
        left join base_aggregated_distribution b on new_modes.date_month = b.date_month
    )

    , unioned as (
        select * from base
        UNION ALL
        select * from update_ridership
    )

    select * from unioned
    order by 1,2

);

create or replace table analytics.fct_collisions as (

with nyct_collisions as (
    select
        make_date(
            extract('year' from Month),
            extract('day' from Month),
            extract('month' from Month)
        ) as date_month
        , case
            when Metric = 'Bus Customer Accidents per million customers' then 'Bus'
            when Metric = 'Subway Customer Accidents' then 'Subway'
            else null
        end as mode
        , Value as injuries
    from raw.MTANYCTSafetyData_20250211
    where Metric in ('Bus Customer Accidents per million customers', 'Subway Customer Accidents') -- both metrics are actually normalized to per million
        and Month >= '2023-01-01'
        and Month < '2025-01-01'
)

, above_ground_collisions as (
    select
        date_trunc('Month', CRASH_DATE) as date_month
        , case
            when PERSON_TYPE = 'Occupant' then 'Car'
            when PERSON_TYPE = 'Bicyclist' then 'Bike'
            else PERSON_TYPE 
        end as mode
        , count(unique_id) as injuries
    from "raw"."Collisions_20250205" 
    where CRASH_DATE >= '2023-01-01' 
        and CRASH_DATE < '2025-01-01' 
        and PERSON_INJURY in ('Injured', 'Killed')
        and PERSON_TYPE in ('Occupant', 'Bicyclist', 'Pedestrian')
    group by 1,2
)

, ridership as (

    select
        date_month
        , mode
        , trips
    from analytics.fct_ridership
    where mode in ('Bus', 'Subway')

)

, unioned as (

    select * from nyct_collisions
    union all
    select * from above_ground_collisions

)

-- updating Bus and Subway injury metric from "per million customers" to "total injuries"
, nyct_adjusted as (

    select
        a.date_month
        , a.mode
        , case
            when a.mode in ('Bus', 'Subway') then (a.injuries / 1000000) * b.trips
            else a.injuries
        end as injuries
    from unioned a
    left join ridership b on a.date_month = b.date_month and a.mode = b.mode

)

select * from nyct_adjusted
order by 1,2

);

create or replace table analytics.fct_safety_incidents as (

    with base as (
        select
            a.date_month
            , a.mode
            , round(a.trips, 0) as trips
            , round(coalesce(b.injuries, 0), 0) as injuries
            , round(coalesce(c.felonies, 0), 0) as felonies
            , round(coalesce(b.injuries, 0) + coalesce(c.felonies, 0), 0) as safety_incidents
        from analytics.fct_ridership a
        left join analytics.fct_collisions b on a.date_month = b.date_month and a.mode = b.mode
        left join analytics.fct_felonies c on a.date_month = c.date_month and a.mode = c.mode
    )

    select
        base.*
        , round(base.safety_incidents * 1000000 / base.trips, 2) as safety_incidents_per_million_trips
    from base
    order by 1,2

);

"""

execute_query(conn, query)

# stop analysis
close_db(conn)
