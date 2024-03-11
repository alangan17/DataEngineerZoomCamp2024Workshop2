# Homework

## Setting up

In order to get a static set of results, we will use historical data from the dataset.

Run the following commands:
```bash
# Add env var for psql
export PATH=/Library/PostgreSQL/16/bin:$PATH

# Load the cluster op commands.
source commands.sh
# First, reset the cluster:
clean-cluster
# Start a new cluster
start-cluster
# wait for cluster to start
sleep 5
# Seed historical data instead of real-time data
seed-kafka
# Recreate trip data table
psql -f risingwave-sql/table/trip_data.sql
# Wait for a while for the trip_data table to be populated.
sleep 5
# Check that you have 100K records in the trip_data table
# You may rerun it if the count is not 100K
psql -c "SELECT COUNT(*) FROM trip_data"
```

## Question 0

_This question is just a warm-up to introduce dynamic filter, please attempt it before viewing its solution._

What are the dropoff taxi zones at the latest dropoff times?

For this part, we will use the [dynamic filter pattern](https://docs.risingwave.com/docs/current/sql-pattern-dynamic-filters/).

<details>
<summary>Solution</summary>

```sql
CREATE MATERIALIZED VIEW latest_dropoff_time AS
    WITH t AS (
        SELECT MAX(tpep_dropoff_datetime) AS latest_dropoff_time
        FROM trip_data
    )
    SELECT taxi_zone.Zone as taxi_zone, latest_dropoff_time
    FROM t,
            trip_data
    JOIN taxi_zone
        ON trip_data.DOLocationID = taxi_zone.location_id
    WHERE trip_data.tpep_dropoff_datetime = t.latest_dropoff_time;

--    taxi_zone    | latest_dropoff_time
-- ----------------+---------------------
--  Midtown Center | 2022-01-03 17:24:54
-- (1 row)
```

</details>

## Question 1

Create a materialized view to compute the average, min and max trip time **between each taxi zone**.

Note that we consider the do not consider `a->b` and `b->a` as the same trip pair.
So as an example, you would consider the following trip pairs as different pairs:
```plaintext
Yorkville East -> Steinway
Steinway -> Yorkville East
```

From this MV, find the pair of taxi zones with the highest average trip time.
You may need to use the [dynamic filter pattern](https://docs.risingwave.com/docs/current/sql-pattern-dynamic-filters/) for this.

Bonus (no marks): Create an MV which can identify anomalies in the data. For example, if the average trip time between two zones is 1 minute,
but the max trip time is 10 minutes and 20 minutes respectively.

Options:
1. -> Yorkville East, Steinway
2. Murray Hill, Midwood
3. East Flatbush/Farragut, East Harlem North
4. Midtown Center, University Heights/Morris Heights

p.s. The trip time between taxi zones does not take symmetricity into account, i.e. `A -> B` and `B -> A` are considered different trips. This applies to subsequent questions as well.

```sql
WITH base AS (
  SELECT
    p.zone as PU_zone
    , d.zone as DO_zone
    , t.tpep_dropoff_datetime - t.tpep_pickup_datetime as avg_trip_duration
  FROM
    trip_data t
  JOIN taxi_zone p ON 1=1
    AND t.puLocationID = p.location_id
  JOIN taxi_zone d ON 1=1
    AND t.dolocationid = d.location_id
  WHERE 1=1
    AND t.puLocationID != t.dolocationid
), avg_trip_duration AS (
  SELECT PU_zone, DO_zone, AVG(avg_trip_duration) as avg_trip_duration
  FROM base
  GROUP BY PU_zone, DO_zone
)
SELECT PU_zone, DO_zone, avg_trip_duration
FROM avg_trip_duration
ORDER BY avg_trip_duration DESC
;

/*
               pu_zone               |               do_zone               | avg_trip_duration 
-------------------------------------+-------------------------------------+-------------------
 Yorkville East                      | Steinway                            | 23:59:33
 Stuy Town/Peter Cooper Village      | Murray Hill-Queens                  | 23:58:44
 Washington Heights North            | Highbridge Park                     | 23:58:40
 Two Bridges/Seward Park             | Bushwick South                      | 23:58:14
 Clinton East                        | Prospect-Lefferts Gardens           | 23:53:56
 SoHo                                | South Williamsburg                  | 23:49:58
 Downtown Brooklyn/MetroTech         | Garment District                    | 23:41:43
 Lower East Side                     | Sunset Park West                    | 20:50:34
 West Village                        | Flatbush/Ditmas Park                | 12:03:18
 Chinatown                           | Lenox Hill West                     | 12:01:53.5
 East Elmhurst                       | Corona                              | 11:37:11
 JFK Airport                         | Broad Channel                       | 11:28:49
 Lower East Side                     | Mott Haven/Port Morris              | 11:20:21.5
 Upper East Side South               | NaN                                 | 08:34:44.666667
 Times Sq/Theatre District           | Dyker Heights                       | 08:24:32.333333
 Upper West Side South               | Battery Park                        | 08:15:45
 Manhattanville                      | Central Harlem                      | 08:05:08.333333
 Alphabet City                       | Two Bridges/Seward Park             | 08:03:11.333333
 Old Astoria                         | Astoria                             | 07:14:15.666667
 West Village                        | Elmhurst/Maspeth                    | 07:06:48
 Greenwich Village South             | Sunset Park West                    | 06:22:21.666667
 East Village                        | Woodside                            | 06:12:35.5
*/
```

## Question 2

Recreate the MV(s) in question 1, to also find the **number of trips** for the pair of taxi zones with the highest average trip time.

Options:
1. 5
2. 3
3. 10
4. -> 1

```sql
WITH base AS (
  SELECT
    p.zone as PU_zone
    , d.zone as DO_zone
    , t.tpep_dropoff_datetime - t.tpep_pickup_datetime as avg_trip_duration
  FROM
    trip_data t
  JOIN taxi_zone p ON 1=1
    AND t.puLocationID = p.location_id
  JOIN taxi_zone d ON 1=1
    AND t.dolocationid = d.location_id
  WHERE 1=1
    AND t.puLocationID != t.dolocationid
), summary AS (
  SELECT
    PU_zone
    , DO_zone
    , AVG(avg_trip_duration) as avg_trip_duration
    , COUNT(*) as num_trips
  FROM base
  GROUP BY PU_zone, DO_zone
)
SELECT
  PU_zone
  , DO_zone
  , avg_trip_duration
  , num_trips
FROM summary
ORDER BY avg_trip_duration DESC
;

/*
               pu_zone               |               do_zone               | avg_trip_duration | num_trips 
-------------------------------------+-------------------------------------+-------------------+-----------
 Yorkville East                      | Steinway                            | 23:59:33          |         1
 Stuy Town/Peter Cooper Village      | Murray Hill-Queens                  | 23:58:44          |         1
 Washington Heights North            | Highbridge Park                     | 23:58:40          |         1
 Two Bridges/Seward Park             | Bushwick South                      | 23:58:14          |         1
 Clinton East                        | Prospect-Lefferts Gardens           | 23:53:56          |         1
 SoHo                                | South Williamsburg                  | 23:49:58          |         1
 Downtown Brooklyn/MetroTech         | Garment District                    | 23:41:43          |         1
 Lower East Side                     | Sunset Park West                    | 20:50:34          |         1
 West Village                        | Flatbush/Ditmas Park                | 12:03:18          |         2
 Chinatown                           | Lenox Hill West                     | 12:01:53.5        |         2
 East Elmhurst                       | Corona                              | 11:37:11          |         2
 JFK Airport                         | Broad Channel                       | 11:28:49          |         2
 Lower East Side                     | Mott Haven/Port Morris              | 11:20:21.5        |         2
 Upper East Side South               | NaN                                 | 08:34:44.666667   |         3
 Times Sq/Theatre District           | Dyker Heights                       | 08:24:32.333333   |         3
 Upper West Side South               | Battery Park                        | 08:15:45          |         3
 Manhattanville                      | Central Harlem                      | 08:05:08.333333   |         3
 Alphabet City                       | Two Bridges/Seward Park             | 08:03:11.333333   |         3
 Old Astoria                         | Astoria                             | 07:14:15.666667   |         3
 West Village                        | Elmhurst/Maspeth                    | 07:06:48          |         1
 Greenwich Village South             | Sunset Park West                    | 06:22:21.666667   |         3
 East Village                        | Woodside                            | 06:12:35.5        |         4
*/
```


## Question 3

From the latest pickup time to 17 hours before, what are the top 3 busiest zones in terms of number of pickups?
For example if the latest pickup time is 2020-01-01 17:00:00,
then the query should return the top 3 busiest zones from 2020-01-01 00:00:00 to 2020-01-01 17:00:00.

HINT: You can use [dynamic filter pattern](https://docs.risingwave.com/docs/current/sql-pattern-dynamic-filters/)
to create a filter condition based on the latest pickup time.

NOTE: For this question `17 hours` was picked to ensure we have enough data to work with.

Options:
1. Clinton East, Upper East Side North, Penn Station
2. -> LaGuardia Airport, Lincoln Square East, JFK Airport
3. Midtown Center, Upper East Side South, Upper East Side North
4. LaGuardia Airport, Midtown Center, Upper East Side North

```sql
WITH base AS (
  SELECT 
    puLocationID
    , COUNT(*) as num_pickups
  FROM trip_data
  WHERE tpep_pickup_datetime > (SELECT MAX(tpep_pickup_datetime) - INTERVAL '17 hours' FROM trip_data)
  GROUP BY puLocationID
)
SELECT
  taxi_zone.Zone as taxi_zone
  , num_pickups
FROM base
JOIN taxi_zone
  ON base.puLocationID = taxi_zone.location_id
ORDER BY num_pickups DESC
;

/*
           taxi_zone            | num_pickups 
--------------------------------+-------------
 LaGuardia Airport              |          19
 Lincoln Square East            |          17
 JFK Airport                    |          17
 Penn Station/Madison Sq West   |          16
 Upper East Side North          |          13
 Times Sq/Theatre District      |          12
 East Chelsea                   |          11
 Upper East Side South          |          10
 Clinton East                   |           8
 Lenox Hill West                |           8
 Midtown South                  |           7
 Sutton Place/Turtle Bay North  |           7
 Upper West Side North          |           7
 Upper West Side South          |           6
 Murray Hill                    |           6
 Yorkville West                 |           5
 Midtown East                   |           5
 Greenwich Village South        |           5
 Gramercy                       |           5
 East Village                   |           5
 Midtown North                  |           5
 Yorkville East                 |           4
*/
```