clean_runner_order = """
CREATE TABLE IF NOT EXISTS pizza_runner.runner_order_clean AS 

    SELECT order_id, runner_id,
    CASE
        WHEN pickup_time LIKE 'null' THEN NULL
        ELSE pickup_time
        END AS pickup_time,
    CASE
        WHEN distance LIKE 'null' THEN NULL
        WHEN distance LIKE '%km' THEN TRIM('km' from distance)
        ELSE distance
        END AS distance,
    CASE
        WHEN duration LIKE 'null' THEN NULL
        WHEN duration LIKE '%mins' THEN TRIM('mins' from duration)
        WHEN duration LIKE '%minute' THEN TRIM('minute' from duration)        
        WHEN duration LIKE '%minutes' THEN TRIM ('minutes' from duration)     
        ELSE duration
        END AS duration,
    CASE
        WHEN cancellation IS NULL or cancellation LIKE 'null' THEN ''
        ELSE cancellation END AS cancellation
    FROM pizza_runner.runner_orders;
"""

alter_run_table = """
    ALTER TABLE pizza_runner.runner_order_clean
    ALTER COLUMN pickup_time TYPE TIMESTAMP USING pickup_time::TIMESTAMP,
    ALTER COLUMN duration TYPE INT USING duration::integer,
    ALTER COLUMN distance TYPE FLOAT USING distance::FLOAT;
"""

clean_customer_order_data = """
    CREATE TABLE IF NOT EXISTS pizza_runner.customer_order_cleans
    AS 
        SELECT order_id, customer_id, pizza_id, 
        CASE 
            WHEN exclusions IS null OR exclusions LIKE 'null' OR exclusions LIKE '' THEN ' '
            ELSE exclusions
            END AS exclusions,
        CASE 
            WHEN extras IS null OR extras LIKE 'null' OR extras LIKE '' THEN ' '
            ELSE extras 
            END AS extras, 
                order_time
        FROM pizza_runner.customer_orders;
"""



task_1 = """
    SELECT COUNT(*) AS Numbers_of_orders 
    FROM pizza_runner.customer_orders;
"""

task_2 = """
    SELECT COUNT(DISTINCT order_id) AS num_unique_orders
    FROM pizza_runner.customer_orders;
"""

task_3 = """
    SELECT runner_id, COUNT(cancellation) AS num_su 
        ccesful_orders
    FROM pizza_runner.runner_order_clean
    WHERE cancellation = ''
    GROUP BY runner_id;
"""

task_4 = """
    SELECT p.pizza_name, COUNT(c.pizza_id)
    FROM pizza_runner.customer_orders as c
    JOIN pizza_runner.runner_order_clean as r
    ON r.order_id = c.order_id
    JOIN pizza_runner.pizza_names as p
    ON p.pizza_id = c.pizza_id
    WHERE r.cancellation = ''
    GROUP BY p.pizza_name;
"""

task_5 = """
    SELECT c.customer_id, p.pizza_name, COUNT(p.piz 
 za_name)
    FROM pizza_runner.customer_orders as c
    JOIN pizza_runner.pizza_names as p
    ON p.pizza_id = c.pizza_id
    GROUP BY c.customer_id, p.pizza_name
    ORDER BY c.customer_id;
"""

task_6 = """
   SELECT c.order_id, COUNT(c.pizza_id) as pizza_per_order
    FROM pizza_runner.customer_orders as c
    JOIN pizza_runner.runner_orders as r
    ON r.order_id = c.order_id
    WHERE r.cancellation = '' OR r.cancellation is NULL OR r.cancellation  LIKE '%null'
    GROUP BY c.order_id
    ORDER BY pizza_per_order DESC
    LIMIT 5; 
"""

task_7 = """
    SELECT c.customer_id,
 SUM(CASE
     WHEN c.exclusions <> '' OR c.extras <> '' THEN 1
     ELSE 0
     END) AS change,
 SUM(CASE
     WHEN c.exclusions = '' OR c.extras <> '' THEN 1
     ELSE 0
     END) AS no_change
FROM pizza_runner.customer_order_cleans AS c
JOIN pizza_runner.runner_orders AS r
ON r.order_id = c.order_id
WHERE r.cancellation = '' OR r.cancellation is NULL OR r.cancellation  LIKE '%null'
GROUP BY c.customer_id
ORDER BY c.customer_id;
"""

task_8 = """
    select c.customer_id,
 (CASE
     WHEN exclusions IS NOT NULL AND extras IS NOT NULL
     THEN 1
     ELSE 0
     END) AS pizza_w_additions
   FROM pizza_runner.customer_order_cleans as c
   JOIN pizza_runner.runner_order_clean AS r
   ON c.order_id = r.order_id
   WHERE r.cancellation = '' AND
   c.exclusions <> ' '
   AND c.extras <> ' ';
"""

task_9 = """
    SELECT
        DATE_PART('hour', order_time) AS hour_of_day ,
        COUNT(order_id)
    FROM pizza_runner.customer_order_cleans
    GROUP BY DATE_PART('hour', order_time);
"""

task_10 = """
    SELECT DATE_PART('dow', order_time)+2 as day_of 
 _week, COUNT(order_id) total_volume
 FROM pizza_runner.customer_orders
 GROUP BY day_of_week
 ORDER BY day_of_week;
"""

"""
    Runner and Customer Experience
"""

Ex_1 = """
    SELECT TO_CHAR(registration_date, 'W') AS r 
 egistration_week, COUNT(runner_id) as SIGNUP
 FROM pizza_runner.runners
 GROUP BY registration_week
 ORDER BY registration_week;
"""

Ex_2 = """
    WITH runner_avg_time AS (SELECT  r.runner_id, c.order_time, r.pickup_time,
      EXTRACT (EPOCH from (r.pickup_time - c.order_time))/60 AS pickup_minutes
      FROM pizza_runner.customer_order_cleans as c
      JOIN pizza_runner.runner_order_clean as r
      ON r.order_id = c.order_id
      WHERE r.cancellation = ''
      GROUP BY r.runner_id, c.order_time, r.pickup_time)

    SELECT runner_id, AVG(runner_avg_time.pickup_minutes) AS avg_time
      FROM runner_avg_time
      GROUP BY runner_id;
"""

Ex_3 = """
    WITH avg_prep_time  AS (
     SELECT c.order_id, COUNT(c.order_id) as pizza_count,
     c.order_time, r.pickup_time, 
     EXTRACT(EPOCH FROM (r.pickup_time - c.order_time))/ 60 AS total_minute
     FROM pizza_runner.customer_order_cleans as c
     JOIN pizza_runner.runner_order_clean as r
     ON r.order_id = c.order_id
     GROUP BY c.order_id, c.order_time, r.pickup_time
 )
 SELECT pizza_count, AVG(avg_prep_time.total_minute) as avg_minute   
 FROM avg_prep_time
 GROUP BY pizza_count;
"""

EX_4 = """
    SELECT c.customer_id, AVG(r.distance) as  
 avg_distance
 FROM pizza_runner.customer_order_cleans AS c
 JOIN pizza_runner.runner_order_clean AS r
 ON r.order_id = c.order_id
 WHERE r.duration IS NOT NULL
 GROUP BY c.customer_id
 ORDER BY c.customer_id;
"""

Ex_5 = """
SELECT MAX(duration) - MIN(duration) as duration_difference
FROM pizza_runner.runner_order_clean
WHERE duration IS NOT NULL;
"""

Ex_6 = """
SELECT r.runner_id, c.customer_id, c.order_id,  
       COUNT(c.order_id) AS pizza_count, r.distance, (r.duration /60) AS duratio 
       n_hr, ROUND((r.distance/r.duration * 60)::numeric, 2) AS avg_speed        
 FROM pizza_runner.runner_order_clean as r
 JOIN pizza_runner.customer_order_cleans as c
 ON r.order_id = c.order_id
 WHERE r.duration IS NOT NULL
 GROUP BY r.runner_id, c.customer_id, c.order_id, r.distance, r.duration   
 ORDER BY c.order_id;
"""

tp_1 = """
  WITH toppings_cte AS (SELECT pizza_id, REGEXP_SPLIT_TO_TABLE(extras , '[,\s]+')::INTEGER AS topping_id
  FROM pizza_runner.customer_order_cleans
  WHERE extras != ' ')

  SELECT t.topping_id, pt.topping_name, COUNT(t.topping_id) as topping_count
  FROM toppings_cte as t
  INNER JOIN pizza_runner.pizza_toppings as pt
  ON pt.topping_id = t.topping_id
  GROUP BY t.topping_id, pt.topping_name
  ORDER BY topping_count DESC;
"""