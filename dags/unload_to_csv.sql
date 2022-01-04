COPY (
    SELECT *
    FROM pizza_runner.customer_order_cleans
    
)TO '{{ params.customer_order }}' WITH (FORMAT CSV, HEADER);

