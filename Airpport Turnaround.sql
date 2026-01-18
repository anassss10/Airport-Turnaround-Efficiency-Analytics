use flightsdb;

SELECT
    operation_id,
    flight_id,
    operation_type,
    team,
    expected_duration_min,
    (TIME_TO_SEC(operation_end) - TIME_TO_SEC(operation_start)) / 60
        AS actual_duration_min
FROM ground_operations;



CREATE OR REPLACE VIEW vw_ground_operations_clean AS
SELECT
    operation_id,
    flight_id,
    operation_type,
    team,
    expected_duration_min,
    actual_duration_min,
    CASE
        WHEN actual_duration_min > expected_duration_min
        THEN 1 ELSE 0
    END AS delay_flag
FROM (
    SELECT
        operation_id,
        flight_id,
        operation_type,
        team,
        expected_duration_min,
        (TIME_TO_SEC(operation_end) - TIME_TO_SEC(operation_start)) / 60
            AS actual_duration_min
    FROM ground_operations
) t;


SELECT *
FROM vw_ground_operations_clean
LIMIT 10;



SELECT
    team,
    COUNT(*) AS total_operations,
    SUM(delay_flag) AS delayed_operations,
    ROUND(SUM(delay_flag) * 100.0 / COUNT(*), 2) AS delay_percentage
FROM vw_ground_operations_clean
GROUP BY team;



SELECT
    operation_type,
    team,
    COUNT(*) AS total_ops,
    SUM(delay_flag) AS delayed_ops,
    ROUND(SUM(delay_flag) * 100.0 / COUNT(*), 2) AS delay_pct
FROM vw_ground_operations_clean
GROUP BY operation_type, team
ORDER BY delay_pct DESC;



SELECT
    f.flight_id,
    f.terminal,
    f.delay_minutes,
    SUM(v.delay_flag) AS delayed_operations
FROM flightsss f
JOIN vw_ground_operations_clean v
    ON f.flight_id = v.flight_id
GROUP BY f.flight_id, f.terminal, f.delay_minutes
ORDER BY delayed_operations DESC;



SELECT
    ROUND(
        SUM(CASE WHEN delay_minutes = 0 THEN 1 ELSE 0 END)
        * 100.0 / COUNT(*),
        2
    ) AS on_time_departure_pct
FROM flightsss;

SELECT
    s.team,
    s.staff_count,
    o.total_operations,
    ROUND(
        o.total_operations / s.staff_count,
        2
    ) AS operations_per_staff
FROM (
    SELECT
        team,
        COUNT(DISTINCT staff_id) AS staff_count
    FROM staff_rosterr
    GROUP BY team
) s
LEFT JOIN (
    SELECT
        team,
        COUNT(operation_id) AS total_operations
    FROM vw_ground_operations_clean
    GROUP BY team
) o
    ON s.team = o.team;





