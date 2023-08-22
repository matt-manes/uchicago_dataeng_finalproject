-- For the sqlite database
-- Number of food inspections by ward
SELECT
    COUNT(DISTINCT inspection_id) AS num_inspections,
    business_licenses.ward
FROM
    food_inspections
    INNER JOIN business_licenses ON food_inspections.license_number = business_licenses.license_number
GROUP BY
    business_licenses.ward
ORDER BY
    num_inspections DESC;