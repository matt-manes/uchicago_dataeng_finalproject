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

-- Business owner legal names where they own(ed) more than one business
SELECT
    legal_name,
    dba,
    COUNT(legal_name) AS num_entities
FROM
    business_licenses
WHERE
    application_type = 'ISSUE'
GROUP BY
    legal_name
HAVING
    num_entities > 1;