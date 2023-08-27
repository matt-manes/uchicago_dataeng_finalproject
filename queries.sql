-- For the sqlite database
-- Number of food inspections by ward
SELECT
    COUNT(inspections.id) AS inspection_count,
    business_addresses.ward
FROM
    inspections
    INNER JOIN licenses ON inspections.license_number = licenses.license_number
    INNER JOIN businesses ON licenses.account_number = businesses.account_number
    INNER JOIN business_addresses ON businesses.address_id = business_addresses.id
GROUP BY
    business_addresses.ward
ORDER BY
    inspection_count DESC;

-- Number of failed food inspections by ward
SELECT
    COUNT(inspections.id) AS inspection_count,
    business_addresses.ward
FROM
    inspections
    INNER JOIN licenses ON inspections.license_number = licenses.license_number
    INNER JOIN businesses ON licenses.account_number = businesses.account_number
    INNER JOIN business_addresses ON businesses.address_id = business_addresses.id
WHERE
    inspections.result_type_id = 2
GROUP BY
    business_addresses.ward
ORDER BY
    inspection_count DESC;