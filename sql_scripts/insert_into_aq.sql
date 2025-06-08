-- Update existing row (category_aq_id = 13) with a new description
UPDATE [geo_climate].[dbo].[air_quality_categories]
SET 
    category_name = 'Good',
    min_value = 0,
    max_value = 20,
    unit_of_measure = 'µg/m³',
    pollutant = 'PM10',
    category_description = 'Air quality is satisfactory.'
WHERE category_aq_id = 13;


-- Insert next row (category_aq_id = 20)
INSERT INTO [geo_climate].[dbo].[air_quality_categories] 
(category_aq_id, category_name, min_value, max_value, unit_of_measure, pollutant, category_description)
VALUES
(20, 'Moderate', 4401, 9400, 'µg/m³', 'Carbon Monoxide', 'Air quality is acceptable.');

DELETE FROM [geo_climate].[dbo].[air_quality_categories]
WHERE category_aq_id = 21;
