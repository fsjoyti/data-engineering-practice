SELECT "VIN", "County", "City", "State", "Postal_Code", "Model_Year", "Make", "Model", "Electric_Vehicle_Type", "Clean_Alternative_Fuel_Vehicle_Eligibility", "Electric_Range", "Base_MSRP", "Legislative_District", "DOL_Vehicle_ID", "Vehicle_Location", "Electric_Utility", "TwentyTwenty_Census_Tract" FROM main."ELECTRIC_VEHICLE_POPULATION";

SELECT City, COUNT(*) FROM main."ELECTRIC_VEHICLE_POPULATION" GROUP BY City order by COUNT(*) desc;

with make_model_count as 
(
    SELECT Make, Model, COUNT(*) FROM main."ELECTRIC_VEHICLE_POPULATION" GROUP BY "Make", "Model" order by COUNT(*) desc
)
select Make, Model from make_model_count LIMIT 3;


SELECT "Postal_Code", Make, Model, COUNT(*) 
FROM main."ELECTRIC_VEHICLE_POPULATION" 
GROUP BY "Postal_Code" , "Make", "Model"
QUALIFY ROW_NUMBER() OVER (PARTITION BY "Postal_Code" ORDER BY COUNT(*) DESC) = 1;

SELECT Make, Model, "Model_Year", COUNT(*) FROM main."ELECTRIC_VEHICLE_POPULATION" GROUP BY "Make", "Model", "Model_Year" order by COUNT(*) desc;

