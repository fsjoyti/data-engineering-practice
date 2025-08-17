DROP TABLE IF EXISTS ELECTRIC_VEHICLE_POPULATION;

CREATE TABLE  IF NOT EXISTS ELECTRIC_VEHICLE_POPULATION(
    VIN VARCHAR,
    County VARCHAR,
    City VARCHAR,
    State VARCHAR,
    Postal_Code INTEGER,
    Model_Year INTEGER, 
    Make VARCHAR,
    Model VARCHAR,
    Electric_Vehicle_Type VARCHAR,
    Clean_Alternative_Fuel_Vehicle_Eligibility VARCHAR,
    Electric_Range INTEGER,
    Base_MSRP INTEGER,
    Legislative_District INTEGER,
    DOL_Vehicle_ID INTEGER,
    Vehicle_Location  GEOMETRY,
    Electric_Utility VARCHAR,
    TwentyTwenty_Census_Tract BIGINT
);