

select "Vehicle Location" FROM 'Data/Electric_Vehicle_Population_Data.csv'
--ST_Point2D
INSTALL SPATIAL;
LOAD SPATIAL;
select ST_GeomFromText(coalesce("Vehicle Location", 'Point (0 0)')) as Vehicle_Location FROM 'Data/Electric_Vehicle_Population_Data.csv';
SELECT DISTINCT ST_GeometryType(ST_GeomFromText(coalesce("Vehicle Location", 'Point (0 0)')))  FROM 'Data/Electric_Vehicle_Population_Data.csv';

CREATE TABLE IF NOT EXISTS ELECTRIC_VEHICLE_POPULATION_TEST(
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

drop table ELECTRIC_VEHICLE_POPULATION_TEST;

INSERT INTO ELECTRIC_VEHICLE_POPULATION(
    VIN ,
    County,
    City ,
    State,
    Postal_Code,
    Model_Year, 
    Make ,
    Model,
    Electric_Vehicle_Type ,
    Clean_Alternative_Fuel_Vehicle_Eligibility,
    Electric_Range,
    Base_MSRP,
    Legislative_District,
    DOL_Vehicle_ID,
    Vehicle_Location,
    Electric_Utility,
    TwentyTwenty_Census_Tract
) SELECT "VIN (1-10)" as VIN,County,City,State,"Postal Code" as Postal_Code,"Model Year" as Model_Year,Make,Model,"Electric Vehicle Type" as Electric_Vehicle_Type,"Clean Alternative Fuel Vehicle (CAFV) Eligibility" as Clean_Alternative_Fuel_Vehicle_Eligibility,"Electric Range" as Electric_Range,"Base MSRP" as Base_MSRP,"Legislative District" as Legislative_District,"DOL Vehicle ID" as DOL_Vehicle_ID, ST_GeomFromText(coalesce("Vehicle Location", 'Point (0 0)')) as "Vehicle_Location","Electric Utility" as "Electric_Utility","2020 Census Tract" as "TwentyTwenty_Census_Tract" FROM 'Data/Electric_Vehicle_Population_Data.csv';

COPY TO ELECTRIC_VEHICLE_POPULATION (SELECT "VIN (1-10)" as VIN,County,City,State,"Postal Code" as Postal_Code,"Model Year" as Model_Year,Make,Model,"Electric Vehicle Type" as Electric_Vehicle_Type,"Clean Alternative Fuel Vehicle (CAFV) Eligibility" as Clean_Alternative_Fuel_Vehicle_Eligibility,"Electric Range" as Electric_Range,"Base MSRP" as Base_MSRP,"Legislative District" as Legislative_District,"DOL Vehicle ID" as DOL_Vehicle_ID, ST_GeomFromText(coalesce("Vehicle Location", 'Point (0 0)')) as "Vehicle_Location","Electric Utility" as "Electric_Utility","2020 Census Tract" as "TwentyTwenty_Census_Tract" FROM 'Data/Electric_Vehicle_Population_Data.csv') ;

select DISTINCT ST_GeometryType("Vehicle_Location") from ELECTRIC_VEHICLE_POPULATION;

CREATE TABLE ELECTRIC_VEHICLE_POPULATION_TEST AS SELECT "VIN (1-10)" as VIN,County,City,State,"Postal Code","Model Year",Make,Model,"Electric Vehicle Type","Clean Alternative Fuel Vehicle (CAFV) Eligibility","Electric Range","Base MSRP","Legislative District","DOL Vehicle ID",ST_GeomFromText(coalesce("Vehicle Location", 'Point (0 0)')) as "Vehicle_Location","Electric Utility","2020 Census Tract" FROM 'Data/Electric_Vehicle_Population_Data.csv';

COPY ELECTRIC_VEHICLE_POPULATION FROM (SELECT "VIN (1-10)",County,City,State,"Postal Code","Model Year",Make,Model,"Electric Vehicle Type","Clean Alternative Fuel Vehicle (CAFV) Eligibility","Electric Range","Base MSRP","Legislative District","DOL Vehicle ID",ST_GeomFromText(coalesce("Vehicle Location", 'Point (0 0)')) as "Vehicle_Location","Electric Utility","2020 Census Tract" FROM 'Data/Electric_Vehicle_Population_Data.csv') ;

COPY ELECTRIC_VEHICLE_POPULATION FROM 'Data/Electric_Vehicle_Population_Data.csv' (DELIMITER ',', HEADER);

CREATE TABLE IF NOT EXISTS ELECTRIC_VEHICLE_POPULATION(
    VIN (1-10) VARCHAR,
    County VARCHAR,
    City VARCHAR,
    State VARCHAR,
    Postal Code INTEGER,
    Model Year INTEGER, 
    Make VARCHAR,
    Model VARCHAR,
    Electric Vehicle Type VARCHAR,
    Clean Alternative Fuel Vehicle (CAFV) Eligibility VARCHAR,
    Electric Range INTEGER,
    Base MSRP INTEGER,
    Legislative District INTEGER,
    DOL Vehicle ID INTEGER,
    Vehicle Location  GEOMETRY,
    Electric Utility VARCHAR,
    2020 Census Tract BIGINT
);

select ST_GeomFromText('POINT(2.1744 41.4036)')

select ST_GeomFromText("Vehicle Location") as Vehicle_Location FROM 'Data/Electric_Vehicle_Population_Data.csv'