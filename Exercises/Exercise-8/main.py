import asyncio
import os

import aioduckdb
import duckdb as dd


async def get_file_paths(root_directory, folder_name, file_extension):
    file_paths = []
    file_directory = os.path.join(root_directory, folder_name)
    for root, _, files in os.walk(file_directory):
        for file_name in files:
            if file_extension in file_name:
                file_paths.append(os.path.join(root, file_name))
    return file_paths


async def setup_extension(connection, extension_name):
    await connection.execute(f"install {extension_name};")
    await connection.execute(f"load {extension_name};")


async def read_sql_file(sql_file_path):
    file = open(file=sql_file_path, mode="r")
    sql_file = file.read()
    file.close()
    sql_commands = sql_file.split(";")
    return sql_commands


async def create_table(connection, ddl_path_location):
    table_name = ""
    for sql_file_path in ddl_path_location:
        sql_commands = await read_sql_file(sql_file_path)
        for command in sql_commands:
            command = command.strip()
            if command:
                print(f"Executing {command}")
                await connection.execute(command)
        table_name = sql_commands[0].split(" ")[-1]
    return table_name


async def insert_rows_from_csv_file(connection, table_name, file_path):
    print(f"truncate table {table_name}")
    truncate_command = f"truncate table {table_name};"
    await connection.execute(truncate_command)
    sql_command = f"""
    INSERT INTO {table_name}(
        VIN,
        County,
        City,
        State,
        Postal_Code,
        Model_Year, 
        Make,
        Model,
        Electric_Vehicle_Type,
        Clean_Alternative_Fuel_Vehicle_Eligibility,
        Electric_Range,
        Base_MSRP,
        Legislative_District,
        DOL_Vehicle_ID,
        Vehicle_Location,
        Electric_Utility,
        TwentyTwenty_Census_Tract
    ) SELECT
        "VIN (1-10)" as VIN,
        County,
        City,
        State,
        "Postal Code" as Postal_Code,
        "Model Year" as Model_Year,
        Make,
        Model,
        "Electric Vehicle Type" as Electric_Vehicle_Type,
        "Clean Alternative Fuel Vehicle (CAFV) Eligibility" as Clean_Alternative_Fuel_Vehicle_Eligibility,
        "Electric Range" as Electric_Range,
        "Base MSRP" as Base_MSRP,
        "Legislative District" as Legislative_District,
        "DOL Vehicle ID" as DOL_Vehicle_ID,
        ST_GeomFromText(coalesce("Vehicle Location", 'Point (0 0)')) as Vehicle_Location,
        "Electric Utility" as Electric_Utility,
        "2020 Census Tract" as TwentyTwenty_Census_Tract
    FROM '{file_path}';
    """
    await connection.execute(sql_command)
    print(f"Successfully inserted rows into {table_name} from {file_path}")


async def get_number_of_cars_per_city(connection, table_name):
    query_str = f"SELECT City, COUNT(*) as Number_Of_Cars FROM {table_name} GROUP BY City order by COUNT(*) desc;"
    cursor = await connection.cursor()
    cursor = await cursor.execute(query_str)
    df = await cursor.df()
    df.to_csv("number_of_cars_per_city.csv", index=False)


async def get_top_three_make_model(connection, table_name):
    query_str = f"""
    with make_model_count as 
    (
        SELECT Make, Model, COUNT(*) FROM {table_name} GROUP BY "Make", "Model" order by COUNT(*) desc
    )
    select Make, Model from make_model_count LIMIT 3;
    """
    cursor = await connection.cursor()
    cursor = await cursor.execute(query_str)
    df = await cursor.df()
    df.to_csv("top_three_make_model.csv", index=False)


async def get_most_popular_vehicle_postal_code(connection, table_name):
    query_str = f"""with postal_code_model as(
            SELECT "Postal_Code", Make, Model, COUNT(*) 
            FROM {table_name} 
            GROUP BY "Postal_Code" , "Make", "Model"
            QUALIFY ROW_NUMBER() OVER (PARTITION BY "Postal_Code" ORDER BY COUNT(*) DESC) = 1
            )
        select "Postal_Code", Make, Model from postal_code_model;"""
    cursor = await connection.cursor()
    cursor = await cursor.execute(query_str)
    df = await cursor.df()
    df.to_csv("most_popular_vehicle_postal_code.csv", index=False)


async def get_number_of_cars_by_model_year(connection, table_name):
    query_str = f"""
        SELECT Make, Model, "Model_Year" as Year, COUNT(*) as Number_Of_Cars FROM {table_name} GROUP BY "Make", "Model", "Model_Year" order by COUNT(*) desc;
    """
    cursor = await connection.cursor()
    cursor = await cursor.execute(query_str)
    df = await cursor.df()
    df.to_parquet("./cars_by_model_year", partition_cols=["Year"])


async def main():
    try:
        current_directory = os.getcwd()
        csv_file_paths = await get_file_paths(current_directory, "data", ".csv")
        print(csv_file_paths)
        csv_file_name = csv_file_paths[0]
        con = await aioduckdb.connect("new_persistent.db")
        await setup_extension(connection=con, extension_name="spatial")
        sql_file_paths = await get_file_paths(current_directory, f"ddl_scripts", "sql")
        table_name = await create_table(
            connection=con, ddl_path_location=sql_file_paths
        )
        print(table_name)
        await insert_rows_from_csv_file(
            connection=con, table_name=table_name, file_path=csv_file_name
        )
        # await get_number_of_cars_per_city(connection=con, table_name=table_name)
        # await get_top_three_make_model(connection=con, table_name=table_name)
        # await get_most_popular_vehicle_postal_code(
        #     connection=con, table_name=table_name
        # )
        await get_number_of_cars_by_model_year(connection=con, table_name=table_name)
        await con.close()
    except Exception as e:
        print(e)
    finally:
        print("Exiting")


if __name__ == "__main__":
    asyncio.run(main())
