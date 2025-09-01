import asyncio
import os

import aioduckdb


def get_env_path(folder, ext):
    """Get all file paths in a folder with a given extension."""
    file_paths = []
    for root, _, files in os.walk(folder):
        for file_name in files:
            if file_name.endswith(ext):
                file_paths.append(os.path.join(root, file_name))
    return file_paths


async def setup_extension(connection, extension_name):
    await connection.execute(f"install {extension_name};")
    await connection.execute(f"load {extension_name};")


async def read_sql_file(sql_file_path):
    with open(sql_file_path, "r") as file:
        sql_commands = file.read().split(";")
    return [cmd.strip() for cmd in sql_commands if cmd.strip()]


async def execute_sql_file(connection, sql_file_paths):
    """Executes all SQL commands in the given files."""
    table_name = ""
    for sql_file_path in sql_file_paths:
        sql_commands = await read_sql_file(sql_file_path)
        for command in sql_commands:
            print(f"Executing {command}")
            await connection.execute(command)
        # Optionally extract table name from the first command
        table_name = sql_commands[0].split(" ")[-1]
    return table_name


async def truncate_table(connection, table_name):
    await connection.execute(f"truncate table {table_name};")


async def get_insert_query(table_name, file_path):
    return f"""
    INSERT INTO {table_name}(
        VIN, County, City, State, Postal_Code, Model_Year, Make, Model,
        Electric_Vehicle_Type, Clean_Alternative_Fuel_Vehicle_Eligibility,
        Electric_Range, Base_MSRP, Legislative_District, DOL_Vehicle_ID,
        Vehicle_Location, Electric_Utility, TwentyTwenty_Census_Tract
    ) SELECT
        "VIN (1-10)" as VIN, County, City, State, "Postal Code" as Postal_Code,
        "Model Year" as Model_Year, Make, Model, "Electric Vehicle Type" as Electric_Vehicle_Type,
        "Clean Alternative Fuel Vehicle (CAFV) Eligibility" as Clean_Alternative_Fuel_Vehicle_Eligibility,
        "Electric Range" as Electric_Range, "Base MSRP" as Base_MSRP,
        "Legislative District" as Legislative_District, "DOL Vehicle ID" as DOL_Vehicle_ID,
        ST_GeomFromText(coalesce("Vehicle Location", 'Point (0 0)')) as Vehicle_Location,
        "Electric Utility" as Electric_Utility, "2020 Census Tract" as TwentyTwenty_Census_Tract
    FROM '{file_path}';
    """


async def insert_from_csv(connection, table_name, file_path):
    await truncate_table(connection, table_name)
    insert_query_command = await get_insert_query(table_name, file_path)
    await connection.execute(insert_query_command)
    print(f"Inserted rows into {table_name} from {file_path}")


async def query_to_csv(connection, query, output_path):
    cursor = await connection.cursor()
    cursor = await cursor.execute(query)
    df = await cursor.df()
    df.to_csv(output_path, index=False)


async def query_to_parquet(connection, query, output_path, partition_cols=None):
    cursor = await connection.cursor()
    cursor = await cursor.execute(query)
    df = await cursor.df()
    df.to_parquet(output_path, partition_cols=partition_cols or [])
    # Explicitly delete the DataFrame to release resources
    del df


async def get_number_of_cars_per_city(
    connection, table_name, output_path="number_of_cars_per_city.csv"
):
    print("Getting number of cars per city")
    print(f"Output path: {output_path}")
    query = f"SELECT City, COUNT(*) as Number_Of_Cars FROM {table_name} GROUP BY City ORDER BY COUNT(*) DESC;"
    await query_to_csv(connection, query, output_path)


async def get_top_three_make_model(
    connection, table_name, output_path="top_three_make_model.csv"
):
    query = f"""
    WITH make_model_count AS (
        SELECT Make, Model, COUNT(*) as cnt FROM {table_name} GROUP BY Make, Model
    )
    SELECT Make, Model FROM make_model_count ORDER BY cnt DESC LIMIT 3;
    """
    await query_to_csv(connection, query, output_path)


async def get_most_popular_vehicle_postal_code(
    connection, table_name, output_path="most_popular_vehicle_postal_code.csv"
):
    query = f"""
    WITH ranked AS (
        SELECT "Postal_Code", Make, Model, COUNT(*) as cnt,
               ROW_NUMBER() OVER (PARTITION BY "Postal_Code" ORDER BY COUNT(*) DESC) as rn
        FROM {table_name}
        GROUP BY "Postal_Code", Make, Model
    )
    SELECT "Postal_Code", Make, Model FROM ranked WHERE rn = 1;
    """
    await query_to_csv(connection, query, output_path)


async def get_number_of_cars_by_model_year(
    connection, table_name, output_path="./cars_by_model_year"
):
    query = f"""
    SELECT Make, Model, "Model_Year" as Year, COUNT(*) as Number_Of_Cars
    FROM {table_name}
    GROUP BY Make, Model, "Model_Year"
    ORDER BY COUNT(*) DESC;
    """
    await query_to_parquet(connection, query, output_path, partition_cols=["Year"])


async def main():
    try:
        current_directory = os.getcwd()
        csv_file_paths = get_env_path(os.path.join(current_directory, "data"), ".csv")
        if not csv_file_paths:
            raise FileNotFoundError("No CSV files found in data directory.")
        csv_file_name = csv_file_paths[0]
        con = await aioduckdb.connect("new_persistent.db")
        await setup_extension(con, "spatial")
        sql_file_paths = get_env_path(
            os.path.join(current_directory, "ddl_scripts"), ".sql"
        )
        table_name = await execute_sql_file(con, sql_file_paths)
        await insert_from_csv(con, table_name, csv_file_name)
        await get_number_of_cars_per_city(con, table_name)
        await get_top_three_make_model(con, table_name)
        await get_most_popular_vehicle_postal_code(con, table_name)
        await get_number_of_cars_by_model_year(con, table_name)
        await con.close()
    except Exception as e:
        print(e)
    finally:
        print("Exiting")


if __name__ == "__main__":
    asyncio.run(main())
