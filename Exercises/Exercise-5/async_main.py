import asyncio
import logging
import os
from io import StringIO

import asyncpg
import pandas as pd


def get_file_paths(root_directory, folder_name, file_extension):
    sql_file_paths = []
    sql_directory = os.path.join(root_directory, folder_name)
    for root, _, files in os.walk(sql_directory):
        for file_name in files:
            if file_extension in file_name:
                sql_file_paths.append(os.path.join(root, file_name))
    return sql_file_paths


def read_sql_file(sql_file_path):
    file = open(file=sql_file_path, mode="r")
    sql_file = file.read()
    file.close()
    sql_commands = sql_file.split(";")
    return sql_commands


async def main():
    connection = await asyncpg.connect(
        host="localhost",
        port=5432,
        user="postgres",
        database="postgres",
        password="postgres",
    )

    current_directory = os.getcwd()
    sql_file_paths = get_file_paths(current_directory, f"ddl_scripts", "sql")
    csv_file_paths = get_file_paths(current_directory, r"data", "csv")
    try:
        async with connection.transaction():
            for sql_file_path in sql_file_paths:
                sql_commands = read_sql_file(sql_file_path)
                for command in sql_commands:
                    command = command.strip()
                    if command:
                        print(f"Executing {command}")
                        await connection.execute(command)

                logging.info("Created tables successfully")
                print("Created tables successfully")

            for csv_file_path in csv_file_paths:
                file_name = csv_file_path.split("\\")[-1]
                table_name = file_name.replace(".csv", "")
                with open(csv_file_path, "r") as file:
                    # next(file)
                    print(f"Reading {table_name}")
                    df = pd.read_csv(csv_file_path)
                    df.rename(columns=lambda x: x.strip(), inplace=True)
                    df = df.convert_dtypes(convert_integer=False)
                    df = df.applymap(
                        lambda x: " ".join(x.split()) if isinstance(x, str) else x
                    )
                    df.fillna("", inplace=True)
                    if table_name == "transactions":
                        df["transaction_date"] = pd.to_datetime(
                            df["transaction_date"], format="%Y/%m/%d"
                        )

                    tuples = [tuple(x) for x in df.values]

                    # # await connection.execute(f"TRUNCATE {table_name};")
                    await connection.copy_records_to_table(
                        f"{table_name}", records=tuples, columns=list(df.columns)
                    )
                print(f"Inserted rows successfully for {table_name}")
                query = f"""Select * from {table_name};"""
                rows = await connection.fetch(query)  # C
                for row in rows:
                    print(dict(row))

    except Exception as err:
        print(f"Unexpected {err=}, {type(err)=}")
        logging.error(f"Unexpected {err=}, {type(err)=}")
    finally:
        await connection.close()


asyncio.run(main())
