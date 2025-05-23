import csv
import os

import psycopg2


def connect_to_posgres():
    host = "localhost"
    port = "5432"
    database = "postgres"
    user = "postgres"
    pas = "postgres"
    conn = psycopg2.connect(
        host=host, port=port, database=database, user=user, password=pas
    )
    cur = conn.cursor()
    return conn, cur


def get_file_paths(root_directory, folder_name, file_extension):
    file_paths = []
    file_directory = os.path.join(root_directory, folder_name)
    for root, _, files in os.walk(file_directory):
        for file_name in files:
            if file_extension in file_name:
                file_paths.append(os.path.join(root, file_name))
    return file_paths


def read_sql_file(sql_file_path):
    file = open(file=sql_file_path, mode="r")
    sql_file = file.read()
    file.close()
    sql_commands = sql_file.split(";")
    return sql_commands


def create_tables(cur, sql_file_paths):
    for sql_file_path in sql_file_paths:
        sql_commands = read_sql_file(sql_file_path)
        for command in sql_commands:
            try:
                if command:
                    cur.execute(command)
            except Exception as err:
                print(f"Unexpected {err=}, {type(err)=}")

                raise


def copy_from_csv_files(conn, cur, csv_file_paths):
    for csv_file_path in csv_file_paths:
        file_name = csv_file_path.split("\\")[-1]
        table_name = file_name.replace(".csv", "")
        with open(csv_file_path, "r") as file:
            next(file)
            cur.copy_from(file, table_name, sep=",")
            conn.commit()


def main():
    conn, cur = connect_to_posgres()
    current_directory = os.getcwd()
    sql_file_paths = get_file_paths(current_directory, f"ddl_scripts", "sql")
    create_tables(cur, sql_file_paths)
    csv_file_paths = get_file_paths(current_directory, r"data", "csv")
    copy_from_csv_files(conn, cur, csv_file_paths)


if __name__ == "__main__":
    main()
