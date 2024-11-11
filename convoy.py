import csv
import json
import re
import sqlite3
from math import floor

import pandas as pd
from lxml import etree


def format_xlsx_to_csv(file_path: str, csv_file_name: str) -> None:
    df = pd.read_excel(file_path, 'Vehicles', dtype=str)
    df.to_csv(f'{csv_file_name}.csv', index=False, header=True)
    print(f'{df.shape[0]} {"lines were" if df.shape[0] > 1 else "line was"} imported to {csv_file_name}.csv')


def correct_dataframe(dataframe: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    corrections = 0
    for column in dataframe:
        for i in range(dataframe.shape[0]):
            if not dataframe[column][i].isdigit():
                dataframe.loc[i, column] = re.sub(r'\D', '', dataframe[column][i])
                corrections += 1
    return dataframe, corrections


def get_vehicles_list(csv_file_name: str) -> list[tuple]:
    with (open(f'{csv_file_name}.csv', 'r') as file):
        reader = csv.reader(file, delimiter=',')
        next(reader)
        vehicles_list = []
        for row in reader:
            vehicle_id = int(row[0])
            engine_capacity = int(row[1])
            fuel_consumption = int(row[2])
            maximum_load = int(row[3])
            fuel_consumption_over_trip = 450 * fuel_consumption / 100
            pit_stops = floor(fuel_consumption_over_trip / engine_capacity)
            score = ((0 if pit_stops >= 2 else 1 if pit_stops == 1 else 2) +
                     (2 if fuel_consumption_over_trip <= 230 else 1) +
                     (2 if maximum_load >= 20 else 0))
            vehicles_list.append((vehicle_id, engine_capacity, fuel_consumption, maximum_load, int(score)))
    return vehicles_list


def create_database(db_file_name: str, df: pd.DataFrame, vehicles_list: list[tuple]) -> None:
    with sqlite3.connect(db_file_name) as con:
        cursor = con.cursor()
        columns_definition = ', '.join([f'{col} INTEGER NOT NULL' for col in df.columns[1:]])
        sql_create_table = (f'CREATE TABLE IF NOT EXISTS convoy({df.columns[0]} INTEGER PRIMARY KEY, '
                            f'{columns_definition}, score INTEGER NOT NULL);')
        cursor.execute(sql_create_table)
        cursor.executemany(f'INSERT INTO convoy VALUES(?, ?, ?, ?, ?)', vehicles_list)
        row_count = cursor.rowcount
        print(f'{row_count} record{"s were" if row_count > 1 else " was"} inserted into {db_file_name}')


def query_database(db_file_name: str) -> list[tuple]:
    with sqlite3.connect(db_file_name) as con:
        cursor = con.cursor()
        cursor.execute('SELECT * FROM convoy;')
        return cursor.fetchall()


def export_to_json(db_all_rows: list, json_file_name: str) -> None:
    vehicles_list = [
        {'vehicle_id': row[0], 'engine_capacity': row[1], 'fuel_consumption': row[2], 'maximum_load': row[3]} for
        row in db_all_rows if row[4] > 3
    ]
    with open(f'{json_file_name}.json', 'w') as json_file:
        json.dump({'convoy': vehicles_list}, json_file, indent=4)

    vehicles_count = len(vehicles_list)
    print(f'{vehicles_count} vehicle{" was" if vehicles_count == 1 else "s were"} saved into {json_file_name}.json')


def export_to_xml(db_all_rows: list, xml_file_name: str) -> None:
    vehicles_list = [
        (f'<vehicle>'
         f'<vehicle_id>{row[0]}</vehicle_id>'
         f'<engine_capacity>{row[1]}</engine_capacity>'
         f'<fuel_consumption>{row[2]}</fuel_consumption>'
         f'<maximum_load>{row[3]}</maximum_load>'
         f'</vehicle>')
        for row in db_all_rows if row[4] <= 3
    ]
    xml_string = '<convoy>' + ''.join(vehicle for vehicle in vehicles_list) + '</convoy>' if vehicles_list else \
        '<convoy>\n</convoy>'
    root = etree.fromstring(xml_string)
    etree.indent(root, space='    ')
    tree = etree.ElementTree(root)
    tree.write(f'{xml_file_name}.xml', pretty_print=True)

    vehicles_count = len(vehicles_list)
    print(f'{vehicles_count} vehicle{" was" if vehicles_count == 1 else "s were"} saved into {xml_file_name}.xml')


def main() -> None:
    file_path = input('Input file name\n')
    file_name, file_ext = file_path.split('.')

    if file_ext == 'xlsx':
        format_xlsx_to_csv(file_path, file_name)

    if not file_name.endswith('[CHECKED]') and file_ext != 's3db':
        df = pd.read_csv(f'{file_name}.csv', dtype=str)
        df, corrections = correct_dataframe(df)
        df.to_csv(f'{file_name}[CHECKED].csv', index=False, header=True)
        print(f'{corrections} cell{"s were" if corrections > 1 else " was"} corrected in {file_name}[CHECKED].csv')
        file_name += '[CHECKED]'

    if file_name.endswith('[CHECKED]') and file_ext != 's3db':
        df = pd.read_csv(f'{file_name}.csv', dtype=str)
        vehicles_list = get_vehicles_list(file_name)
        db_file = f'{file_name.rstrip("[CHECKED]")}.s3db'
        create_database(db_file, df, vehicles_list)
        file_path = db_file

    if file_path.endswith('s3db'):
        file_name, _ = file_path.split('.')
        query_result = query_database(file_path)
        export_to_json(query_result, file_name)
        export_to_xml(query_result, file_name)


if __name__ == '__main__':
    main()
