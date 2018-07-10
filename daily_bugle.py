import datetime
import os.path
import pandas as pd
import sqlite3
import sys
import yaml

def get_config(config_path):
    try:
        with open(config_path, 'r') as config_file:
            config = yaml.load(config_file.read())
    except:
        print("Error: Check config file")
        exit()
    return config

def get_commands(file_list):
    commands = []
    for filepath in file_list:
        with open(filepath, 'r') as command_file:
            commands.extend(command_file.read().splitlines())
    return commands

def run_views(view_list, c, conn):
    for view in view_list:
        c.execute(view)
    conn.commit()

def run_queries(query_list, conn, output):
    with open(output, 'w') as of:
        count = 0
        for query in query_list:
            if count%2==0:
                of.write(query)
                of.write('\n')
            else:
                of.write(str(pd.read_sql_query(query, conn)))
                of.write('\n\n')
            count+=1

def main(config_path):
    config = get_config(config_path)
    db = config.get('database')
    conn = sqlite3.connect(db)
    c = conn.cursor()

    try:
        view_files = config.get('view_files')
        view_list = get_commands(view_files)
        run_views(view_list, c, conn)
    except KeyError as e:
        pass

    output_folder = config.get('output_folder')
    timestamp = datetime.datetime.now().strftime("%Y_%m_%d")
    output = os.path.join(output_folder, (timestamp + '.txt'))

    query_files = config.get('query_files')
    query_list = get_commands(query_files)
    run_queries(query_list, conn, output)

    print("Check " + output)

if __name__ == '__main__':
    main(sys.argv[1])
