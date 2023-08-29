import csv
import datetime
import sqlite3

def transfer_data_db(server_file_name: str, client_file_name: str, date_interval: str):
    date_interval_start = int(datetime.datetime.strptime(date_interval.split('TO')[0], '%Y-%m-%d').timestamp())
    date_interval_stop = int(datetime.datetime.strptime(date_interval.split('TO')[1], '%Y-%m-%d').timestamp())
    server_for_date = []
    client_for_date = []
    try:
        with open(server_file_name, 'r', newline='') as csv_server_file:
            server_reader = csv.DictReader(csv_server_file)
            with open(client_file_name, 'r', newline='') as csv_client_file:
                client_reader = csv.DictReader(csv_client_file)
                for server_record, client_record in zip(server_reader, client_reader):
                    if date_interval_start <= int(server_record['timestamp']) <= date_interval_stop:
                        server_for_date.append(server_record)
                    if date_interval_start <= int(client_record['timestamp']) <= date_interval_stop:
                        client_for_date.append(client_record)
    except Exception as e:
        print(f"Ошибка при чтение csv файлов {str(e)}")
    united_records = {}
    try:
        with sqlite3.connect('cheaters.db') as conn:
            cursor = conn.cursor()
            for server_record in server_for_date:
                for client_record in client_for_date:
                    if client_record['error_id'] == server_record['error_id']:
                        player_id = client_record['player_id']
                        cursor.execute(f"SELECT ban_time FROM cheaters WHERE player_id = '{player_id}'")
                        db_record = cursor.fetchall()
                        if int(datetime.datetime.strptime(db_record[0][0], '%Y-%m-%d %H:%M:%S').timestamp()) < int(server_record['timestamp']):
                            continue
                        else:
                            united_records.update(
                                {
                                    server_record['error_id']: {
                                        'timestamp': server_record['timestamp'],
                                        'player_id': client_record['player_id'],
                                        'json_server': server_record['description'],
                                        'json_client': client_record['description']
                                    }
                                }
                            )
                    break
    except Exception as e:
        print(f"Ошибка при объединении данных из csv файлов: {str(e)}")
    with sqlite3.connect('identifier.sqlite') as conn:
        cursor = conn.cursor()
        records_to_db = []
        for key, value in united_records.items():
            records_to_db.append((value['timestamp'], value['player_id'], key, value['json_server'], value['json_client']))
        cursor.executemany("INSERT INTO meowpunk VALUES(?, ?, ?, ?, ?);", records_to_db)
        conn.commit()


transfer_data_db('server.csv', 'client.csv', date_interval='2021-04-07TO2021-04-09')
