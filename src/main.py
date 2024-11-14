import numpy as np
import pandas as pd
#import sqlalchemy
from sqlalchemy import (
    create_engine,
    MetaData,
    Table,
    Column,
    DateTime,
    exists,
    inspect,
    text,
    Integer, 
    String, 
    Float, 
    Boolean
)
from sqlalchemy.dialects import postgresql
from sqlalchemy.types import VARCHAR
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.dialects.postgresql import insert
import time
from datetime import datetime,timedelta
import mqtt_db_service as service
import os
import dotenv

def createEngine():    
    engine = create_engine(
        "postgresql://fotovoltaica:fotovoltaica123@150.162.142.84/fotovoltaica", echo=False
    )
    return engine

data = {}

def convert_to_numeric(df):
    for column in df.columns:
        if column != 'TIMESTAMP':  # Ignorar a coluna TIMESTAMP
            try:
                # Tenta converter a coluna para numérico
                df[column] = pd.to_numeric(df[column])
            except (ValueError, TypeError):
                # Se houver erro, mantém a coluna como está
                pass

# Function to map types from pandas to SQLalchey
def map_dtype(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return Integer
    elif pd.api.types.is_float_dtype(dtype):
        return Float 
    elif pd.api.types.is_bool_dtype(dtype):
        return Boolean
    else:
        return VARCHAR

def insert_dataframe(engine, table_name, dataframe):

    # Convert dataframe to dictionary format
    records = dataframe.to_dict(orient='records')
    
    metadata = MetaData()
    table = Table(table_name, metadata, autoload_with=engine)

    with engine.connect() as conn:
        with conn.begin():
            for record in records:
                stmt = insert(table).values(record)
                stmt = stmt.on_conflict_do_nothing(index_elements=['TIMESTAMP']) 
                conn.execute(stmt)

def uploadToDB(engine, dataframe, table_name):
    try:    #Try to incert data all at once (faster)
        dataframe.to_sql(table_name, engine, if_exists='append', index=False)
        print(f"Data successfully uploaded to {table_name} table!")
    except: # Insert data with conflict handling (Slow)
        print(f'primary key conflict, atempeting to insert data avoiding conflicts')
        records = dataframe.to_dict(orient='records')

        metadata = MetaData()
        table = Table(table_name, metadata, autoload_with=engine)

        with engine.connect() as conn:
            with conn.begin():
                for record in records:
                    stmt = insert(table).values(record)
                    stmt = stmt.on_conflict_do_nothing(index_elements=['TIMESTAMP'])
                    conn.execute(stmt)

# Function to check if a table exists inside the database
def tableExists(tableName, engine):
    ins = inspect(engine)
    ret =ins.dialect.has_table(engine.connect(),tableName)
    #print('Table "{}" exists: {}'.format(tableName, ret))
    return ret

# function to create a table on database
def createTable(dataFrame, engine, tableName, column_types):
    print(column_types)
    metadata = MetaData()
    table = Table(tableName, metadata, *(Column(name, column_types[name]) for name in column_types))
    metadata.create_all(engine)

# function that compare header between dataframe and databse to extract diferences
def headerMismach(tableName, engine, dataFrame):
    inspector = inspect(engine)
    columns = inspector.get_columns(tableName)
    column_names = [column['name'] for column in columns]
    headers = dataFrame.columns.tolist()
    missing_in_db = set(headers) - set(column_names)
    return missing_in_db

# function that adds missing columns present on pandas dataframe
def addMissingColumn(missing_columns,engine,table_name,dataFrame):
    metadata = MetaData()
    for column_name in missing_columns:
        column_types = {name: map_dtype(dtype) for name, dtype in dataFrame.dtypes.items()}
        column_type = column_types[column_name]
        print(column_type)
        alter_statement = text(f'ALTER TABLE "{table_name}" ADD COLUMN "{column_name}" {column_type().compile(dialect=engine.dialect)}')
        try:
            with engine.connect() as conn:
                with conn.begin():
                    conn.execute(alter_statement)
            print(f"column '{column_name}' sucessfully added")
        except ProgrammingError as e:
            print(f"Column creation error '{column_name}': {e}")

def primaryKeyExists(engine, tableName):
    metadata = MetaData()
    inspector = inspect(engine)
    primary_keys = inspector.get_pk_constraint(tableName)['constrained_columns']
    return primary_keys

def addPrimarykey(engine, tableName, keyName):
    alter_statement = text(f'ALTER TABLE "{tableName}" ADD PRIMARY KEY ("{keyName}")')
    try:
        with engine.connect() as conn:
            with conn.begin():
                conn.execute(alter_statement)
    except ProgrammingError as e:
        print(f"Set primary key error: {e}")

def check_for_changes(old_df, new_df):
    return not old_df.equals(new_df)

def table_check(ip,lines):
    
    # Removing the first line that contains the ESN
    lines = lines[1:]

    # Processing the data to separate by equipment
    current_equipment_name = None

    tables = []

    for line in lines:
        if line.startswith('#INV'):
            # Capture the name of the new equipment (inverter)
            current_equipment_name = line.strip().split(':')[1]
            #print(f"New equipment detected: {current_equipment_name}")
            tables.append(current_equipment_name)
    
    return tables

# Função auxiliar para converter o formato de timestamp
def parse_time(value):
    try:
        # Verifica o comprimento da string para decidir o formato
        if len(value) == 19:  # Formato: '2024-11-07 11:34:00'
            return pd.to_datetime(value, format='%Y-%m-%d %H:%M:%S')
        elif len(value) == 17:  # Formato: '24-11-07 09:18:00'
            return pd.to_datetime(value, format='%y-%m-%d %H:%M:%S')
    except ValueError:
        print("TIMESTAMP format not supported")
        return pd.NaT

def healthCheck():
    # Código crítico
    with open('/tmp/heartbeat.txt', 'w') as f:
        f.write(str(time.time()))  # Escreve o timestamp

# Coisas adicionadas para mandar pro servidor do Lucas
#engine2 = create_engine(f'postgresql://fotovoltaica:TSAL6ujJn8pD7Nq@150.162.142.79/fotovoltaica', echo=False) # tirar depois

dotenv.load_dotenv()
brokers = os.getenv("MQTT_BROKER").split(',')
service.initDBService(user=os.getenv("USER"), server1=brokers[0], server2=brokers[1])
ips = os.getenv("IPS").split(',')

def main():
    
    # falta fazer o loop para pegar os dados do arquivo
    # Faquer que nem o outro código que percebe a passagem do dia para pegar mais dados
    # print(tables)

    df_antigo = pd.read_csv('../ftp/data/init.csv')
    global data
    engine = createEngine()

    print("start")

    while(1):
        for ip in ips:
            
            print(f'Checking logger on ip: {ip}')
            # read current csv
            folder_terminator = ip.split(".")[3]
            today = datetime.today() - timedelta(hours=2)
            try:
                file_path = f'../ftp/data/HW{folder_terminator}/min{today.strftime("%Y%m%d")}.csv'
                with open(file_path, 'r') as file:
                    lines = file.readlines()
            except:
                print("Newest File not found")
                continue
            
            #Utilizar a verificação se o arquivo mudou, caso não tenha mudado não tem pq fazer o processo (futuro)
            #tomorrow = today + timedelta(days=1)
            tables = table_check(ip,lines)
            print(f'tables found: {tables}')
            
            ###
            for table in tables:
                #check for the most recent stored data timestamp
                server_timestamp = service.getLastTimestamp(table)
                
                if server_timestamp is None:
                    server_timestamp = today - timedelta(days=30)
                
                #print(f'server_timestap_get: {server_timestamp}')
                # try to read file from server tome to today
                while today>=server_timestamp:
                    #print(server_timestamp)
                    #print(f'../ftp/data/HW{folder_terminator}/min{server_timestamp.strftime("%Y%m%d")}.csv')
                    try:
                        file_path = f'../ftp/data/HW{folder_terminator}/min{server_timestamp.strftime("%Y%m%d")}.csv'
                        with open(file_path, 'r') as file:
                            lines = file.readlines()
                    except:
                        print("file not found on this date, jumping to next day")
                        server_timestamp += timedelta(days=1)
                        continue
                    
                    # finding on CSV file the correct table chunk
                    init_found = 0
                    for index,line in enumerate(lines):
                        if table in line:
                            line_init = index + 1
                            logger_name = line.strip().split(':')[1]
                            init_found = 1
                            #print(f'line_init_{line_init}: {lines[line_init]}')
                        elif init_found and "#" in line and "#Time" not in line: # IQ teste here to test your logic
                            line_end = index -1
                            #print(f'line_end_{line_end}: {lines[line_end]}')
                            break
                        elif index == len(lines)-1:
                            line_end = index
                            #print(f'line_end_{line_end}: {lines[line_end]}')
                            break
                    #print(lines[line_init:line_end])
                    
                    #extrai o chunck de dados referente a um data logger
                    try:
                        data_chunk=lines[line_init:line_end]
                    except:
                        print(f'missing {table} on file: ../ftp/data/HW{folder_terminator}/min{server_timestamp.strftime("%Y%m%d")}.csv')
                        server_timestamp += timedelta(days=1)
                        continue
                    #extai os headers
                    headers = data_chunk[0].strip("#").strip().split(";")
                    # Remover o caractere de nova linha e separador extra (se houver) nas linhas de 
                    data_rows = [line.strip().rstrip(";").split(";") for line in data_chunk[1:]]
                    # Criar o DataFrame
                    df = pd.DataFrame(data_rows, columns=headers)

                    # Aplica a função de conversão na coluna 'Time'
                    df['Time'] = df['Time'].apply(parse_time)
                    # Renomeando a coluna "Time" para "TIMESTAMP"
                    df.rename(columns={'Time': 'TIMESTAMP'}, inplace=True)
                    #print(df)

                    # Converting datatype
                    convert_to_numeric(df)
                    #column_types = {name: map_dtype(dtype) for name, dtype in df.dtypes.items()}

                    #print("uploading data to database")
                    data = {}
                    data["df_data"] = df
                    data['loggerRequestBeginTime'] = datetime.now().isoformat()
                    data['loggerRequestEndTime'] = data['loggerRequestBeginTime']
                    print(data['loggerRequestBeginTime'])
                    data['report'] = "Success"
                    if data["report"] == "Success":
                        response = service.sendDF(data, table=logger_name)
                        print(response)
                        if response == "mqtt timeout":
                            print("Sending data to mqtt timeout")
                        time.sleep(0.2)
                        healthCheck()
                    
                    print(logger_name)
                    print(f'../ftp/data/HW{folder_terminator}/min{server_timestamp.strftime("%Y%m%d")}.csv')
                    print(f'today : {today}')
                    print(f'server_timestamp : {server_timestamp}')
                    print(" ")
                    server_timestamp += timedelta(days=1)

                ######

        print("Waiting 900s")
        time.sleep(900)  

main()

"""
    today = datetime.today()
    file_path = f'../ftp/data/HW64/min{today.strftime("%Y%m%d")}.csv'
    #print(file_path)
    df_novo = pd.read_csv(file_path)
    #print(df_novo)
    if check_for_changes(df_antigo, df_novo):
        df_antigo = df_novo
        #global data

        #engine = createEngine()

        # Reading the CSV file, assuming the first line is the SmartLogger ESN
        with open(file_path, 'r') as file:
            lines = file.readlines()
        #print(lines[0].split(" "))
        # Extracting the SmartLogger serial number
        smartlogger_esn = lines[0].split("\t")[1].strip()
        print(f"SmartLogger ESN: {smartlogger_esn}")

        # Removing the first line that contains the ESN
        lines = lines[1:]

        # Processing the data to separate by equipment
        equipment_data_dict = {}
        current_data = []
        headers = None
        current_equipment_name = None

        for line in lines:
            if line.startswith('#INV'):
                # Save the previous data if it exists
                if current_data and headers and current_equipment_name:
                    df = pd.DataFrame(current_data, columns=headers)
                    equipment_data_dict[current_equipment_name] = df
                    current_data = []
                # Capture the name of the new equipment (inverter)
                current_equipment_name = line.strip().split(':')[1]
                print(f"New equipment detected: {current_equipment_name}")
                headers = None  # Reset headers for the new equipment
            
            elif line.startswith('#Time'):
                # Header line for the data
                headers = line.strip().split(';')
                headers = ["TIMESTAMP" if h == "#Time" else h for h in headers]  # Replace '#Time' with 'TIMESTAMP'
                print(f"Headers identified: {headers}")
            
            else:
                # Data line
                if headers:
                    data = line.strip().split(';')
                    if len(data) == len(headers) + 1:  # Check if there is an extra data
                        data = data[:-1]  # Remove the last element
                    if len(data) == len(headers):  # Ensure data matches headers
                        current_data.append(data)
                        #print(f"Data added: {data}")

        # Add the last set of collected data if any
        if current_data and headers and current_equipment_name:
            df = pd.DataFrame(current_data, columns=headers)
            equipment_data_dict[current_equipment_name] = df
            print(f"Final dataframe added with headers: {headers}")

        for equipment_name, dataframe in equipment_data_dict.items():

            #Check if TIMESTAMP is writed in uper case
            if 'timestamp' in df.columns:
                df.rename(columns={'timestamp': 'TIMESTAMP'}, inplace=True)

            #change TIMESTAMP datatype if necessary
            dataframe['TIMESTAMP'] = pd.to_datetime(dataframe['TIMESTAMP'])

            # Converting datatype
            convert_to_numeric(dataframe)
            column_types = {name: map_dtype(dtype) for name, dtype in dataframe.dtypes.items()}
            
            #if not tableExists(equipment_name,engine):
            #    print('The table does not exist, creating table')
                #createTable(dataframe, engine, equipment_name, column_types)

            #missmach = headerMismach(equipment_name,engine,dataframe)
            #if(len(missmach) !=0):
            #    print(f'{len(missmach)} mismach were found, adding headers to database')
                #addMissingColumn(missmach,engine,equipment_name,dataframe)

            #primaryKey = primaryKeyExists(engine,equipment_name)
            #if primaryKey == []:
            #    print(primaryKey)
                #addPrimarykey(engine,equipment_name,'TIMESTAMP') # for now the only primary key is going to be timestamp, changein the future

            print("uploading data to database")
            data = {}
            data["df_data"] = dataframe
            data['report'] = "Success"
            if data["report"] == "Success":
                response = service.sendDF(data, table=equipment_name)
                print(response)
                if response == "mqtt timeout":
                    print("Sending data to mqtt timeout")
                    continue
                time.sleep(0.2)
                #healthCheck() add health check system.
                continue
            #uploadToDB(engine,dataframe,equipment_name)
            #uploadToDB(engine2,dataframe,equipment_name)

    else:
        print("No new changes, waiting 5 min")
    print("Waiting 5min until next check")
    time.sleep(300)
"""

