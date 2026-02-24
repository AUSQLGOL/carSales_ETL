import boto3
import csv
import psycopg2
from psycopg2 import sql
from datetime import datetime
import re
import os
import logging
from retrying import retry
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# Cargar las variables de entorno desde el archivo .env
load_dotenv()
DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASS')
DB_HOST = os.getenv('DB_HOST')

# Configuración de logging para registrar errores
logging.basicConfig(filename='app.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Parámetros generales
S3_BUCKET_NAME = 'beheritbucket'
LOCAL_CSV_FILE = r'C:\Users\USER\Desktop\carSales_ETL\car_prices.csv'
PROCESSED_CSV_FILE = r'C:\Users\USER\Desktop\carSales_ETL\processed_data.csv'

# Retry decorator: Espera exponencial en caso de error
@retry(stop_max_attempt_number=3, wait_exponential_multiplier=1000, wait_exponential_max=10000)
def connect_postgres():
    """Establece una conexión a PostgreSQL con retries y timeout."""
    print("Conectando a la base de datos PostgreSQL...")
    return psycopg2.connect(
        dbname="transactions",
        user=DB_USER,
        password=DB_PASS,
        host=DB_HOST,
        connect_timeout=10
    )

def create_table(cursor):
    """Crea la tabla 'cars' si no existe."""
    create_table_query = """
    CREATE TABLE IF NOT EXISTS cars (
        id SERIAL PRIMARY KEY,
        year INT,
        maker VARCHAR(255),
        model VARCHAR(255),
        trim VARCHAR(255),
        body VARCHAR(255),
        transmission VARCHAR(255),
        vin VARCHAR(17) UNIQUE,
        state VARCHAR(255),
        condition VARCHAR(255),
        odometer INT,
        color VARCHAR(255),
        interior VARCHAR(255),
        seller VARCHAR(255),
        mmr INT,
        sellingprice INT,
        saledate TIMESTAMP
    );
    """
    cursor.execute(create_table_query)

def validate_and_process_row(row, index):
    """Validar y procesar la fila del CSV, asignando valores predeterminados si es necesario."""
    # Asignar valores predeterminados para celdas vacías o nulas
    for field in row:
        if not row[field]:  # Si el valor está vacío o es None
            if field in ['year', 'odometer', 'mmr', 'sellingprice']:  # Campos numéricos
                row[field] = 0  # Asignar 0 en caso de que el campo sea numérico
            else:
                row[field] = None  # Asignar None para texto

    # Procesar la fecha de saledate si está presente
    if row.get('saledate'):
        try:
            date_parts = re.split(r' GMT[+-]\d{4} \(.+\)', row['saledate']) # Elimina zona horaria
            row['saledate'] = datetime.strptime(date_parts[0].strip(), "%a %b %d %Y %H:%M:%S") # Tue Dec 16 2014 12:30:00 GMT-0800 (PST)  --->  Tue Dec 16 2014 12:30:00
        except Exception as e:
            logging.error(f"Error procesando la fecha en la fila {index}: {e}")
            row['saledate'] = None  # Asignar None si la fecha no es válida

    # Renombrar 'make' a 'maker'
    if 'make' in row:
        row['maker'] = row.pop('make')
    return row

def process_csv_and_insert(cursor, csv_file):
    """Lee y procesa el archivo CSV e inserta datos en PostgreSQL."""
    header = ['year', 'make', 'model', 'trim', 'body', 'transmission', 'vin', 'state', 
            'condition', 'odometer', 'color', 'interior', 'seller', 'mmr', 'sellingprice', 'saledate']

    with open(csv_file, 'r', encoding='utf-8') as csvFile:
        reader = csv.DictReader(csvFile)
        data_to_insert = []

        for index, each in enumerate(reader, start=1):
            try:
                row = {field: each[field].strip() if each[field] else None for field in header}

                # Validar y procesar la fila
                row = validate_and_process_row(row, index)

                # Convertir campos numéricos a enteros
                for field in ['year', 'odometer', 'mmr', 'sellingprice']:
                    row[field] = int(row[field]) if row[field] else 0  # Asignar 0 si es None

                # Agregar la fila procesada a la lista para insertar
                data_to_insert.append(( 
                    row['year'], row['maker'], row['model'], row['trim'], row['body'],
                    row['transmission'], row['vin'], row['state'], row['condition'],
                    row['odometer'], row['color'], row['interior'], row['seller'],
                    row['mmr'], row['sellingprice'], row['saledate']
                ))
            except Exception as e:
                logging.error(f"Error procesando la fila {index}: {e}")
                print(f"Error procesando la fila {index}: {e}")

        # Insertar datos en lote
        insert_query = """
        INSERT INTO cars (year, maker, model, trim, body, transmission, vin, state, condition, odometer, 
                        color, interior, seller, mmr, sellingprice, saledate)
        VALUES %s ON CONFLICT (vin) DO NOTHING;
        """
        execute_values(cursor, insert_query, data_to_insert)
        print(f"Se insertaron {len(data_to_insert)} filas en la base de datos.")

def export_to_s3(s3_client, processed_csv, bucket_name, s3_key):
    """Carga el archivo procesado a S3 con retries."""
    @retry(stop_max_attempt_number=3, wait_exponential_multiplier=1000)
    def upload():
        print("Subiendo archivo a S3...")
        s3_client.upload_file(processed_csv, bucket_name, s3_key)
    upload()
    print(f"Archivo '{s3_key}' subido exitosamente a S3.")

def cleanup_local_files():
    """Elimina archivos locales después de ser subidos a S3."""
    if os.path.isfile(PROCESSED_CSV_FILE):
        os.remove(PROCESSED_CSV_FILE)
        print(f"Archivo {PROCESSED_CSV_FILE} eliminado localmente.")

def main():
    # Verificar existencia del archivo CSV
    if not os.path.isfile(LOCAL_CSV_FILE):
        print(f"El archivo {LOCAL_CSV_FILE} no existe.")
        return

    # Conectar a PostgreSQL
    try:
        conn = connect_postgres()
        cursor = conn.cursor()
        create_table(cursor)

        # Procesar CSV e insertar datos
        process_csv_and_insert(cursor, LOCAL_CSV_FILE)
        conn.commit()

        # Exportar datos procesados a archivo CSV
        print("Exportando datos procesados a archivo CSV...")
        with open(PROCESSED_CSV_FILE, 'w', encoding='utf-8', newline='') as outputFile:
            writer = csv.writer(outputFile)
            writer.writerow(['ID', 'Year', 'Maker', 'Model', 'Trim', 'Body', 'Transmission', 'VIN', 'State', 
                            'Condition', 'Odometer', 'Color', 'Interior', 'Seller', 'MMR', 'Selling Price', 'Sale Date'])
            cursor.execute("SELECT * FROM cars")
            for row in cursor.fetchall():
                writer.writerow(row)

        # Subir CSV procesado a S3
        s3 = boto3.client('s3')
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        s3_key = f'processed_data_{timestamp}.csv'
        export_to_s3(s3, PROCESSED_CSV_FILE, S3_BUCKET_NAME, s3_key)

        # Limpiar archivos locales
        cleanup_local_files()

    except Exception as e:
        logging.error(f"Error general: {e}")
        print(f"Error general: {e}")
    finally:
        if cursor: cursor.close()
        if conn: conn.close()
        print("Conexión cerrada.")

if __name__ == "__main__":
    main()
