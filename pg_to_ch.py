import os
import time
import psycopg2
import clickhouse_connect
import json
import pandas as pd
import logging
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'/tmp/logs/migration_{timestamp}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class PGtoCHmigration:

    def __init__(self, config_path='/config/config.json'):
        with open(config_path, 'r') as config_file:
            self.config = json.load(config_file)
        
        self.pg_params = {
            'host'      : os.getenv('PG_HOST'),
            'database'  : os.getenv('PG_DB'),
            'user'      : os.getenv('PG_USER'),
            'password'  : os.getenv('PG_PASSWORD'),
            'options'   : f"-c search_path={os.getenv('PG_SCHEMA')}"
        }

        self.ch_params = {
            'host'      : os.getenv('CH_HOST'),
            'database'  : os.getenv('CH_DB'),
            'username'  : os.getenv('CH_USER'),
            'password'  : os.getenv('CH_PASSWORD')
        }

        self.dump_dir = '/tmp/dumps'
    
    def hc_postgres(self):
        while True:
            try:
                conn = psycopg2.connect(
                    host=self.pg_params['host'],
                    database=self.pg_params['database'],
                    user=self.pg_params['user'],
                    password=self.pg_params['password']
                )
                conn.close()
                logger.info("PostgreSQL connection successful")
                return
            except psycopg2.OperationalError as e:
                logger.warning(f"Waiting for PostgreSQL... Error: {str(e)}")
                time.sleep(2)

    def hc_clickhouse(self):
        while True:
            try:
                client = clickhouse_connect.get_client(
                    host=self.ch_params['host'],
                    database=self.ch_params['database'],
                    username=self.ch_params['username'],
                    password=self.ch_params['password']
                )
                client.close()
                logger.info("ClickHouse connection successful")
                return
            except Exception as e:
                logger.warning(f"Waiting for ClickHouse... Error: {str(e)}")
                time.sleep(2)

    def postgres_to_csv_copy(self, output_file,table):
        logger.info(f"Starting PostgreSQL export to {output_file}")
        start_time = time.time()
        
        conn = psycopg2.connect(
            host=self.pg_params['host'],
            database=self.pg_params['database'],
            user=self.pg_params['user'],
            password=self.pg_params['password'],
            options=self.pg_params['options']
        )
        try:
            with conn.cursor() as cursor:
                with open(output_file, 'w') as f:
                    cursor.copy_expert(
                        sql=f"""
                        COPY {table} 
                        TO STDOUT 
                        WITH (
                            FORMAT CSV,
                            HEADER true,
                            DELIMITER ',',
                            QUOTE '"',
                            ENCODING 'UTF8'
                        )
                        """,
                        file=f
                    )
            
            file_size = os.path.getsize(output_file) / (1024 * 1024)  # Size in MB
            elapsed_time = time.time() - start_time
            logger.info(f"Data exported to {output_file} (Size: {file_size:.2f} MB, Time: {elapsed_time:.2f} seconds)")
        
        except Exception as e:
            logger.error(f"Error during PostgreSQL export: {str(e)}")
            raise
        finally:
            conn.close()

    def csv_to_clickhouse(self, input_file, batch_size, ch_table):
        logger.info(f"Starting ClickHouse import from {input_file} with batch size {batch_size}")
        start_time = time.time()
        
        client = clickhouse_connect.get_client(
            host=self.ch_params['host'],
            database=self.ch_params['database'],
            username=self.ch_params['username'],
            password=self.ch_params['password']
        )
        
        try:
            total_rows = 0
            with open(input_file, 'r') as f:
                # Read header
                header = f.readline()
                
                while True:
                    chunk_start_time = time.time()
                    
                    # Read batch_size lines
                    lines = []
                    for _ in range(batch_size):
                        line = f.readline()
                        if not line:  # EOF
                            break
                        lines.append(line)
                    
                    if not lines:  # stop condition
                        break
                    
                    # Reconstruct CSV chunk with header
                    chunk_data = header + ''.join(lines)
                    
                    # Insert chunk
                    client.command(
                        f"INSERT INTO {ch_table} FORMAT CSV",
                        data=chunk_data
                    )
                    
                    chunk_time = time.time() - chunk_start_time
                    total_rows += len(lines)
                    logger.info(f"Inserted batch of {len(lines)} rows (Total: {total_rows}, Batch time: {chunk_time:.2f}s)")
                
            elapsed_time = time.time() - start_time
            logger.info(f"Import complete! Total {total_rows} rows imported to {ch_table} in {elapsed_time:.2f} seconds")
                
        except Exception as e:
            logger.error(f"Error during ClickHouse import: {str(e)}")
            raise
        finally:
            client.close()


    def BackupTable(self):
        migration_schema = self.config.get('migration_schema')     
        
        if migration_schema == "push":
            for csv_config in self.config.get('csv_files', []):
                csv_name = f"{self.dump_dir}/{csv_config.get('csv')}"
                table_name = csv_config.get('table')
                batchsize = csv_config.get('insert_batchsize')
                logger.info(f"{csv_name},{table_name}")
                try:
                    # Health checks
                    self.hc_clickhouse()
                    self.csv_to_clickhouse(csv_name, batchsize, table_name)
                    logger.info("Data ingestion completed successfully!")
                except Exception as e:
                    logger.error(f"Migration failed: {str(e)}")
                    raise   
        else:
            for table_config in self.config.get('tables', []):
                table_name = table_config.get('name')
                batchsize = table_config.get('insert_batchsize')
                try:
                    # Create CSV file
                    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                    direct_csv_file = f'{self.dump_dir}/dump_{timestamp}_{table_name}.csv'

                    # Health checks
                    if migration_schema == "pull":
                        self.hc_postgres()
                        self.postgres_to_csv_copy(direct_csv_file,table_name)
                        logger.info("Data extracted successfully!")
                    elif migration_schema == "full":
                        self.hc_postgres()
                        self.hc_clickhouse()    
                        # Execute migration
                        self.postgres_to_csv_copy(direct_csv_file,table_name)
                        self.csv_to_clickhouse(direct_csv_file, batchsize, table_name)
                        
                        logger.info("Migration completed successfully!")
                
                except Exception as e:
                    logger.error(f"Migration failed: {str(e)}")
                    raise   

def main():
    backup = PGtoCHmigration()
    backup.BackupTable()

if __name__ == "__main__":
    main()
