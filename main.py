# import libraries
import requests 
import logging 
import time 
import pandas as pd
import os
from requests.exceptions import RequestException, Timeout, HTTPError
from config import API_BASE_URL, EMAIL, KEY



logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def fetch_data(dataset_type: str = 'ecommerce', rows: int = 1000) -> dict:  #fetch data with retry nueva
    """Obtiene datos de la API."""
    url = f"{API_BASE_URL}/datasets.php"
    #params = {  
       # 'type': dataset_type,
      #  'rows': rows,
     #   'token': API_TOKEN
    #}
    params = {
    'email': EMAIL,
    'key': KEY,
    'type': dataset_type,
    'rows': rows,
}

    logger.info(f"Fetching {rows} rows of {dataset_type} data...")
    # Llamada HTTP a la API
    response = requests.get(url, params = params, timeout = 30)  # timeout = 30 son los segundos máximos de espera
    # Lanza excepción si el status code es 4xx o 5xx:
    response.raise_for_status()  
    #parseo a json
    data = response.json() 
    #Log de control
    logger.info(f"Received {len(data.get('tables', {}).get('orders', []))} orders")
    return data

def fetch_data_with_retry(dataset_type="ecommerce", rows=1000, max_retries=3, backoff_factor=1):

    for attempt in range(max_retries):
        try:
            return fetch_data(dataset_type, rows)

        except Exception as e:
            logger.warning(f"Error de conexión: {e}")

            if attempt < max_retries - 1:
                sleep_time = backoff_factor * (2 ** attempt)
                logger.info(f"Reintentando en {sleep_time} segundos...")
                time.sleep(sleep_time)
            else:
                raise Exception(f"Falló después de {max_retries} intentos")





def transform_data(raw_data: dict) -> pd.DataFrame:
    """Transforma y enriquece los datos."""
    logger.info("Transformando datos...")
    
    orders = raw_data.get('tables', {}).get('orders', [])
    df = pd.DataFrame(orders)
    
    if df.empty:
        logger.warning("No hay datos para procesar")
        return df

    logger.info(f"Columnas recibidas: {df.columns.tolist()}")
    print("COLUMNAS REALES:", df.columns.tolist())

    # Convertir tipos de datos
    df['order_date'] = pd.to_datetime(df['order_date'])

    # 👇 AQUÍ ESTÁ EL CAMBIO CLAVE
    df['total'] = pd.to_numeric(df['total_amount'], errors='coerce')

    # Agregar campos calculados
    df['order_month'] = df['order_date'].dt.to_period('M').astype(str)
    df['order_year'] = df['order_date'].dt.year
    df['is_high_value'] = df['total'] > 100
    df['day_of_week'] = df['order_date'].dt.day_name()
    
    # Validaciones
    invalid_totals = df['total'].isna().sum()
    if invalid_totals > 0:
        logger.warning(f"{invalid_totals} órdenes con total inválido")
    
    logger.info(f"Transformadas {len(df)} órdenes")
    return df


#guardo particionando por fecha lo que hace que las queries sean más rápidas: solo lee los meses que necesito.
def save_data(df: pd.DataFrame, output_dir: str = 'output'):
    """Guarda datos particionados por mes."""
    logger.info(f"Guardando datos en {output_dir}/...")
    
    os.makedirs(output_dir, exist_ok=True)
    
    # Guarda particionado por mes
    df.to_parquet(
        f'{output_dir}/orders',
        partition_cols=['order_year', 'order_month'],
        index=False
    )
    
    # También guarda un archivo consolidado
    df.to_parquet(f'{output_dir}/orders_all.parquet', index=False)
    
    # Estadísticas
    logger.info(f"Guardadas {len(df)} órdenes")
    logger.info(f"Particiones: {df['order_month'].nunique()} meses")



def main():
    """Pipeline principal."""
    logger.info("=" * 50)
    logger.info("API Pipeline - Iniciando")
    logger.info("=" * 50)
    
    try:
        # Extract
        raw_data = fetch_data_with_retry(rows=5000)
        
        # Transform
        df = transform_data(raw_data)
        
        if df.empty:
            logger.error("No hay datos para guardar")
            return
        
        # Load
        save_data(df)
        
        logger.info("=" * 50)
        logger.info("Pipeline completado exitosamente!")
        logger.info("=" * 50)
        
    except Exception as e:
        logger.error(f"Pipeline falló: {e}")
        raise

if __name__ == "__main__":
    main()