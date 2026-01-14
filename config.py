import os
from dotenv import load_dotenv

# Carga las variables del archivo .env al entorno
load_dotenv()

# Lee las variables de entorno
API_TOKEN = os.getenv('API_TOKEN')
API_BASE_URL = os.getenv('API_BASE_URL','https://iansaura.com/api') # Si no existe API_BASE_URL, usá este valor por defecto

# Validación de seguridad
if not API_TOKEN:
    raise ValueError("API_TOKEN no configurado. Creá un archivo .env")

