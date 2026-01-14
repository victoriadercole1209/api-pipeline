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


#import os : Este módulo viene con Python. Sirve para: leer variables del sistema, acceder al entorno . Sin os, no se pueden leer variables de entorno.
#from dotenv import load_dotenv: Esto viene de python-dotenv (que ya se instalo en requirements).Sirve para: “leer el archivo .env y cargarlo en memoria” . Sin esto: Python NO ve el .env
#load_dotenv() : Busca un archivo .env, Lee sus variables , Las pone disponibles para Python . Después de esto, Python “sabe” que existe API_TOKEN.
# os.getenv('API_TOKEN') : Esto pregunta “¿Existe una variable de entorno llamada API_TOKEN?” Si existe → devuelve su valor ; Si no → devuelve None