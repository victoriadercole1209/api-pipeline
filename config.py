

import os
from dotenv import load_dotenv

load_dotenv()


API_BASE_URL = os.getenv('API_BASE_URL')
EMAIL = os.getenv('EMAIL')
KEY = os.getenv('KEY')

if not KEY:
    raise ValueError("KEY not configured. Create a .env file")
