# API Pipeline - E-commerce Data

Pipeline **ETL (Extract, Transform, Load)** desarrollado en Python que consume datos de una API de e-commerce, los transforma y los guarda en formato **Parquet**, tanto de forma consolidada como particionada por fecha.

Este proyecto fue realizado como parte del **Bootcamp de Data Engineering – Ian Saura** y sigue buenas prácticas profesionales: uso de variables de entorno, manejo de errores, logging y estructura modular.

---

## 📌 Descripción del Pipeline

El pipeline realiza los siguientes pasos:

1. **Extract**: Obtiene datos desde la API pública de Ian Saura utilizando autenticación mediante **email y API key**.
2. **Transform**: Limpia y transforma los datos usando pandas, agregando columnas derivadas y validaciones.
3. **Load**: Guarda los datos en formato Parquet:
   - Un archivo consolidado con todas las órdenes.
   - Datos particionados por año y mes para optimizar consultas.

---

## 🔐 Autenticación

La API utilizada requiere autenticación mediante dos parámetros:

- `email`
- `key` (API Key personal)

Estos valores se envían como parámetros en la request HTTP y se configuran mediante variables de entorno.

---

## ⚙️ Setup

### 1️⃣ Clonar el repositorio

```bash
git clone <url-del-repositorio>
cd api-pipeline
```

---

### 2️⃣ Crear el archivo .env

Crear un archivo .env en la raíz del proyecto con el siguiente contenido:

```
EMAIL=tu_email_registrado
KEY=tu_api_key
API_BASE_URL=https://iansaura.com/api
```

⚠️ Importante: El archivo .env no debe subirse a GitHub. Está incluido en .gitignore.

---

### 3️⃣ Instalar dependencias

```
pip install -r requirements.txt
```

---

### ▶️ Uso

Para ejecutar el pipeline completo:

```
python main.py
```

El pipeline incluye:

* Manejo de errores HTTP
* Logging detallado
* Reintentos automáticos ante fallas de conexión
* Transformaciones de datos con pandas

---

### 🔄 Transformaciones aplicadas

Durante la etapa de transformación se realizan, entre otras, las siguientes operaciones:

* Conversión de fechas (order_date)
* Conversión de montos numéricos (total_amount)
* Generación de nuevas columnas:
         * order_month
         * order_year
         * is_high_value
         * day_of_week

También se validan valores inválidos y se registran advertencias en los logs.

---

### 📂 Output

Los datos generados se guardan en la carpeta output/:

output/
├── orders/
│   └── order_year=2024/
│       ├── order_month=2024-01/
│       │   └── data.parquet
│       ├── order_month=2024-02/
│       │   └── data.parquet
│       └── ...
└── orders_all.parquet

📁 Tipos de salida

* orders_all.parquet: archivo consolidado con todas las órdenes (útil para análisis exploratorio y debugging).

* orders/: datos particionados por año y mes (pensado para consultas eficientes en producción).

---

### 🐳 Docker

Este proyecto puede ejecutarse dentro de un container Docker para garantizar un entorno reproducible.

## Build de la imagen

Desde la raíz del proyecto:

```
docker build -t api-pipeline .
```

## Ejecutar el container
```
docker run --env-file .env api-pipeline
```

---

### 🛠️ Tecnologías utilizadas

* Python 3
* requests
* pandas
* pyarrow
* python-dotenv

---

### 👩‍💻 Autora

María Victoria D'Ercole