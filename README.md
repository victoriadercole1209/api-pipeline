# API Pipeline - E-commerce Data

Pipeline **ETL (Extract, Transform, Load)** desarrollado en Python que consume datos de una API de e-commerce, los transforma y los guarda en formato **Parquet**, tanto de forma consolidada como particionada por fecha.

Este proyecto fue realizado como parte del **Bootcamp de Data Engineering – Ian Saura** y sigue buenas prácticas profesionales: uso de variables de entorno, manejo de errores, logging y estructura modular.

---

## 📌 Descripción del Pipeline

El pipeline realiza los siguientes pasos:

1. **Extract**: Consume datos desde una API REST protegida por token.
2. **Transform**: Limpia y transforma los datos usando pandas, agregando columnas derivadas y validaciones.
3. **Load**: Guarda los datos en formato Parquet:

   * Un archivo consolidado (todo junto)
   * Datos particionados por año y mes para optimizar consultas

---

## ⚙️ Setup

### 1️⃣ Clonar el repositorio

```bash
git clone <url-del-repositorio>
cd api-pipeline
```

### 2️⃣ Crear el archivo `.env`

Crear un archivo `.env` en la raíz del proyecto con el siguiente contenido:

```
API_TOKEN=tu_token_aqui
API_BASE_URL=https://iansaura.com/api
```

⚠️ **Importante**: El archivo `.env` no debe subirse a GitHub. Está incluido en `.gitignore`.

---

### 3️⃣ Instalar dependencias

```bash
pip install -r requirements.txt
```

---

## ▶️ Uso

Para ejecutar el pipeline completo:

```bash
python main.py
```

El pipeline incluye manejo de errores y logging para facilitar el monitoreo y debugging.

---

## 📂 Output

Los datos generados se guardan en la carpeta `output/`:

```
output/
├── orders/
│   └── order_year=2024/
│       ├── order_month=2024-01/
│       │   └── data.parquet
│       ├── order_month=2024-02/
│       │   └── data.parquet
│       └── ...
└── orders_all.parquet
```

### 📁 Tipos de salida

* **orders_all.parquet**: archivo consolidado con todas las órdenes (útil para análisis exploratorio y debugging).
* **orders/**: datos particionados por año y mes (pensado para consultas eficientes en producción).

---

## 🐳 Docker

Este proyecto puede ejecutarse dentro de un container Docker para garantizar un entorno reproducible.

### Build de la imagen
Desde la raíz del proyecto:

```bash
docker build -t api-pipeline .
```

---

## 🛠️ Tecnologías utilizadas

* Python 3
* requests
* pandas
* pyarrow
* python-dotenv

---

## 👩‍💻 Autora

María Victoria D'Ercole
