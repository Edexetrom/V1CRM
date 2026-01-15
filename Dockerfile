# Usar una imagen base de Python ligera
FROM python:3.10-slim

# Establecer el directorio de trabajo en el contenedor
WORKDIR /app

# Instalar dependencias del sistema necesarias
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copiar el archivo de requerimientos e instalar dependencias
# Nota: Asegúrate de tener un archivo requirements.txt en tu carpeta
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el código fuente del backend
COPY app.py .
COPY data_handler.py .

# Exponer el puerto interno de Gunicorn
EXPOSE 5000

# Ejecutar la aplicación con Gunicorn (3 workers para mayor estabilidad)
CMD ["gunicorn", "--workers", "3", "--bind", "0.0.0.0:5000", "app:app"]