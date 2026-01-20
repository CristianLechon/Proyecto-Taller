# Usamos una imagen base de Python (muy ligera, sin Hadoop ni Java)
FROM python:3.9-slim

# Definimos la carpeta de trabajo dentro del contenedor
WORKDIR /app

# Copiamos los archivos de tu carpeta (app.py, templates/, requirements.txt) al contenedor
COPY . /app

# Instalamos las librerías necesarias (Flask y PyMongo)
# (Asegúrate de haber creado el archivo requirements.txt)
RUN pip install --no-cache-dir -r requirements.txt

# Exponemos el puerto 5000 para entrar por la web
EXPOSE 5000

# Comando para iniciar tu servidor web
CMD ["python", "app.py"]