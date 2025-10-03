module.exports = {
  apps: [
    {
      name: "api_sensores", // Nombre de la aplicación
      script: "/var/www/api_sensores/venv/bin/gunicorn", // Ruta completa de Gunicorn
      args: "--workers 4 --bind 0.0.0.0:8084 app:app --timeout 60", // Argumentos de ejecución para Gunicorn
      cwd: "/var/www/api_sensores", // Carpeta del proyecto
      env: {
        FLASK_APP: "app.py", // Archivo principal de tu aplicación Flask
        FLASK_ENV: "production", // Configuración del entorno (producción)
      },
      interpreter: "/var/www/api_sensores/venv/bin/python", // Interprete Python dentro del entorno virtual
    },
  ],
};
