from flask import Blueprint, jsonify

import os

files_bp = Blueprint("files", __name__)



@files_bp.route('/listarProyectos', methods=['GET'])
def listar_proyectos():
    """
    Lista los proyectos disponibles (carpetas en el directorio de proyectos).
    ---
    tags:
      - Archivos
    responses:
      200:
      description: Lista de proyectos disponibles
      examples:
        application/json: {
        "proyectos": ["proyecto_1", "proyecto_2", "proyecto_3"]
        }
    """
    # Usar la variable de entorno CSVS_FOLDER si está definida; si no, usar la ruta por defecto
    directorio_proyectos = os.environ.get('CSVS_FOLDER')
   
    try:
        # Listar solo directorios (proyectos)
        proyectos = [d for d in os.listdir(directorio_proyectos) 
                    if os.path.isdir(os.path.join(directorio_proyectos, d))]
        print(f"Proyectos encontrados: {proyectos}")
        return jsonify({"proyectos": proyectos})
    except Exception as e:
        return jsonify({"error": f"Error al listar proyectos: {str(e)}"}), 500


@files_bp.route('/listarArchivosCSV', methods=['GET'])
def listar_archivos_csv():
    """
    Lista los archivos CSV por proyecto con estructura jerárquica.
    ---
    tags:
      - Archivos
    parameters:
      - name: proyecto
        in: query
        type: string
        required: false
        description: Nombre del proyecto específico. Si no se especifica, lista todos los proyectos.
    responses:
      200:
        description: Estructura de archivos CSV por proyecto
        examples:
          application/json: {
            "estructura": {
              "proyecto_1": ["archivo1.csv", "archivo2.csv"],
              "proyecto_2": ["datos.csv", "resultados.csv"]
            }
          }
      404:
        description: Proyecto no encontrado
        examples:
          application/json: {
            "error": "Proyecto 'nombre_proyecto' no encontrado"
          }
    """
    from flask import request
    directorio_proyectos = os.environ.get('CSVS_FOLDER')
    
    # directorio_proyectos = "C:/Users/Alienware/Desktop/Proyectos software/bajar_cargar_csv/datos"
    proyecto_especifico = request.args.get('proyecto')
    
    try:
        if proyecto_especifico:
            # Listar archivos de un proyecto específico
            ruta_proyecto = os.path.join(directorio_proyectos, proyecto_especifico)
            
            if not os.path.exists(ruta_proyecto) or not os.path.isdir(ruta_proyecto):
                return jsonify({"error": f"Proyecto '{proyecto_especifico}' no encontrado"}), 404
            
            archivos_csv = []
            for archivo in os.listdir(ruta_proyecto):
                if archivo.endswith(".csv"):
                    ruta_archivo = os.path.join(ruta_proyecto, archivo)
                    tamaño = os.path.getsize(ruta_archivo)
                    archivos_csv.append({
                        "nombre": archivo,
                        "tamaño_bytes": tamaño,
                        "tamaño_legible": format_file_size(tamaño)
                    })
            
            print(f"Archivos CSV en {proyecto_especifico}: {len(archivos_csv)} archivos")
            
            return jsonify({
                "proyecto": proyecto_especifico,
                "total_archivos": len(archivos_csv),
                "archivos": archivos_csv
            })
        else:
            # Listar estructura completa de todos los proyectos
            estructura = {}
            
            # Obtener todos los proyectos (directorios)
            proyectos = [d for d in os.listdir(directorio_proyectos) 
                        if os.path.isdir(os.path.join(directorio_proyectos, d))]
            
            # Para cada proyecto, obtener sus archivos CSV con información
            for proyecto in proyectos:
                ruta_proyecto = os.path.join(directorio_proyectos, proyecto)
                archivos_csv = []
                for archivo in os.listdir(ruta_proyecto):
                    if archivo.endswith(".csv"):
                        ruta_archivo = os.path.join(ruta_proyecto, archivo)
                        tamaño = os.path.getsize(ruta_archivo)
                        archivos_csv.append({
                            "nombre": archivo,
                            "tamaño_bytes": tamaño,
                            "tamaño_legible": format_file_size(tamaño)
                        })
                estructura[proyecto] = archivos_csv
            
            print(f"Estructura completa: {estructura}")
            return jsonify({"estructura": estructura})
            
    except Exception as e:
        return jsonify({"error": f"Error al listar archivos: {str(e)}"}), 500


@files_bp.route('/listarArchivosProyecto/<proyecto>', methods=['GET'])
def listar_archivos_proyecto(proyecto):
    """
    Lista todos los archivos (no solo CSV) de un proyecto específico.
    ---
    tags:
      - Archivos
    parameters:
      - name: proyecto
        in: path
        type: string
        required: true
        description: Nombre del proyecto
    responses:
      200:
        description: Lista de archivos del proyecto
        examples:
          application/json: {
            "proyecto": "proyecto_1",
            "archivos": [
              {
                "nombre": "archivo1.csv",
                "tipo": "csv",
                "tamaño": 1024
              }
            ]
          }
    """
    directorio_proyectos = os.environ.get('CSVS_FOLDER')

    # directorio_proyectos = "C:/Users/Alienware/Desktop/Proyectos software/bajar_cargar_csv/datos"
    ruta_proyecto = os.path.join(directorio_proyectos, proyecto)
    
    try:
        if not os.path.exists(ruta_proyecto) or not os.path.isdir(ruta_proyecto):
            return jsonify({"error": f"Proyecto '{proyecto}' no encontrado"}), 404
        
        archivos_info = []
        
        for archivo in os.listdir(ruta_proyecto):
            ruta_archivo = os.path.join(ruta_proyecto, archivo)
            
            if os.path.isfile(ruta_archivo):
                extension = os.path.splitext(archivo)[1].lower().lstrip('.')
                tamaño = os.path.getsize(ruta_archivo)
                
                archivos_info.append({
                    "nombre": archivo,
                    "tipo": extension if extension else "sin_extension",
                    "tamaño_bytes": tamaño,
                    "tamaño_legible": format_file_size(tamaño)
                })
        
        print(f"Archivos en proyecto {proyecto}: {len(archivos_info)} archivos")
        
        return jsonify({
            "proyecto": proyecto,
            "total_archivos": len(archivos_info),
            "archivos": archivos_info
        })
        
    except Exception as e:
        return jsonify({"error": f"Error al listar archivos del proyecto: {str(e)}"}), 500


@files_bp.route('/descargarArchivo/<proyecto>/<filename>', methods=['GET'])
def descargar_archivo(proyecto, filename):
    """
    Descarga un archivo específico de un proyecto.
    ---
    tags:
      - Archivos
    parameters:
      - name: proyecto
        in: path
        type: string
        required: true
        description: Nombre del proyecto
      - name: filename
        in: path
        type: string
        required: true
        description: Nombre del archivo a descargar
    responses:
      200:
        description: Archivo descargado exitosamente
        content:
          application/octet-stream:
            schema:
              type: string
              format: binary
      404:
        description: Proyecto o archivo no encontrado
        schema:
          type: object
          properties:
            error:
              type: string
              example: "Archivo 'nombre.csv' no encontrado en proyecto 'proyecto_1'"
      400:
        description: Nombre de archivo inválido por seguridad
        schema:
          type: object
          properties:
            error:
              type: string
              example: "Nombre de archivo inválido"
    """
    from flask import send_file, abort
    from werkzeug.utils import secure_filename
    import mimetypes
    directorio_proyectos = os.environ.get('CSVS_FOLDER')
    
    # directorio_proyectos = "C:/Users/Alienware/Desktop/Proyectos software/bajar_cargar_csv/datos"
    
    try:
        # Validar nombre de archivo por seguridad
        filename_seguro = secure_filename(filename)
        if not filename_seguro or filename_seguro != filename:
            return jsonify({"error": "Nombre de archivo inválido"}), 400
        
        # Construir ruta completa
        ruta_proyecto = os.path.join(directorio_proyectos, proyecto)
        ruta_archivo = os.path.join(ruta_proyecto, filename_seguro)
        
        # Validar que el proyecto existe
        if not os.path.exists(ruta_proyecto) or not os.path.isdir(ruta_proyecto):
            return jsonify({"error": f"Proyecto '{proyecto}' no encontrado"}), 404
        
        # Validar que el archivo existe
        if not os.path.exists(ruta_archivo) or not os.path.isfile(ruta_archivo):
            return jsonify({"error": f"Archivo '{filename}' no encontrado en proyecto '{proyecto}'"}), 404
        
        # Validar que el archivo está dentro del directorio del proyecto (seguridad)
        ruta_real = os.path.realpath(ruta_archivo)
        directorio_real = os.path.realpath(ruta_proyecto)
        if not ruta_real.startswith(directorio_real):
            return jsonify({"error": "Acceso denegado"}), 403
        
        # Determinar tipo MIME del archivo
        tipo_mime, _ = mimetypes.guess_type(ruta_archivo)
        if not tipo_mime:
            tipo_mime = 'application/octet-stream'
        
        print(f"Descargando archivo: {ruta_archivo} (tipo: {tipo_mime})")
        
        # Enviar el archivo
        return send_file(
            ruta_archivo,
            mimetype=tipo_mime,
            as_attachment=True,
            download_name=filename_seguro
        )
        
    except Exception as e:
        print(f"Error al descargar archivo: {str(e)}")
        return jsonify({"error": f"Error interno al descargar archivo: {str(e)}"}), 500


@files_bp.route('/descargarArchivo', methods=['GET'])
def descargar_archivo_query():
    """
    Descarga un archivo usando parámetros de query (alternativa).
    ---
    tags:
      - Archivos
    parameters:
      - name: proyecto
        in: query
        type: string
        required: true
        description: Nombre del proyecto
      - name: archivo
        in: query
        type: string
        required: true
        description: Nombre del archivo a descargar
    responses:
      200:
        description: Archivo descargado exitosamente
      400:
        description: Faltan parámetros requeridos
      404:
        description: Proyecto o archivo no encontrado
    """
    from flask import request
    
    proyecto = request.args.get('proyecto')
    archivo = request.args.get('archivo')
    
    if not proyecto or not archivo:
        return jsonify({"error": "Los parámetros 'proyecto' y 'archivo' son requeridos"}), 400
    
    # Redirigir a la función principal
    return descargar_archivo(proyecto, archivo)


def format_file_size(size_bytes):
    """Convierte bytes a formato legible (KB, MB, GB)"""
    if size_bytes == 0:
        return "0 B"
    
    size_names = ["B", "KB", "MB", "GB", "TB"]
    import math
    i = int(math.floor(math.log(size_bytes, 1024)))
    p = math.pow(1024, i)
    s = round(size_bytes / p, 2)
    return f"{s} {size_names[i]}"

