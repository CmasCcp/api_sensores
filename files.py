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
          # Listar archivos de un proyecto específico agrupados por dispositivo
          ruta_proyecto = os.path.join(directorio_proyectos, proyecto_especifico)

          if not os.path.exists(ruta_proyecto) or not os.path.isdir(ruta_proyecto):
            return jsonify({"error": f"Proyecto '{proyecto_especifico}' no encontrado"}), 404

          dispositivos = [d for d in os.listdir(ruta_proyecto)
                  if os.path.isdir(os.path.join(ruta_proyecto, d))]

          resultado = {}

          # Para cada dispositivo (carpeta), buscar recursivamente archivos CSV en sus carpetas de fechas
          for dispositivo in dispositivos:
            ruta_dispositivo = os.path.join(ruta_proyecto, dispositivo)
            archivos_csv = []
            for root, dirs, files in os.walk(ruta_dispositivo):
              for archivo in files:
                if archivo.lower().endswith('.csv'):
                  ruta_archivo = os.path.join(root, archivo)
                  tamaño = os.path.getsize(ruta_archivo)
                  # ruta relativa respecto a la carpeta del dispositivo (incluye la carpeta de fecha)
                  ruta_relativa = os.path.relpath(ruta_archivo, ruta_dispositivo)
                  archivos_csv.append({
                    "nombre": archivo,
                    "ruta_relativa": ruta_relativa,
                    "tamaño_bytes": tamaño,
                    "tamaño_legible": format_file_size(tamaño)
                  })
            resultado[dispositivo] = archivos_csv

          # También comprobar si hay CSV directamente en la raíz del proyecto
          archivos_en_raiz = []
          for f in os.listdir(ruta_proyecto):
            ruta_f = os.path.join(ruta_proyecto, f)
            if os.path.isfile(ruta_f) and f.lower().endswith('.csv'):
              tamaño = os.path.getsize(ruta_f)
              archivos_en_raiz.append({
                "nombre": f,
                "ruta_relativa": f,
                "tamaño_bytes": tamaño,
                "tamaño_legible": format_file_size(tamaño)
              })
          if archivos_en_raiz:
            resultado["_raiz_proyecto"] = archivos_en_raiz

          total = sum(len(v) for v in resultado.values())
          print(f"Archivos CSV en {proyecto_especifico}: {total} archivos (agrupados por dispositivo)")

          return jsonify({
            "proyecto": proyecto_especifico,
            "total_archivos": total,
            "dispositivos": resultado
          })
        else:
            # Listar estructura completa de todos los proyectos
            estructura = {}
            
            # Obtener todos los proyectos (directorios)
            proyectos = [d for d in os.listdir(directorio_proyectos) 
                        if os.path.isdir(os.path.join(directorio_proyectos, d))]
            
            # Para cada proyecto, agrupar CSVs por dispositivo (cada proyecto tiene carpetas de dispositivos)
            for proyecto in proyectos:
              ruta_proyecto = os.path.join(directorio_proyectos, proyecto)
              dispositivos = [d for d in os.listdir(ruta_proyecto)
                      if os.path.isdir(os.path.join(ruta_proyecto, d))]

              resultado = {}
              for dispositivo in dispositivos:
                ruta_dispositivo = os.path.join(ruta_proyecto, dispositivo)
                archivos_csv = []
                for root, dirs, files in os.walk(ruta_dispositivo):
                  for archivo in files:
                    if archivo.lower().endswith('.csv'):
                      ruta_archivo = os.path.join(root, archivo)
                      tamaño = os.path.getsize(ruta_archivo)
                      ruta_relativa = os.path.relpath(ruta_archivo, ruta_dispositivo)
                      archivos_csv.append({
                        "nombre": archivo,
                        "ruta_relativa": ruta_relativa,
                        "tamaño_bytes": tamaño,
                        "tamaño_legible": format_file_size(tamaño)
                      })
                resultado[dispositivo] = archivos_csv

              # CSVs en la raíz del proyecto (si los hay)
              archivos_en_raiz = []
              for f in os.listdir(ruta_proyecto):
                ruta_f = os.path.join(ruta_proyecto, f)
                if os.path.isfile(ruta_f) and f.lower().endswith('.csv'):
                  tamaño = os.path.getsize(ruta_f)
                  archivos_en_raiz.append({
                    "nombre": f,
                    "ruta_relativa": f,
                    "tamaño_bytes": tamaño,
                    "tamaño_legible": format_file_size(tamaño)
                  })
              if archivos_en_raiz:
                resultado["_raiz_proyecto"] = archivos_en_raiz

              estructura[proyecto] = resultado
            
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


@files_bp.route('/descargarArchivoCSV', methods=['GET'])
def descargar_por_nombre():
  """
  Descarga un archivo buscando por su nombre en todo el directorio de proyectos.
  Parámetros de query:
    - archivo: nombre del archivo (requerido)
  Busca de forma recursiva dentro de `CSVS_FOLDER` y devuelve el primer match.
  """
  from flask import request, send_file
  import mimetypes

  directorio_proyectos = os.environ.get('CSVS_FOLDER')
  archivo = request.args.get('archivo')

  if not archivo:
    return jsonify({"error": "El parámetro 'archivo' es requerido"}), 400

  # Asegurar que solo recibimos un nombre de archivo (evitar paths)
  nombre_buscar = os.path.basename(archivo)

  try:
    for root, dirs, files in os.walk(directorio_proyectos):
      for f in files:
        # comparación case-insensitive para mayor flexibilidad
        if f == nombre_buscar or f.lower() == nombre_buscar.lower():
          ruta_archivo = os.path.join(root, f)

          # seguridad: comprobar que está dentro del dir base
          ruta_real = os.path.realpath(ruta_archivo)
          directorio_real = os.path.realpath(directorio_proyectos)
          if not ruta_real.startswith(directorio_real):
            continue

          tipo_mime, _ = mimetypes.guess_type(ruta_archivo)
          if not tipo_mime:
            tipo_mime = 'application/octet-stream'

          return send_file(
            ruta_archivo,
            mimetype=tipo_mime,
            as_attachment=True,
            download_name=f
          )

    return jsonify({"error": f"Archivo '{archivo}' no encontrado"}), 404

  except Exception as e:
    return jsonify({"error": f"Error al buscar/descargar archivo: {str(e)}"}), 500

