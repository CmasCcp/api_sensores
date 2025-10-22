from dotenv import load_dotenv

from flask import Flask, jsonify, request, send_from_directory, Response, stream_with_context
from flask_cors import CORS
from flasgger import Swagger

import mysql.connector
import pandas as pd
import csv, decimal, io, os, json
from datetime import datetime, date

from werkzeug.utils import secure_filename
from openpyxl import Workbook
from openpyxl.worksheet.table import Table, TableStyleInfo
from openpyxl.styles import PatternFill

from insertarMedicionV2 import insertar_medicion_bp as insertar_medicion_v2_bp
from flask_socketio import SocketIO, emit
from app import f_numero_variables_por_proyecto, f_numero_mediciones_por_dispositivo, build_csv, build_excel

load_dotenv()

config = {
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "database": os.getenv("DB_NAME"),
    "port": int(os.getenv("DB_PORT", 3306)),  # Valor por defecto: 3306
}

def listar_datos_estructurados_v2(args):

    tabla = "datos"  # args.get('tabla')  # Nombre de la tabla como parámetro
        
    limit_param = args.get('limite')
    if limit_param is not None:
      limit = int(limit_param)
    else:
      limit = None

    offset = int(args.get('offset', 0))
    formato = args.get('formato', 'json')

    print(offset)

    fecha_inicio = args.get('fecha_inicio')
    fecha_fin = args.get('fecha_fin')

    # args_dict = request.args.to_dict()
    not_primary_keys = ['tabla', 'limite', 'offset', 'formato', 'fecha_inicio', 'fecha_fin']
    # Filtrar los argumentos relevantes
    filtered_args = {k: v for k, v in args.items() if k not in not_primary_keys}

    where_clauses = []
    params = []

    # Rango de fechas
    if fecha_inicio:
        where_clauses.append("(d.fecha >= %s)")
        params.append(fecha_inicio)
    if fecha_fin:
        where_clauses.append("(d.fecha <= %s)")
        params.append(fecha_fin)

    # Mejor manejo de argumentos: acepta listas y valores únicos
    for key, value in filtered_args.items():
        if isinstance(value, list):
            or_conditions = " OR ".join([f"{key}=%s" for _ in value])
            where_clauses.append(f"({or_conditions})")
            params.extend(value)
        else:
            where_clauses.append(f"({key}=%s)")
            params.append(value)

    where_clause = ' AND '.join(where_clauses)
    where_clause = f"WHERE {where_clause}" if where_clause else ""

    
    try:
        conn = mysql.connector.connect(**config)
        cursor = conn.cursor()

        # Adaptar el valor de limit y offset para la tabla estructurada
        id_proyecto = args.get("disp.id_proyecto")
        if id_proyecto is not None:
            result_func = f_numero_variables_por_proyecto(id_proyecto)
            if result_func is not None and isinstance(result_func, dict):
                num_dispositivos = result_func.get("num_dispositivos", 0)
                num_variables_proyecto = result_func.get("num_variables_proyecto", 0)
                num_variables_dispositivo = int(result_func.get("num_variables_dispositivo", 0))
                limit_adaptado = (limit * num_variables_dispositivo) if (limit is not None and limit > 0 and num_variables_dispositivo > 0) else 0
                offset_adaptado = offset * num_variables_dispositivo if offset > 0 and num_variables_dispositivo > 0 else 0
            else:
                print("Error: result_func es None o no es un diccionario")
                num_dispositivos = None
                num_variables_proyecto = None
                num_variables_dispositivo = None
                limit_adaptado = limit if limit is not None else 0
                offset_adaptado = 0
        else:
            print(offset)
            print("DEBUGUEANDOO", id_proyecto)
            num_dispositivos = None
            num_variables_proyecto = None
            num_variables_dispositivo = None
            limit_adaptado = limit if limit is not None else 0
            offset_adaptado = 0
        sql_query = f"""
            SELECT
                d.id_dato,
                d.fecha,
                d.id_sesion,
                d.valor,
                d.fecha_insercion,
                CONCAT(st.modelo, ' [', v.descripcion, ' (', v.unidad, ')]') AS unidad_medida,
                s.descripcion AS sesion_descripcion,
                s.fecha_inicio,
                s.ubicacion,
                disp.id_proyecto,
                disp.codigo_interno,
                disp.descripcion AS dispositivo_descripcion
            FROM
                sensores_dev.datos AS d
            LEFT JOIN
                sensores_dev.variables AS v ON d.id_variable = v.id_variable
            LEFT JOIN
                sensores_dev.sesiones AS s ON d.id_sesion = s.id_sesion
            LEFT JOIN
                sensores_dev.sensores AS sens ON d.id_sensor = sens.id_sensor
            LEFT JOIN
                sensores_dev.sensores_tipo AS st ON sens.id_sensor_tipo = st.id_sensor_tipo
            LEFT JOIN
                sensores_dev.sensores_en_dispositivo AS sed ON sens.id_sensor = sed.id_sensor
            LEFT JOIN
                sensores_dev.dispositivos AS disp ON sed.id_dispositivo = disp.id_dispositivo
            {where_clause}
            ORDER BY d.fecha DESC
        """

        params_sql = params.copy()
        if limit is not None:
          # Solo agrega LIMIT y OFFSET si se entregó el parámetro limite
          sql_query += " LIMIT %s OFFSET %s"
          params_sql.extend([limit_adaptado, offset_adaptado])

        cursor.execute(sql_query, params_sql)
        print("Consulta SQL:", sql_query, params_sql)

        filas = cursor.fetchall()
        # Manejo robusto de resultados y pivot
        import pandas as pd
        respuesta = []
        colnames = [desc[0] for desc in cursor.description]
        for fila in filas:
            datos_dict = {key: value for key, value in zip(colnames, fila)}
            respuesta.append(datos_dict)
        try:
            df = pd.DataFrame(respuesta)
            if not df.empty:
                df = df.fillna("")
                df_pivoted = df.pivot_table(
                    index=["fecha","fecha_insercion", "id_sesion", "sesion_descripcion", "fecha_inicio", "ubicacion", "id_proyecto", "codigo_interno", "dispositivo_descripcion"],
                    columns="unidad_medida",
                    values="valor",
                    aggfunc=list
                ).reset_index()
            else:
                df_pivoted = pd.DataFrame()
        except Exception as e:
            print(f"Error en pivot: {e}")
            df_pivoted = pd.DataFrame()

        # Fallback para total_count
        try:
            total_count = len(df_pivoted)
        except Exception:
            total_count = len(respuesta)

        # Formato de respuesta
        def clean_for_json(obj):
            if obj is None:
                return ""
            if isinstance(obj, (pd.Timestamp, datetime, date, decimal.Decimal)):
                return str(obj)
            if isinstance(obj, dict):
                return {k: clean_for_json(v) for k, v in obj.items()}
            if isinstance(obj, list):
                return [clean_for_json(v) for v in obj]
            return str(obj)

        def flatten_single_list_values(row):
            for k, v in row.items():
                if isinstance(v, list) and len(v) == 1:
                    row[k] = v[0]
            return row

        table_data = df_pivoted.to_dict(orient="records") if not df_pivoted.empty else respuesta
        table_data = [flatten_single_list_values(clean_for_json(row)) for row in table_data]

        res = {
            'status': 'success',
            'data': {
                'tableData': table_data,
                'tabla': tabla,
                'totalCount': total_count
            }
        }

        if formato == 'json':
            return res
        elif formato == 'csv':
            return build_csv(df_pivoted)
        elif formato == 'xlsx':
            return build_excel(df_pivoted)
        else:
            return {'status': 'fail', 'error': f"Formato '{formato}' no soportado. Use 'json', 'csv' o 'xlsx'."}


    except mysql.connector.Error as e:
        mensaje_error = f"Error al conectarse a la base de datos {e}"
        print(mensaje_error)
        return jsonify({'status': 'fail', 'error': mensaje_error}), 500

    except Exception as e:
        mensaje_error = f"Error desconocido: {e}"
        print(mensaje_error)
        return jsonify({'status': 'fail', 'error': mensaje_error}), 500

    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()
