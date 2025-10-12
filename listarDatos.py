import mysql.connector
import decimal
from datetime import datetime, date

def listar_datos(params: dict, db_config: dict, ALLOWED_TABLES, generar_csv=None):
    """
    Función pura para obtener datos de la tabla solicitada.

    params: dict con los filtros y opciones (tabla, limite, offset, formato, orden, primarykey, y filtros)
    db_config: dict para mysql.connector.connect(**db_config)
    ALLOWED_TABLES: lista o set con tablas permitidas
    generar_csv: función opcional que recibe la lista de dicts y devuelve texto CSV

    Retorna un diccionario (no Response HTTP):
      {'status':'success','data':{'tableData': [...], 'tabla': tabla, 'totalCount': n}}
    En caso de error retorna {'status':'fail','error': 'mensaje'}
    """

    tabla = params.get('tabla')
    if not tabla:
        return {'status': 'fail', 'error': "El parámetro 'tabla' es obligatorio"}

    if tabla not in ALLOWED_TABLES:
        return {'status': 'fail', 'error': 'Tabla no permitida'}

    # parse limit/offset
    limit = params.get('limite')
    try:
        limit = int(limit) if limit is not None and str(limit) != '' else None
    except Exception:
        return {'status': 'fail', 'error': "Parámetro 'limite' inválido"}

    try:
        offset = int(params.get('offset', 0))
    except Exception:
        offset = 0

    formato = params.get('formato', 'json')
    orden = params.get('orden', 'desc')
    primarykey = params.get('primarykey', '')

    # construir filtros: aceptar strings "a,b" o listas
    not_primary_keys = {'tabla', 'limite', 'offset', 'formato', 'orden', 'primarykey'}
    filtered_args = {}
    for k, v in params.items():
        if k in not_primary_keys:
            continue
        if v is None or v == '':
            continue
        if isinstance(v, (list, tuple)):
            filtered_args[k] = [str(x) for x in v]
        else:
            filtered_args[k] = [s for s in str(v).split(',') if s != '']

    where_clauses = []
    sql_params = []
    for key, values in filtered_args.items():
        if not values:
            continue
        or_conditions = " OR ".join([f"{key}=%s" for _ in values])
        where_clauses.append(f"({or_conditions})")
        sql_params.extend(values)

    where_clause = ' AND '.join(where_clauses)
    where_clause = f"WHERE {where_clause}" if where_clause else ""

    conn = None
    cursor = None
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()

        sql_query = f"SELECT * FROM {tabla} {where_clause}"
        if limit is not None:
            sql_query += " LIMIT %s OFFSET %s"
            sql_params.extend([int(limit), int(offset)])

        cursor.execute(sql_query, sql_params)
        filas = cursor.fetchall()

        respuesta = []
        colnames = cursor.column_names
        for fila in filas:
            datos_dict = {key: value for key, value in zip(colnames, fila)}
            for key, value in list(datos_dict.items()):
                if isinstance(value, decimal.Decimal):
                    datos_dict[key] = float(value)
                elif isinstance(value, (datetime, date)):
                    datos_dict[key] = value.isoformat()
            respuesta.append(datos_dict)

        total_count = len(respuesta)

        if formato == 'json':
            return {'status': 'success', 'data': {'tableData': respuesta, 'tabla': tabla, 'totalCount': total_count}}
        elif formato == 'csv':
            if callable(generar_csv):
                csv_text = generar_csv(respuesta)
                return {'status': 'success', 'csv': csv_text, 'tabla': tabla, 'totalCount': total_count}
            else:
                return {'status': 'fail', 'error': "Formato 'csv' solicitado pero no se dio 'generar_csv'"}
        else:
            return {'status': 'fail', 'error': f"Formato '{formato}' no soportado. Use 'json' o 'csv'."}

    except mysql.connector.Error as e:
        return {'status': 'fail', 'error': f"Error al conectarse a la base de datos {e}"}
    except Exception as e:
        return {'status': 'fail', 'error': f"Error desconocido: {e}"}
    finally:
        try:
            if cursor is not None:
                cursor.close()
            if conn is not None and conn.is_connected():
                conn.close()
        except Exception:
            pass