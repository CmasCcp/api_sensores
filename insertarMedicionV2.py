from dotenv import load_dotenv
from flask import Blueprint,request, jsonify, current_app
import mysql.connector
from datetime import datetime
import os



load_dotenv()
config = {
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "database": os.getenv("DB_NAME"),
    "port": int(os.getenv("DB_PORT", 3306)),  # Valor por defecto: 3306
}
insertar_medicion_bp = Blueprint('insertar_medicion', __name__)

@insertar_medicion_bp.route('/generarLink', methods=['GET'])
def generar_link():
    # Lógica para generar el link

    id_dispositivos_raw = request.args.get('id_dispositivo')  # Puede ser '1,2,3' o None
    codigo_interno = request.args.get('codigo_interno')  # Nuevo parámetro

    # Si no hay id_dispositivo, buscarlo por codigo_interno
    if not id_dispositivos_raw and codigo_interno:
        try:
            conn = mysql.connector.connect(**config)
            cursor = conn.cursor()
            cursor.execute(
                "SELECT id_dispositivo FROM dispositivos WHERE codigo_interno = %s",
                (codigo_interno,)
            )
            results = cursor.fetchall()
            id_dispositivos_raw = ','.join([str(row[0]) for row in results])
        finally:
            if conn.is_connected():
                cursor.close()
                conn.close()

    try:
        conn = mysql.connector.connect(**config)
        cursor = conn.cursor()

        sql_query = """
        SELECT 
            sensores.id_sensor,    
            sensores.id_sensor_tipo,
            sensores_tipo.marca,    
            sensores_tipo.modelo,
            variables_en_sensores.idVariable,
            variables.unidad,
            variables.descripcion
        FROM sensores
        LEFT JOIN sensores_tipo ON sensores.id_sensor_tipo = sensores_tipo.id_sensor_tipo
        LEFT JOIN sensores_en_dispositivo ON sensores.id_sensor = sensores_en_dispositivo.id_sensor
        LEFT JOIN variables_en_sensores ON sensores.id_sensor_tipo = variables_en_sensores.idSensorTipo
        LEFT JOIN variables ON variables_en_sensores.idVariable = variables.id_variable
        """

        params = []

        if id_dispositivos_raw:
            # Convertir string '1,2,3' a lista ['1','2','3']
            id_dispositivos = [id_.strip() for id_ in id_dispositivos_raw.split(',') if id_.strip().isdigit()]
            if id_dispositivos:
                placeholders = ','.join(['%s'] * len(id_dispositivos))
                sql_query += f" WHERE sensores_en_dispositivo.id_dispositivo IN ({placeholders})"
                params.extend(id_dispositivos)
        # else no WHERE

        # sql_query += " ORDER BY sensores.id_sensor ASC"
        sql_query += " ORDER BY sensores.id_sensor ASC, variables_en_sensores.idVariable ASC"

        cursor.execute(sql_query, params)
        filas = cursor.fetchall()

        columnas = [
            "Id Sensor",
            "Id Sensor Tipo",
            "Marca",
            "Modelo",
            "Id Variable",
            "Unidad",
            "Descripcion",
        ]

        respuesta = [dict(zip(columnas, fila)) for fila in filas]

        # Crear el link concatenando los id_sensor_tipo y agregando el id_dispositivo
        sensor_tipos = [str(item["Id Sensor Tipo"]) for item in respuesta if item["Id Sensor Tipo"] is not None]
        sensor_tipos_str = ",".join(sensor_tipos)
        variables = [str(item["Id Variable"]) for item in respuesta if item["Id Variable"] is not None]
        variables_str = ",".join(variables)
        valores = [str(item["Descripcion"] + " [" + item["Unidad"] + "]") for item in respuesta if item["Descripcion"] is not None]
        valores_str = ",".join(valores)
        

        link_v2_id_dispositivos_raw = f"https://api-sensores.cmasccp.cl/insertarMedicionV2?idDispositivo={id_dispositivos_raw}&idsSensorTipo={sensor_tipos_str}&idsVariables={variables_str}&valores={valores_str}"
        link_v2_codigo_interno = f"https://api-sensores.cmasccp.cl/insertarMedicionV2?codigoInterno={codigo_interno}&idsSensorTipo={sensor_tipos_str}&idsVariables={variables_str}&valores={valores_str}"

        sensores = [str(item["Id Sensor"]) for item in respuesta if item["Id Sensor"] is not None]
        sensores_str = ",".join(sensores)
        link_v1 = f"https://api-sensores.cmasccp.cl/insertarMedicion?idsSensores={sensores_str}&idsVariables={variables_str}&valores={valores_str}"
        # https://api-sensores.cmasccp.cl/insertarMedicion?idsSensores=173,174,175,176&idsVariables=10,10,10,10&valores=10,10,10,10
        
        return jsonify({
            'status': 'success',
            'data': {
                'tableData': respuesta,
                'tabla': 'sensores_combinados'
            },
            "link_v1": link_v1,
            "link_v2_id_dispositivos_raw": link_v2_id_dispositivos_raw,
            "link_v2_codigo_interno": link_v2_codigo_interno
        }), 200, {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'}
        # return jsonify({'status': 'success', 'link': 'http://example.com/link'}), 200
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()

@insertar_medicion_bp.route('/generarLinkV2', methods=['GET'])
def generar_link_v2():
    # Lógica para generar el link
    id_dispositivos_raw = request.args.get('id_dispositivo')  # Puede ser '1,2,3' o None

    try:
        conn = mysql.connector.connect(**config)
        cursor = conn.cursor()

        sql_query = """
        SELECT 
            sensores.id_sensor,    
            sensores.id_sensor_tipo,
            sensores_tipo.marca,    
            sensores_tipo.modelo,
            variables_en_sensores.idVariable,
            variables.unidad,
            variables.descripcion
        FROM sensores
        LEFT JOIN sensores_tipo ON sensores.id_sensor_tipo = sensores_tipo.id_sensor_tipo
        LEFT JOIN sensores_en_dispositivo ON sensores.id_sensor = sensores_en_dispositivo.id_sensor
        LEFT JOIN variables_en_sensores ON sensores.id_sensor_tipo = variables_en_sensores.idSensorTipo
        LEFT JOIN variables ON variables_en_sensores.idVariable = variables.id_variable
        """

        params = []

        if id_dispositivos_raw:
            # Convertir string '1,2,3' a lista ['1','2','3']
            id_dispositivos = [id_.strip() for id_ in id_dispositivos_raw.split(',') if id_.strip().isdigit()]
            if id_dispositivos:
                placeholders = ','.join(['%s'] * len(id_dispositivos))
                sql_query += f" WHERE sensores_en_dispositivo.id_dispositivo IN ({placeholders})"
                params.extend(id_dispositivos)
        # else no WHERE

        # sql_query += " ORDER BY sensores.id_sensor ASC"
        sql_query += " ORDER BY sensores.id_sensor_tipo ASC, variables_en_sensores.idVariable ASC"

        cursor.execute(sql_query, params)
        filas = cursor.fetchall()

        columnas = [
            "Id Sensor",
            "Id Sensor Tipo",
            "Marca",
            "Modelo",
            "Id Variable",
            "Unidad",
            "Descripcion",
        ]

        respuesta = [dict(zip(columnas, fila)) for fila in filas]

        # Crear el link concatenando los id_sensor_tipo y agregando el id_dispositivo
        sensor_tipos = [str(item["Id Sensor Tipo"]) for item in respuesta if item["Id Sensor Tipo"] is not None]
        sensor_tipos_str = ",".join(sensor_tipos)
        variables = [str(item["Id Variable"]) for item in respuesta if item["Id Variable"] is not None]
        variables_str = ",".join(variables)
        valores = [str(item["Descripcion"] + " [" + item["Unidad"] + "]") for item in respuesta if item["Descripcion"] is not None]
        valores_str = ",".join(valores)
        link_v2 = f"https://api-sensores.cmasccp.cl/insertarMedicionV2?idDispositivo={id_dispositivos_raw}&idsSensorTipo={sensor_tipos_str}&idsVariables={variables_str}&valores={valores_str}"


        sensores = [str(item["Id Sensor"]) for item in respuesta if item["Id Sensor"] is not None]
        sensores_str = ",".join(sensores)
        link_v1 = f"https://api-sensores.cmasccp.cl/insertarMedicion?&idsSensores={sensores_str}&idsVariables={variables_str}&valores={valores_str}"
        # https://api-sensores.cmasccp.cl/insertarMedicion?idsSensores=173,174,175,176&idsVariables=10,10,10,10&valores=10,10,10,10
        
        return jsonify({
            'status': 'success',
            'data': {
                'tableData': respuesta,
                'tabla': 'sensores_combinados'
            },
            "link_v1": link_v1,
            "link_v2": link_v2
        }), 200, {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'}
        # return jsonify({'status': 'success', 'link': 'http://example.com/link'}), 200
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()

@insertar_medicion_bp.route('/insertarMedicionV2', methods=['GET'])
def insertar_medicion_v2():
    conn = None

    dispositivo_id_raw = request.args.get('idDispositivo', '')
    codigo_interno = request.args.get('codigoInterno', '')
    sensorTipo_ids = request.args.get('idsSensorTipo', '').split(',')

    # Si no hay idDispositivo, buscarlo por codigoInterno
    if not dispositivo_id_raw:
        dispositivo_id = []
        proyecto_id = []
        if codigo_interno:
            try:
                conn = mysql.connector.connect(**config)
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT id_dispositivo, id_proyecto FROM dispositivos WHERE codigo_interno = %s",
                    (codigo_interno,)
                )
                results = cursor.fetchall()
                dispositivo_id = [str(row[0]) for row in results]
                proyecto_id = [str(row[1]) for row in results]
            finally:
                if conn and conn.is_connected():
                    cursor.close()
                    conn.close()
    else:
        dispositivo_id = dispositivo_id_raw.split(',')

    timestamps = request.args.get('times', '').split(',')
    sesiones_ids = request.args.get('idsSesiones', '').split(',')
    variable_ids = request.args.get('idsVariables', '').split(',')
    values = request.args.get('valores', '').split(',')

    # Determinar sensor_ids a partir de dispositivo_id y sensorTipo_ids
    sensor_ids = []
    try:
        conn = mysql.connector.connect(**config)
        cursor = conn.cursor()
        for dispositivo in dispositivo_id:
            for sensor_tipo in sensorTipo_ids:
                if dispositivo and sensor_tipo:
                    cursor.execute(
                        """
                        SELECT sensores.id_sensor
                        FROM sensores
                        JOIN sensores_en_dispositivo ON sensores.id_sensor = sensores_en_dispositivo.id_sensor
                        WHERE sensores_en_dispositivo.id_dispositivo = %s AND sensores.id_sensor_tipo = %s
                        """,
                        (dispositivo, sensor_tipo)
                    )
                    result = cursor.fetchone()
                    if result:
                        sensor_ids.append(str(result[0]))
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()
    
    # Si timestamps tiene un solo valor
    # se entrega un UNIXTIME  
    if len(timestamps) == 1 and timestamps[0]:  # Un solo valor
        timestamps = [timestamps[0]] * len(sensor_ids)

    # Si timestamps NO tiene valor
    elif not timestamps[0]:  
        current_timestamp = datetime.now().timestamp()
        timestamps = [str(current_timestamp)] * len(sensor_ids)

    # Si sesiones_ids tiene un solo valor
    if len(sesiones_ids) == 1 and sesiones_ids[0]:  # Un solo valor
        sesiones_ids = [sesiones_ids[0]] * len(sensor_ids)

    # Si sesiones_ids NO tiene valor
    elif not sesiones_ids[0]:  
        sesiones_ids = [None] * len(sensor_ids)


    if not (len(timestamps) == len(sensor_ids) == len(variable_ids) == len(values) == len(sesiones_ids)):
        return jsonify({'status': 'fail', 'error': 'Las longitudes de los parametros no coinciden'}), 400
    
    measurements = []

    
    # Fecha de inserción (servidor)  
    server_timestamp = datetime.now().timestamp()
    insertion_timestamps = [str(server_timestamp)] * len(sensor_ids)


    for i in range(len(sensor_ids)):
        timestamp_float = float(timestamps[i])
        datetime_obj = datetime.fromtimestamp(timestamp_float)
        formatted_datetime = datetime_obj.strftime('%Y-%m-%d %H:%M:%S')

        # fechas insercion
        insertion_timestamp_float = float(insertion_timestamps[i])
        insertion_datetime_obj = datetime.fromtimestamp(insertion_timestamp_float)
        formatted_insertion_datetime = insertion_datetime_obj.strftime('%Y-%m-%d %H:%M:%S')

        # Convertir valores vacíos a None para insertarlos como NULL en la base de datos
        valor = values[i] if values[i] and values[i].strip() else None

        measurements.append({
            "timestamp":formatted_datetime, #timestamps[i],
            "sesionId": sesiones_ids[i],
            "sensorId": sensor_ids[i],
            "variableId": variable_ids[i],
            "value": valor,
            "insertionTimestamp": formatted_insertion_datetime

        })

    try:
        conn = mysql.connector.connect(**config)
        cursor = conn.cursor()

        for measurement in measurements:
            valores = [measurement['sensorId'], measurement['value'], measurement['timestamp'], measurement['variableId'], measurement['sesionId']]
            sql_query = f"INSERT INTO datos (id_sensor, valor, fecha, id_variable, id_sesion) VALUES (%s, %s, %s, %s, %s)"
            log_query = sql_query % tuple(valores)  # Para fines de depuración
            print("Consulta SQL para depuración:", log_query)
            cursor.execute(sql_query, valores)

        conn.commit()

        # Listar Datos Estructurados
        arguments = {
            "tabla": "datos",
            "disp.id_proyecto": proyecto_id[0],
            "disp.codigo_interno": codigo_interno,
            "limite": 1,
            "offset": 0
        }

        from listarDatosEstructuradosV2 import listar_datos_estructurados_v2
        res = listar_datos_estructurados_v2(arguments)
        print("Respuesta de listar_datos_estructurados_v2:", res, arguments)

        data_websocket = res
        
        
        # Emitir mensaje por SocketIO después del commit exitoso
        # socketio = current_app.extensions['socketio']
        # Importar socketio desde el archivo principal
        # from app import socketio
        # socketio.emit('medicion_insertada', "data_websocket")
        # return jsonify({'status': 'success', 'message': 'Registro insertado correctamente'}), 201, {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'}
        return jsonify({'status': 'success', 'message': 'Registro insertado correctamente', "data": res}), 201, {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'}

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

    # return jsonify({
    #     'status': 'success',
    #     'message': measurements
    # }), 201, {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'}