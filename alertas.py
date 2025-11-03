from flask import Blueprint, request, jsonify, current_app, make_response
import os
import json
import threading
import mysql.connector
from datetime import datetime, timedelta
import smtplib
from email.mime.text import MIMEText

# /c:/Users/Alienware/Desktop/Proyectos software/api_sensores/alertas.py

alertas_bp = Blueprint("alertas", __name__)
_file_lock = threading.Lock()

def _alerts_file_path():
    base = os.path.join(os.path.dirname(__file__), "json")
    os.makedirs(base, exist_ok=True)
    return os.path.join(base, "alerts.json")

def _read_alerts():
    path = _alerts_file_path()
    with _file_lock:
        if not os.path.exists(path):
            return []
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, list):
                    return data
                # if file contains single object, wrap as list
                if isinstance(data, dict):
                    return [data]
        except (json.JSONDecodeError, IOError):
            # corrupt or unreadable file -> start fresh
            return []
    return []

def _write_alerts(alerts_list):
    path = _alerts_file_path()
    with _file_lock:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(alerts_list, f, ensure_ascii=False, indent=2)

@alertas_bp.route("/insertarAlerta", methods=["POST"])
def insertar_alerta():
    """
    Recibe un objeto JSON en el cuerpo de la petición y lo añade a json/alerts.json
    Retorna la alerta creada y código 201.
    """
    if not request.is_json:
        return make_response(jsonify({"error": "Se requiere JSON en el cuerpo"}), 400)

    payload = request.get_json()
    if not isinstance(payload, dict):
        return make_response(jsonify({"error": "Se requiere un objeto JSON (diccionario)"}), 400)

    alerts = _read_alerts()

    # Verificar si ya existe una alerta con los mismos campos clave
    duplicate_fields = ['projectId', 'ruleType', 'validationId', 'parameter', 'config']
    
    for existing_alert in alerts:
        is_duplicate = True
        for field in duplicate_fields:
            # Si el campo no existe en payload o existing_alert, o son diferentes, no es duplicado
            if (field not in payload or 
                field not in existing_alert or 
                payload[field] != existing_alert[field]):
                is_duplicate = False
                break
        
        if is_duplicate:
            return make_response(jsonify({
                "error": "Ya existe una alerta con los mismos parámetros, pero puedes editarla",
                "existing_alert": existing_alert
            }), 409)

    alerts.append(payload)
    try:
        _write_alerts(alerts)
    except Exception as e:
        current_app.logger.exception("Error guardando alerta")
        return make_response(jsonify({"error": "No se pudo guardar la alerta"}), 500)

    return make_response(jsonify(payload), 201)

@alertas_bp.route("/listarAlertas", methods=["GET"])
def listar_alertas():

    """
    Devuelve la lista completa de alertas almacenadas en json/alerts.json
    """
    alerts = _read_alerts()
    return jsonify(alerts)


# Controladores de validación

def _get_db_config():
    """Obtiene la configuración de la base de datos desde variables de entorno"""
    return {
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "host": os.getenv("DB_HOST"),
        "database": os.getenv("DB_NAME"),
        "port": int(os.getenv("DB_PORT", 3306)),
    }

def _update_alert_last_validation(alert_id, validation_date):
    """Actualiza la fecha de última validación de una alerta específica"""
    alerts = _read_alerts()
    updated = False
    
    for alert in alerts:
        if alert.get("id") == alert_id:
            alert["ultima_validacion"] = validation_date.isoformat()
            updated = True
            break
    
    if updated:
        _write_alerts(alerts)
    
    return updated



# Funciones de Validaciones Parametros
def _validate_missing_value_for_alert(alert):
    """
    Valida si hay valores nulos o vacíos para el parámetro especificado en la alerta
    
    Args:
        alert: Objeto de alerta con la configuración
    
    Returns:
        dict: Resultado de la validación
    """
    try:
        config = _get_db_config()
        conn = mysql.connector.connect(**config)
        cursor = conn.cursor()
        
        project_id = alert["projectId"]
        parameter = alert["parameter"]

        validation_time = datetime.now()
        
        # Determinar dispositivos a validar
        if alert.get("applyToAllDevices", True):
            # Todos los dispositivos del proyecto
            device_query = """
                SELECT id_dispositivo, codigo_interno 
                FROM sensores_dev.dispositivos 
                WHERE id_proyecto = %s
            """
            device_params = [project_id]
        else:
            # Dispositivos específicos
            target_devices = alert.get("targetDevices", [])
            if not target_devices:
                return {
                    "status": "error",
                    "message": "No hay dispositivos objetivo especificados"
                }
            
            device_placeholders = ','.join(['%s'] * len(target_devices))
            device_query = f"""
                SELECT id_dispositivo, codigo_interno 
                FROM sensores_dev.dispositivos 
                WHERE id_proyecto = %s AND codigo_interno IN ({device_placeholders})
            """
            device_params = [project_id] + list(target_devices)
        
        cursor.execute(device_query, device_params)
        project_devices = cursor.fetchall()
        
        if not project_devices:
            return {
                "status": "error",
                "message": "No se encontraron dispositivos para validar"
            }
        
        # Buscar valores nulos o vacíos en las últimas 24 horas
        end_time = validation_time
        start_time = end_time - timedelta(hours=24)
        
        devices_with_issues = []
        
        for device_id, codigo_interno in project_devices:
            # Buscar valores NULL, vacíos o que falten completamente
            null_values_query = """
                SELECT d.id_dato, d.fecha, d.valor
                FROM sensores_dev.datos AS d
                LEFT JOIN sensores_dev.variables AS v ON d.id_variable = v.id_variable
                LEFT JOIN sensores_dev.sensores AS sens ON d.id_sensor = sens.id_sensor
                LEFT JOIN sensores_dev.sensores_tipo AS st ON sens.id_sensor_tipo = st.id_sensor_tipo
                LEFT JOIN sensores_dev.sensores_en_dispositivo AS sed ON sens.id_sensor = sed.id_sensor
                LEFT JOIN sensores_dev.dispositivos AS disp ON sed.id_dispositivo = disp.id_dispositivo
                WHERE disp.id_dispositivo = %s
                AND CONCAT(st.modelo, ' [', v.descripcion, ' (', v.unidad, ')]') = %s
                AND d.fecha >= %s AND d.fecha <= %s
                AND (d.valor IS NULL OR d.valor = '' OR d.valor = 0)
                ORDER BY d.fecha DESC
            """
            
            cursor.execute(null_values_query, [device_id, parameter, start_time, end_time])
            null_results = cursor.fetchall()
            
            # También verificar si no hay datos en absoluto
            total_count_query = """
                SELECT COUNT(*) as total
                FROM sensores_dev.datos AS d
                LEFT JOIN sensores_dev.variables AS v ON d.id_variable = v.id_variable
                LEFT JOIN sensores_dev.sensores AS sens ON d.id_sensor = sens.id_sensor
                LEFT JOIN sensores_dev.sensores_tipo AS st ON sens.id_sensor_tipo = st.id_sensor_tipo
                LEFT JOIN sensores_dev.sensores_en_dispositivo AS sed ON sens.id_sensor = sed.id_sensor
                LEFT JOIN sensores_dev.dispositivos AS disp ON sed.id_dispositivo = disp.id_dispositivo
                WHERE disp.id_dispositivo = %s
                AND CONCAT(st.modelo, ' [', v.descripcion, ' (', v.unidad, ')]') = %s
                AND d.fecha >= %s AND d.fecha <= %s
            """
            
            cursor.execute(total_count_query, [device_id, parameter, start_time, end_time])
            total_count = cursor.fetchone()[0]
            
            if null_results or total_count == 0:
                issue_type = "no_data" if total_count == 0 else "null_values"
                
                devices_with_issues.append({
                    "device_id": device_id,
                    "codigo_interno": codigo_interno,
                    "issue_type": issue_type,
                    "null_count": len(null_results) if null_results else 0,
                    "total_measurements": total_count,
                    "null_values": [
                        {
                            "id_dato": row[0],
                            "fecha": row[1].isoformat() if row[1] else None,
                            "valor": row[2]
                        } for row in null_results[:5]  # Limitar a 5 ejemplos
                    ] if null_results else []
                })
        
        # Enviar correo si hay dispositivos con problemas
        email_results = []
        if devices_with_issues:
            # Obtener destinatarios desde la configuración de la alerta
            alert_emails = alert.get("email", [])
            if isinstance(alert_emails, str):
                alert_emails = [alert_emails]
            
            for device_issue in devices_with_issues:
                try:
                    # Llamar a la función de envío de correo con los parámetros requeridos
                    email_result = send_email_alert(
                        TITULO=f"Alerta: Valores perdidos para {parameter} en dispositivo {device_issue['codigo_interno']}.",
                        PROYECTO_ID=project_id,
                        CODIGO_INTERNO=device_issue["codigo_interno"],
                        FECHA=validation_time.strftime('%Y-%m-%d %H:%M:%S'),
                        receivers=alert_emails if alert_emails else None  # Usar emails de la alerta o default
                    )
                    email_results.append({
                        "device": device_issue["codigo_interno"],
                        "email_result": email_result
                    })
                except Exception as email_error:
                    print(f"{datetime.now()}: Error enviando correo para dispositivo {device_issue['codigo_interno']}: {email_error}")
                    email_results.append({
                        "device": device_issue["codigo_interno"],
                        "email_result": {
                            "success": False,
                            "error": str(email_error)
                        }
                    })
        
        # Actualizar fecha de última validación en la alerta
        _update_alert_last_validation(alert["id"], validation_time)
        
        # Calcular estadísticas de correos enviados
        total_emails_sent = sum(result["email_result"].get("emails_sent", 0) for result in email_results)
        total_emails_failed = sum(result["email_result"].get("emails_failed", 0) for result in email_results)
        
        return {
            "status": "success",
            "alert_id": alert["id"],
            "validation_type": "missing_value",
            "project_id": project_id,
            "parameter": parameter,
            "validation_time": validation_time.isoformat(),
            "period_checked": f"{start_time.isoformat()} a {end_time.isoformat()}",
            "total_devices_checked": len(project_devices),
            "devices_with_issues": len(devices_with_issues),
            "issues_found": devices_with_issues,
            "email_notifications": {
                "total_emails_sent": total_emails_sent,
                "total_emails_failed": total_emails_failed,
                "devices_notified": len(email_results),
                "email_details": email_results
            }
        }
        
    except mysql.connector.Error as e:
        return {
            "status": "error",
            "message": f"Error de base de datos: {str(e)}"
        }
    except Exception as e:
        return {
            "status": "error",
            "message": f"Error inesperado: {str(e)}"
        }
    finally:
        try:
            if 'cursor' in locals():
                cursor.close()
            if 'conn' in locals() and conn.is_connected():
                conn.close()
        except Exception:
            pass
def _validate_range_threshold_for_alert(alert):
    """
    Valida si hay valores que exceden un umbral específico usando operadores de comparación
    
    Configuración esperada en alert["config"]:
    {
        "operador": ">",  # >, <, =, <=, >=
        "limite": 100     # Valor límite para comparar
    }
    
    Args:
        alert: Objeto de alerta con la configuración
    
    Returns:
        dict: Resultado de la validación
    """
    try:
        config = _get_db_config()
        conn = mysql.connector.connect(**config)
        cursor = conn.cursor()
        
        project_id = alert["projectId"]
        parameter = alert["parameter"]
        
        # Obtener configuración del umbral
        alert_config = alert.get("config", {})
        operador = alert_config.get("operador", ">")
        limite = alert_config.get("limite")
        
        if limite is None:
            return {
                "status": "error",
                "message": "No se especificó límite en la configuración de la alerta"
            }
        
        # Validar operador
        operadores_validos = [">", "<", "=", "<=", ">=", "!="]
        if operador not in operadores_validos:
            return {
                "status": "error",
                "message": f"Operador '{operador}' no válido. Use uno de: {operadores_validos}"
            }

        validation_time = datetime.now()
        
        # Determinar dispositivos a validar
        if alert.get("applyToAllDevices", True):
            # Todos los dispositivos del proyecto
            device_query = """
                SELECT id_dispositivo, codigo_interno 
                FROM sensores_dev.dispositivos 
                WHERE id_proyecto = %s
            """
            device_params = [project_id]
        else:
            # Dispositivos específicos
            target_devices = alert.get("targetDevices", [])
            if not target_devices:
                return {
                    "status": "error",
                    "message": "No hay dispositivos objetivo especificados"
                }
            
            device_placeholders = ','.join(['%s'] * len(target_devices))
            device_query = f"""
                SELECT id_dispositivo, codigo_interno 
                FROM sensores_dev.dispositivos 
                WHERE id_proyecto = %s AND codigo_interno IN ({device_placeholders})
            """
            device_params = [project_id] + list(target_devices)
        
        cursor.execute(device_query, device_params)
        project_devices = cursor.fetchall()
        
        if not project_devices:
            return {
                "status": "error",
                "message": "No se encontraron dispositivos para validar"
            }
        
        # Buscar valores que superen el umbral en las últimas 24 horas
        end_time = validation_time
        start_time = end_time - timedelta(hours=24)
        
        devices_with_issues = []
        
        for device_id, codigo_interno in project_devices:
            # Consultar valores que exceden el umbral según el operador
            threshold_query = f"""
                SELECT d.id_dato, d.fecha, d.valor
                FROM sensores_dev.datos AS d
                LEFT JOIN sensores_dev.variables AS v ON d.id_variable = v.id_variable
                LEFT JOIN sensores_dev.sensores AS sens ON d.id_sensor = sens.id_sensor
                LEFT JOIN sensores_dev.sensores_tipo AS st ON sens.id_sensor_tipo = st.id_sensor_tipo
                LEFT JOIN sensores_dev.sensores_en_dispositivo AS sed ON sens.id_sensor = sed.id_sensor
                LEFT JOIN sensores_dev.dispositivos AS disp ON sed.id_dispositivo = disp.id_dispositivo
                WHERE disp.id_dispositivo = %s
                AND CONCAT(st.modelo, ' [', v.descripcion, ' (', v.unidad, ')]') = %s
                AND d.fecha >= %s AND d.fecha <= %s
                AND d.valor IS NOT NULL
                AND CAST(d.valor AS DECIMAL(10,2)) {operador} %s
                ORDER BY d.fecha DESC
            """
            
            cursor.execute(threshold_query, [device_id, parameter, start_time, end_time, limite])
            threshold_results = cursor.fetchall()
            
            # Contar el total de mediciones válidas para contexto
            total_count_query = """
                SELECT COUNT(*) as total
                FROM sensores_dev.datos AS d
                LEFT JOIN sensores_dev.variables AS v ON d.id_variable = v.id_variable
                LEFT JOIN sensores_dev.sensores AS sens ON d.id_sensor = sens.id_sensor
                LEFT JOIN sensores_dev.sensores_tipo AS st ON sens.id_sensor_tipo = st.id_sensor_tipo
                LEFT JOIN sensores_dev.sensores_en_dispositivo AS sed ON sens.id_sensor = sed.id_sensor
                LEFT JOIN sensores_dev.dispositivos AS disp ON sed.id_dispositivo = disp.id_dispositivo
                WHERE disp.id_dispositivo = %s
                AND CONCAT(st.modelo, ' [', v.descripcion, ' (', v.unidad, ')]') = %s
                AND d.fecha >= %s AND d.fecha <= %s
                AND d.valor IS NOT NULL
            """
            
            cursor.execute(total_count_query, [device_id, parameter, start_time, end_time])
            total_count = cursor.fetchone()[0]
            
            if threshold_results:
                devices_with_issues.append({
                    "device_id": device_id,
                    "codigo_interno": codigo_interno,
                    "issue_type": "threshold_exceeded",
                    "threshold_violations": len(threshold_results),
                    "total_measurements": total_count,
                    "operador": operador,
                    "limite": limite,
                    "violating_values": [
                        {
                            "id_dato": row[0],
                            "fecha": row[1].isoformat() if row[1] else None,
                            "valor": float(row[2]) if row[2] is not None else None
                        } for row in threshold_results[:10]  # Limitar a 10 ejemplos
                    ]
                })
        
        # Enviar correo si hay dispositivos con problemas
        email_results = []
        if devices_with_issues:
            # Obtener destinatarios desde la configuración de la alerta
            alert_emails = alert.get("email", [])
            if isinstance(alert_emails, str):
                alert_emails = [alert_emails]
            
            for device_issue in devices_with_issues:
                try:
                    # Llamar a la función de envío de correo con los parámetros requeridos
                    email_result = send_email_alert(
                        TITULO=f"Alerta: Umbral excedido para {parameter} en dispositivo {device_issue['codigo_interno']} ({operador} {limite})",
                        PROYECTO_ID=project_id,
                        CODIGO_INTERNO=device_issue["codigo_interno"],
                        FECHA=validation_time.strftime('%Y-%m-%d %H:%M:%S'),
                        receivers=alert_emails if alert_emails else None
                    )
                    email_results.append({
                        "device": device_issue["codigo_interno"],
                        "email_result": email_result
                    })
                except Exception as email_error:
                    print(f"{datetime.now()}: Error enviando correo para dispositivo {device_issue['codigo_interno']}: {email_error}")
                    email_results.append({
                        "device": device_issue["codigo_interno"],
                        "email_result": {
                            "success": False,
                            "error": str(email_error)
                        }
                    })
        
        # Actualizar fecha de última validación en la alerta
        _update_alert_last_validation(alert["id"], validation_time)
        
        # Calcular estadísticas de correos enviados
        total_emails_sent = sum(result["email_result"].get("emails_sent", 0) for result in email_results)
        total_emails_failed = sum(result["email_result"].get("emails_failed", 0) for result in email_results)
        
        return {
            "status": "success",
            "alert_id": alert["id"],
            "validation_type": "range_threshold",
            "project_id": project_id,
            "parameter": parameter,
            "threshold_config": {
                "operador": operador,
                "limite": limite
            },
            "validation_time": validation_time.isoformat(),
            "period_checked": f"{start_time.isoformat()} a {end_time.isoformat()}",
            "total_devices_checked": len(project_devices),
            "devices_with_issues": len(devices_with_issues),
            "total_violations": sum(device["threshold_violations"] for device in devices_with_issues),
            "issues_found": devices_with_issues,
            "email_notifications": {
                "total_emails_sent": total_emails_sent,
                "total_emails_failed": total_emails_failed,
                "devices_notified": len(email_results),
                "email_details": email_results
            }
        }
        
    except mysql.connector.Error as e:
        return {
            "status": "error",
            "message": f"Error de base de datos: {str(e)}"
        }
    except Exception as e:
        return {
            "status": "error",
            "message": f"Error inesperado: {str(e)}"
        }
    finally:
        try:
            if 'cursor' in locals():
                cursor.close()
            if 'conn' in locals() and conn.is_connected():
                conn.close()
        except Exception:
            pass
def _validate_between_range_for_alert(alert):
    """
    Valida si hay valores dentro o fuera de un rango específico
    
    Configuración esperada en alert["config"]:
    {
        "min": 0,           # Valor mínimo del rango
        "max": 40,          # Valor máximo del rango
        "inclusivo": true,  # Si los límites son inclusivos (<=, >=) o exclusivos (<, >)
        "alertar_si": "fuera"  # "fuera" o "dentro" - cuándo alertar
    }
    
    Args:
        alert: Objeto de alerta con la configuración
    
    Returns:
        dict: Resultado de la validación
    """
    try:
        config = _get_db_config()
        conn = mysql.connector.connect(**config)
        cursor = conn.cursor()
        
        project_id = alert["projectId"]
        parameter = alert["parameter"]
        
        # Obtener configuración del rango
        alert_config = alert.get("config", {})
        min_value = alert_config.get("min")
        max_value = alert_config.get("max")
        inclusivo = alert_config.get("inclusivo", True)
        alertar_si = alert_config.get("alertar_si", "fuera")
        
        # Validar configuración
        if min_value is None or max_value is None:
            return {
                "status": "error",
                "message": "Se requieren valores 'min' y 'max' en la configuración"
            }
        
        if min_value >= max_value:
            return {
                "status": "error",
                "message": "El valor 'min' debe ser menor que 'max'"
            }
        
        if alertar_si not in ["fuera", "dentro"]:
            return {
                "status": "error",
                "message": "El campo 'alertar_si' debe ser 'fuera' o 'dentro'"
            }

        validation_time = datetime.now()
        
        # Determinar dispositivos a validar
        if alert.get("applyToAllDevices", True):
            device_query = """
                SELECT id_dispositivo, codigo_interno 
                FROM sensores_dev.dispositivos 
                WHERE id_proyecto = %s
            """
            device_params = [project_id]
        else:
            target_devices = alert.get("targetDevices", [])
            if not target_devices:
                return {
                    "status": "error",
                    "message": "No hay dispositivos objetivo especificados"
                }
            
            device_placeholders = ','.join(['%s'] * len(target_devices))
            device_query = f"""
                SELECT id_dispositivo, codigo_interno 
                FROM sensores_dev.dispositivos 
                WHERE id_proyecto = %s AND codigo_interno IN ({device_placeholders})
            """
            device_params = [project_id] + list(target_devices)
        
        cursor.execute(device_query, device_params)
        project_devices = cursor.fetchall()
        
        if not project_devices:
            return {
                "status": "error",
                "message": "No se encontraron dispositivos para validar"
            }
        
        # Construir condición SQL según configuración
        if inclusivo:
            if alertar_si == "fuera":
                # Valores fuera del rango (inclusive): valor < min OR valor > max
                range_condition = "AND (CAST(d.valor AS DECIMAL(10,2)) < %s OR CAST(d.valor AS DECIMAL(10,2)) > %s)"
            else:
                # Valores dentro del rango (inclusive): valor >= min AND valor <= max
                range_condition = "AND (CAST(d.valor AS DECIMAL(10,2)) >= %s AND CAST(d.valor AS DECIMAL(10,2)) <= %s)"
        else:
            if alertar_si == "fuera":
                # Valores fuera del rango (exclusive): valor <= min OR valor >= max
                range_condition = "AND (CAST(d.valor AS DECIMAL(10,2)) <= %s OR CAST(d.valor AS DECIMAL(10,2)) >= %s)"
            else:
                # Valores dentro del rango (exclusive): valor > min AND valor < max
                range_condition = "AND (CAST(d.valor AS DECIMAL(10,2)) > %s AND CAST(d.valor AS DECIMAL(10,2)) < %s)"
        
        # Buscar valores que cumplen la condición en las últimas 24 horas
        end_time = validation_time
        start_time = end_time - timedelta(hours=24)
        
        devices_with_issues = []
        
        for device_id, codigo_interno in project_devices:
            # Consultar valores que cumplen la condición de rango
            range_query = f"""
                SELECT d.id_dato, d.fecha, d.valor
                FROM sensores_dev.datos AS d
                LEFT JOIN sensores_dev.variables AS v ON d.id_variable = v.id_variable
                LEFT JOIN sensores_dev.sensores AS sens ON d.id_sensor = sens.id_sensor
                LEFT JOIN sensores_dev.sensores_tipo AS st ON sens.id_sensor_tipo = st.id_sensor_tipo
                LEFT JOIN sensores_dev.sensores_en_dispositivo AS sed ON sens.id_sensor = sed.id_sensor
                LEFT JOIN sensores_dev.dispositivos AS disp ON sed.id_dispositivo = disp.id_dispositivo
                WHERE disp.id_dispositivo = %s
                AND CONCAT(st.modelo, ' [', v.descripcion, ' (', v.unidad, ')]') = %s
                AND d.fecha >= %s AND d.fecha <= %s
                AND d.valor IS NOT NULL
                {range_condition}
                ORDER BY d.fecha DESC
            """
            
            cursor.execute(range_query, [device_id, parameter, start_time, end_time, min_value, max_value])
            range_results = cursor.fetchall()
            
            # Contar el total de mediciones válidas para contexto
            total_count_query = """
                SELECT COUNT(*) as total
                FROM sensores_dev.datos AS d
                LEFT JOIN sensores_dev.variables AS v ON d.id_variable = v.id_variable
                LEFT JOIN sensores_dev.sensores AS sens ON d.id_sensor = sens.id_sensor
                LEFT JOIN sensores_dev.sensores_tipo AS st ON sens.id_sensor_tipo = st.id_sensor_tipo
                LEFT JOIN sensores_dev.sensores_en_dispositivo AS sed ON sens.id_sensor = sed.id_sensor
                LEFT JOIN sensores_dev.dispositivos AS disp ON sed.id_dispositivo = disp.id_dispositivo
                WHERE disp.id_dispositivo = %s
                AND CONCAT(st.modelo, ' [', v.descripcion, ' (', v.unidad, ')]') = %s
                AND d.fecha >= %s AND d.fecha <= %s
                AND d.valor IS NOT NULL
            """
            
            cursor.execute(total_count_query, [device_id, parameter, start_time, end_time])
            total_count = cursor.fetchone()[0]
            
            if range_results:
                devices_with_issues.append({
                    "device_id": device_id,
                    "codigo_interno": codigo_interno,
                    "issue_type": f"values_{alertar_si}_range",
                    "range_violations": len(range_results),
                    "total_measurements": total_count,
                    "range_config": {
                        "min": min_value,
                        "max": max_value,
                        "inclusivo": inclusivo,
                        "alertar_si": alertar_si
                    },
                    "violating_values": [
                        {
                            "id_dato": row[0],
                            "fecha": row[1].isoformat() if row[1] else None,
                            "valor": float(row[2]) if row[2] is not None else None
                        } for row in range_results[:10]  # Limitar a 10 ejemplos
                    ]
                })
        
        # Enviar correo si hay dispositivos con problemas
        email_results = []
        if devices_with_issues:
            alert_emails = alert.get("email", [])
            if isinstance(alert_emails, str):
                alert_emails = [alert_emails]
            
            for device_issue in devices_with_issues:
                try:
                    range_desc = f"{min_value}-{max_value}"
                    inclusivo_desc = "inclusive" if inclusivo else "exclusive"
                    titulo = f"Alerta: Valores {alertar_si} del rango {range_desc} ({inclusivo_desc}) para {parameter} en dispositivo {device_issue['codigo_interno']}"
                    
                    email_result = send_email_alert(
                        TITULO=titulo,
                        PROYECTO_ID=project_id,
                        CODIGO_INTERNO=device_issue["codigo_interno"],
                        FECHA=validation_time.strftime('%Y-%m-%d %H:%M:%S'),
                        receivers=alert_emails if alert_emails else None
                    )
                    email_results.append({
                        "device": device_issue["codigo_interno"],
                        "email_result": email_result
                    })
                except Exception as email_error:
                    print(f"{datetime.now()}: Error enviando correo para dispositivo {device_issue['codigo_interno']}: {email_error}")
                    email_results.append({
                        "device": device_issue["codigo_interno"],
                        "email_result": {
                            "success": False,
                            "error": str(email_error)
                        }
                    })
        
        # Actualizar fecha de última validación en la alerta
        _update_alert_last_validation(alert["id"], validation_time)
        
        # Calcular estadísticas de correos enviados
        total_emails_sent = sum(result["email_result"].get("emails_sent", 0) for result in email_results)
        total_emails_failed = sum(result["email_result"].get("emails_failed", 0) for result in email_results)
        
        return {
            "status": "success",
            "alert_id": alert["id"],
            "validation_type": "between_range",
            "project_id": project_id,
            "parameter": parameter,
            "range_config": {
                "min": min_value,
                "max": max_value,
                "inclusivo": inclusivo,
                "alertar_si": alertar_si
            },
            "validation_time": validation_time.isoformat(),
            "period_checked": f"{start_time.isoformat()} a {end_time.isoformat()}",
            "total_devices_checked": len(project_devices),
            "devices_with_issues": len(devices_with_issues),
            "total_violations": sum(device["range_violations"] for device in devices_with_issues),
            "issues_found": devices_with_issues,
            "email_notifications": {
                "total_emails_sent": total_emails_sent,
                "total_emails_failed": total_emails_failed,
                "devices_notified": len(email_results),
                "email_details": email_results
            }
        }
        
    except mysql.connector.Error as e:
        return {
            "status": "error",
            "message": f"Error de base de datos: {str(e)}"
        }
    except Exception as e:
        return {
            "status": "error",
            "message": f"Error inesperado: {str(e)}"
        }
    finally:
        try:
            if 'cursor' in locals():
                cursor.close()
            if 'conn' in locals() and conn.is_connected():
                conn.close()
        except Exception:
            pass
def _validate_rate_of_change_for_alert(alert):
    """
    Valida cambios bruscos en los valores (pendiente/tasa de cambio)
    
    Configuración esperada en alert["config"]:
    {
        "ventana_muestras": 1,      # Número de muestras anteriores a comparar
        "max_delta_pct": 100        # Máximo porcentaje de cambio permitido
    }
    
    Args:
        alert: Objeto de alerta con la configuración
    
    Returns:
        dict: Resultado de la validación
    """
    try:
        config = _get_db_config()
        conn = mysql.connector.connect(**config)
        cursor = conn.cursor()
        
        project_id = alert["projectId"]
        parameter = alert["parameter"]
        
        # Obtener configuración de tasa de cambio
        alert_config = alert.get("config", {})
        ventana_muestras = alert_config.get("ventana_muestras", 1)
        max_delta_pct = alert_config.get("max_delta_pct", 100)
        
        # Validar configuración
        if ventana_muestras < 1:
            return {
                "status": "error",
                "message": "La ventana de muestras debe ser al menos 1"
            }
        
        if max_delta_pct <= 0:
            return {
                "status": "error",
                "message": "El porcentaje máximo de cambio debe ser mayor a 0"
            }

        validation_time = datetime.now()
        
        # Determinar dispositivos a validar
        if alert.get("applyToAllDevices", True):
            device_query = """
                SELECT id_dispositivo, codigo_interno 
                FROM sensores_dev.dispositivos 
                WHERE id_proyecto = %s
            """
            device_params = [project_id]
        else:
            target_devices = alert.get("targetDevices", [])
            if not target_devices:
                return {
                    "status": "error",
                    "message": "No hay dispositivos objetivo especificados"
                }
            
            device_placeholders = ','.join(['%s'] * len(target_devices))
            device_query = f"""
                SELECT id_dispositivo, codigo_interno 
                FROM sensores_dev.dispositivos 
                WHERE id_proyecto = %s AND codigo_interno IN ({device_placeholders})
            """
            device_params = [project_id] + list(target_devices)
        
        cursor.execute(device_query, device_params)
        project_devices = cursor.fetchall()
        
        if not project_devices:
            return {
                "status": "error",
                "message": "No se encontraron dispositivos para validar"
            }
        
        # Buscar cambios bruscos en las últimas 24 horas
        end_time = validation_time
        start_time = end_time - timedelta(hours=24)
        
        devices_with_issues = []
        
        for device_id, codigo_interno in project_devices:
            # Obtener todos los valores ordenados por fecha (más recientes primero)
            values_query = """
                SELECT d.id_dato, d.fecha, d.valor
                FROM sensores_dev.datos AS d
                LEFT JOIN sensores_dev.variables AS v ON d.id_variable = v.id_variable
                LEFT JOIN sensores_dev.sensores AS sens ON d.id_sensor = sens.id_sensor
                LEFT JOIN sensores_dev.sensores_tipo AS st ON sens.id_sensor_tipo = st.id_sensor_tipo
                LEFT JOIN sensores_dev.sensores_en_dispositivo AS sed ON sens.id_sensor = sed.id_sensor
                LEFT JOIN sensores_dev.dispositivos AS disp ON sed.id_dispositivo = disp.id_dispositivo
                WHERE disp.id_dispositivo = %s
                AND CONCAT(st.modelo, ' [', v.descripcion, ' (', v.unidad, ')]') = %s
                AND d.fecha >= %s AND d.fecha <= %s
                AND d.valor IS NOT NULL
                AND CAST(d.valor AS DECIMAL(10,2)) > 0
                ORDER BY d.fecha ASC
            """
            
            cursor.execute(values_query, [device_id, parameter, start_time, end_time])
            values_results = cursor.fetchall()
            
            if len(values_results) < ventana_muestras + 1:
                # No hay suficientes muestras para calcular cambios
                continue
            
            # Analizar cambios bruscos
            rapid_changes = []
            
            for i in range(ventana_muestras, len(values_results)):
                current_value = float(values_results[i][2])
                
                # Calcular promedio de las muestras anteriores en la ventana
                window_values = [float(values_results[j][2]) for j in range(i - ventana_muestras, i)]
                avg_previous = sum(window_values) / len(window_values)
                
                # Evitar división por cero
                if avg_previous == 0:
                    continue
                
                # Calcular porcentaje de cambio
                delta_pct = abs((current_value - avg_previous) / avg_previous) * 100
                
                if delta_pct > max_delta_pct:
                    rapid_changes.append({
                        "id_dato": values_results[i][0],
                        "fecha": values_results[i][1].isoformat() if values_results[i][1] else None,
                        "valor_actual": current_value,
                        "valor_promedio_anterior": round(avg_previous, 2),
                        "delta_porcentaje": round(delta_pct, 2),
                        "ventana_anterior": [
                            {
                                "fecha": values_results[j][1].isoformat() if values_results[j][1] else None,
                                "valor": float(values_results[j][2])
                            } for j in range(i - ventana_muestras, i)
                        ]
                    })
            
            # Contar total de mediciones para contexto
            total_count = len(values_results)
            
            if rapid_changes:
                devices_with_issues.append({
                    "device_id": device_id,
                    "codigo_interno": codigo_interno,
                    "issue_type": "rapid_change",
                    "rapid_changes_count": len(rapid_changes),
                    "total_measurements": total_count,
                    "rate_config": {
                        "ventana_muestras": ventana_muestras,
                        "max_delta_pct": max_delta_pct
                    },
                    "rapid_changes": rapid_changes[:10]  # Limitar a 10 ejemplos
                })
        
        # Enviar correo si hay dispositivos con problemas
        email_results = []
        if devices_with_issues:
            alert_emails = alert.get("email", [])
            if isinstance(alert_emails, str):
                alert_emails = [alert_emails]
            
            for device_issue in devices_with_issues:
                try:
                    titulo = f"Alerta: Cambios bruscos detectados en {parameter} para dispositivo {device_issue['codigo_interno']} (>{max_delta_pct}%)"
                    
                    email_result = send_email_alert(
                        TITULO=titulo,
                        PROYECTO_ID=project_id,
                        CODIGO_INTERNO=device_issue["codigo_interno"],
                        FECHA=validation_time.strftime('%Y-%m-%d %H:%M:%S'),
                        receivers=alert_emails if alert_emails else None
                    )
                    email_results.append({
                        "device": device_issue["codigo_interno"],
                        "email_result": email_result
                    })
                except Exception as email_error:
                    print(f"{datetime.now()}: Error enviando correo para dispositivo {device_issue['codigo_interno']}: {email_error}")
                    email_results.append({
                        "device": device_issue["codigo_interno"],
                        "email_result": {
                            "success": False,
                            "error": str(email_error)
                        }
                    })
        
        # Actualizar fecha de última validación en la alerta
        _update_alert_last_validation(alert["id"], validation_time)
        
        # Calcular estadísticas de correos enviados
        total_emails_sent = sum(result["email_result"].get("emails_sent", 0) for result in email_results)
        total_emails_failed = sum(result["email_result"].get("emails_failed", 0) for result in email_results)
        
        return {
            "status": "success",
            "alert_id": alert["id"],
            "validation_type": "rate_of_change",
            "project_id": project_id,
            "parameter": parameter,
            "rate_config": {
                "ventana_muestras": ventana_muestras,
                "max_delta_pct": max_delta_pct
            },
            "validation_time": validation_time.isoformat(),
            "period_checked": f"{start_time.isoformat()} a {end_time.isoformat()}",
            "total_devices_checked": len(project_devices),
            "devices_with_issues": len(devices_with_issues),
            "total_rapid_changes": sum(device["rapid_changes_count"] for device in devices_with_issues),
            "issues_found": devices_with_issues,
            "email_notifications": {
                "total_emails_sent": total_emails_sent,
                "total_emails_failed": total_emails_failed,
                "devices_notified": len(email_results),
                "email_details": email_results
            }
        }
        
    except mysql.connector.Error as e:
        return {
            "status": "error",
            "message": f"Error de base de datos: {str(e)}"
        }
    except Exception as e:
        return {
            "status": "error",
            "message": f"Error inesperado: {str(e)}"
        }
    finally:
        try:
            if 'cursor' in locals():
                cursor.close()
            if 'conn' in locals() and conn.is_connected():
                conn.close()
        except Exception:
            pass

def _validate_stuck_value_for_alert(alert):
    """
    Valida si hay valores congelados/repetidos durante una ventana de muestras
    
    Configuración esperada en alert["config"]:
    {
        "ventana_muestras": 10,     # Número de muestras consecutivas a analizar
        "max_unicos": 1             # Máximo número de valores únicos permitidos en la ventana
    }
    
    Args:
        alert: Objeto de alerta con la configuración
    
    Returns:
        dict: Resultado de la validación
    """
    try:
        config = _get_db_config()
        conn = mysql.connector.connect(**config)
        cursor = conn.cursor()
        
        project_id = alert["projectId"]
        parameter = alert["parameter"]
        
        # Obtener configuración de valor congelado
        alert_config = alert.get("config", {})
        ventana_muestras = alert_config.get("ventana_muestras", 10)
        max_unicos = alert_config.get("max_unicos", 1)
        
        # Validar configuración
        if ventana_muestras < 2:
            return {
                "status": "error",
                "message": "La ventana de muestras debe ser al menos 2"
            }
        
        if max_unicos < 1:
            return {
                "status": "error",
                "message": "El número máximo de valores únicos debe ser al menos 1"
            }

        validation_time = datetime.now()
        
        # Determinar dispositivos a validar
        if alert.get("applyToAllDevices", True):
            device_query = """
                SELECT id_dispositivo, codigo_interno 
                FROM sensores_dev.dispositivos 
                WHERE id_proyecto = %s
            """
            device_params = [project_id]
        else:
            target_devices = alert.get("targetDevices", [])
            if not target_devices:
                return {
                    "status": "error",
                    "message": "No se especificaron dispositivos objetivo"
                }
            
            device_placeholders = ','.join(['%s'] * len(target_devices))
            device_query = f"""
                SELECT id_dispositivo, codigo_interno 
                FROM sensores_dev.dispositivos 
                WHERE id_proyecto = %s AND codigo_interno IN ({device_placeholders})
            """
            device_params = [project_id] + list(target_devices)
        
        cursor.execute(device_query, device_params)
        project_devices = cursor.fetchall()
        
        if not project_devices:
            return {
                "status": "error",
                "message": "No se encontraron dispositivos para validar"
            }
        
        # Buscar valores congelados en las últimas 24 horas
        end_time = validation_time
        start_time = end_time - timedelta(hours=24)
        
        devices_with_issues = []
        
        for device_id, codigo_interno in project_devices:
            # Obtener las últimas mediciones ordenadas por fecha para análisis de ventana deslizante
            values_query = """
                SELECT d.id_dato, d.fecha, d.valor
                FROM sensores_dev.datos AS d
                LEFT JOIN sensores_dev.variables AS v ON d.id_variable = v.id_variable
                LEFT JOIN sensores_dev.sensores AS sens ON d.id_sensor = sens.id_sensor
                LEFT JOIN sensores_dev.sensores_tipo AS st ON sens.id_sensor_tipo = st.id_sensor_tipo
                LEFT JOIN sensores_dev.sensores_en_dispositivo AS sed ON sens.id_sensor = sed.id_sensor
                LEFT JOIN sensores_dev.dispositivos AS disp ON sed.id_dispositivo = disp.id_dispositivo
                WHERE disp.id_dispositivo = %s
                AND CONCAT(st.modelo, ' [', v.descripcion, ' (', v.unidad, ')]') = %s
                AND d.fecha >= %s AND d.fecha <= %s
                AND d.valor IS NOT NULL
                ORDER BY d.fecha ASC
            """
            
            cursor.execute(values_query, [device_id, parameter, start_time, end_time])
            all_values = cursor.fetchall()
            
            if len(all_values) < ventana_muestras:
                # No hay suficientes datos para analizar
                continue
            
            stuck_periods = []
            
            # Análisis de ventana deslizante
            for i in range(len(all_values) - ventana_muestras + 1):
                # Extraer ventana de valores
                window_data = all_values[i:i + ventana_muestras]
                
                # Extraer solo los valores (índice 2 es el valor)
                window_values = [row[2] for row in window_data]
                
                # Contar valores únicos en la ventana
                unique_values = set()
                for val in window_values:
                    # Para valores numéricos, convertir a float para comparación
                    try:
                        unique_values.add(float(val))
                    except (ValueError, TypeError):
                        # Para valores no numéricos (boolean, string), agregar tal como está
                        unique_values.add(str(val).lower())
                
                # Verificar si excede el máximo de valores únicos permitidos
                if len(unique_values) <= max_unicos:
                    # Encontrado período de valor congelado
                    period_start = window_data[0][1]  # fecha del primer elemento
                    period_end = window_data[-1][1]   # fecha del último elemento
                    stuck_value = list(unique_values)[0] if len(unique_values) == 1 else "múltiples"
                    
                    stuck_periods.append({
                        "periodo_inicio": period_start.isoformat(),
                        "periodo_fin": period_end.isoformat(),
                        "valor_congelado": stuck_value,
                        "muestras_analizadas": len(window_data),
                        "valores_unicos_encontrados": len(unique_values),
                        "ventana_datos": [
                            {
                                "fecha": row[1].isoformat(),
                                "valor": row[2],
                                "id_dato": row[0]
                            } for row in window_data
                        ]
                    })
            
            # Consolidar períodos superpuestos para evitar duplicados
            if stuck_periods:
                consolidated_periods = []
                current_period = stuck_periods[0]
                
                for next_period in stuck_periods[1:]:
                    # Si los períodos se superponen o son consecutivos, consolidar
                    if (current_period["valor_congelado"] == next_period["valor_congelado"] and
                        abs((datetime.fromisoformat(next_period["periodo_inicio"]) - 
                             datetime.fromisoformat(current_period["periodo_fin"])).total_seconds()) < 3600):
                        # Extender el período actual
                        current_period["periodo_fin"] = next_period["periodo_fin"]
                        current_period["muestras_analizadas"] += next_period["muestras_analizadas"]
                    else:
                        # Agregar período actual y comenzar uno nuevo
                        consolidated_periods.append(current_period)
                        current_period = next_period
                
                # Agregar el último período
                consolidated_periods.append(current_period)
                
                devices_with_issues.append({
                    "dispositivo_id": device_id,
                    "codigo_interno": codigo_interno,
                    "parametro": parameter,
                    "stuck_periods_count": len(consolidated_periods),
                    "total_measurements_analyzed": len(all_values),
                    "stuck_periods": consolidated_periods[:5]  # Limitar a los primeros 5 para el reporte
                })
        
        # Enviar correo si hay dispositivos con problemas
        email_results = []
        if devices_with_issues:
            # Obtener destinatarios desde la configuración de la alerta
            alert_emails = alert.get("email", [])
            if isinstance(alert_emails, str):
                alert_emails = [alert_emails]
            
            for device_issue in devices_with_issues:
                email_subject = f"ALERTA: Valor congelado detectado - {device_issue['codigo_interno']}"
                email_body = f"""
ALERTA DE VALOR CONGELADO

Dispositivo: {device_issue['codigo_interno']} (ID: {device_issue['dispositivo_id']})
Parámetro: {device_issue['parametro']}
Proyecto: {project_id}

DETALLES:
- Períodos de valores congelados detectados: {device_issue['stuck_periods_count']}
- Total de mediciones analizadas: {device_issue['total_measurements_analyzed']}
- Configuración de alerta:
  * Ventana de muestras: {ventana_muestras}
  * Máximo valores únicos permitidos: {max_unicos}

PERÍODOS DETECTADOS:
"""
                
                for i, period in enumerate(device_issue['stuck_periods'][:3], 1):
                    email_body += f"""
Período {i}:
- Inicio: {period['periodo_inicio']}
- Fin: {period['periodo_fin']}
- Valor congelado: {period['valor_congelado']}
- Valores únicos encontrados: {period['valores_unicos_encontrados']}
"""
                
                email_body += f"\nFecha de validación: {validation_time.isoformat()}"
                
                # Enviar correo
                email_result = send_email_alert(
                    TITULO=email_subject,
                    PROYECTO_ID=project_id,
                    CODIGO_INTERNO=device_issue['codigo_interno'],
                    FECHA=validation_time.isoformat(),
                    receivers=alert_emails
                )
                
                email_results.append({
                    "dispositivo": device_issue['codigo_interno'],
                    "email_result": email_result
                })
        
        # Actualizar fecha de última validación en la alerta
        _update_alert_last_validation(alert["id"], validation_time)
        
        # Calcular estadísticas de correos enviados
        total_emails_sent = sum(result["email_result"].get("emails_sent", 0) for result in email_results)
        total_emails_failed = sum(result["email_result"].get("emails_failed", 0) for result in email_results)
        
        return {
            "status": "success",
            "alert_id": alert["id"],
            "validation_type": "stuck_value",
            "project_id": project_id,
            "parameter": parameter,
            "stuck_config": {
                "ventana_muestras": ventana_muestras,
                "max_unicos": max_unicos
            },
            "validation_time": validation_time.isoformat(),
            "period_checked": f"{start_time.isoformat()} a {end_time.isoformat()}",
            "total_devices_checked": len(project_devices),
            "devices_with_issues": len(devices_with_issues),
            "total_stuck_periods": sum(device["stuck_periods_count"] for device in devices_with_issues),
            "issues_found": devices_with_issues,
            "email_notifications": {
                "total_emails_sent": total_emails_sent,
                "total_emails_failed": total_emails_failed,
                "devices_notified": len(email_results),
                "email_details": email_results
            }
        }
        
    except mysql.connector.Error as e:
        return {
            "status": "error",
            "message": f"Error de base de datos: {str(e)}"
        }
    except Exception as e:
        return {
            "status": "error",
            "message": f"Error inesperado: {str(e)}"
        }
    finally:
        try:
            if 'cursor' in locals():
                cursor.close()
            if 'conn' in locals() and conn.is_connected():
                conn.close()
        except Exception:
            pass

def _validate_cross_parameter_rule_for_alert(alert):
    """
    Valida relaciones lógicas entre dos parámetros del mismo dispositivo
    
    Configuración esperada en alert["config"]:
    {
        "izq": "parametro_izquierdo",     # Nombre del parámetro izquierdo
        "relacion": "<=",                 # Operador de comparación (<, >, <=, >=, =, !=)
        "der": "parametro_derecho"        # Nombre del parámetro derecho
    }
    
    Args:
        alert: Objeto de alerta con la configuración
    
    Returns:
        dict: Resultado de la validación
    """
    try:
        config = _get_db_config()
        conn = mysql.connector.connect(**config)
        cursor = conn.cursor()
        
        project_id = alert["projectId"]
        
        # Obtener configuración de la regla entre parámetros
        alert_config = alert.get("config", {})
        parametro_izq = alert_config.get("izq")
        relacion = alert_config.get("relacion")
        parametro_der = alert_config.get("der")
        
        # Validar configuración
        if not parametro_izq or not parametro_der:
            return {
                "status": "error",
                "message": "Se requieren los parámetros 'izq' y 'der' en la configuración"
            }
        
        if not relacion:
            return {
                "status": "error",
                "message": "Se requiere especificar la 'relacion' en la configuración"
            }
        
        # Validar operador
        operadores_validos = ["<", ">", "<=", ">=", "=", "!="]
        if relacion not in operadores_validos:
            return {
                "status": "error",
                "message": f"Operador '{relacion}' no válido. Use uno de: {operadores_validos}"
            }
        
        if parametro_izq == parametro_der:
            return {
                "status": "error",
                "message": "Los parámetros izquierdo y derecho no pueden ser iguales"
            }

        validation_time = datetime.now()
        
        # Determinar dispositivos a validar
        if alert.get("applyToAllDevices", True):
            device_query = """
                SELECT id_dispositivo, codigo_interno 
                FROM sensores_dev.dispositivos 
                WHERE id_proyecto = %s
            """
            device_params = [project_id]
        else:
            target_devices = alert.get("targetDevices", [])
            if not target_devices:
                return {
                    "status": "error",
                    "message": "No se especificaron dispositivos objetivo"
                }
            
            device_placeholders = ','.join(['%s'] * len(target_devices))
            device_query = f"""
                SELECT id_dispositivo, codigo_interno 
                FROM sensores_dev.dispositivos 
                WHERE id_proyecto = %s AND codigo_interno IN ({device_placeholders})
            """
            device_params = [project_id] + list(target_devices)
        
        cursor.execute(device_query, device_params)
        project_devices = cursor.fetchall()
        
        if not project_devices:
            return {
                "status": "error",
                "message": "No se encontraron dispositivos para validar"
            }
        
        # Buscar violaciones de la regla en las últimas 24 horas
        end_time = validation_time
        start_time = end_time - timedelta(hours=24)
        
        devices_with_issues = []
        
        for device_id, codigo_interno in project_devices:
            # Consulta para obtener pares de valores de ambos parámetros en la misma fecha/hora
            cross_param_query = """
                SELECT 
                    d1.fecha,
                    d1.valor as valor_izq,
                    d2.valor as valor_der,
                    d1.id_dato as id_dato_izq,
                    d2.id_dato as id_dato_der
                FROM sensores_dev.datos AS d1
                INNER JOIN sensores_dev.datos AS d2 ON d1.fecha = d2.fecha
                LEFT JOIN sensores_dev.variables AS v1 ON d1.id_variable = v1.id_variable
                LEFT JOIN sensores_dev.sensores AS sens1 ON d1.id_sensor = sens1.id_sensor
                LEFT JOIN sensores_dev.sensores_tipo AS st1 ON sens1.id_sensor_tipo = st1.id_sensor_tipo
                LEFT JOIN sensores_dev.sensores_en_dispositivo AS sed1 ON sens1.id_sensor = sed1.id_sensor
                LEFT JOIN sensores_dev.dispositivos AS disp1 ON sed1.id_dispositivo = disp1.id_dispositivo
                LEFT JOIN sensores_dev.variables AS v2 ON d2.id_variable = v2.id_variable
                LEFT JOIN sensores_dev.sensores AS sens2 ON d2.id_sensor = sens2.id_sensor
                LEFT JOIN sensores_dev.sensores_tipo AS st2 ON sens2.id_sensor_tipo = st2.id_sensor_tipo
                LEFT JOIN sensores_dev.sensores_en_dispositivo AS sed2 ON sens2.id_sensor = sed2.id_sensor
                LEFT JOIN sensores_dev.dispositivos AS disp2 ON sed2.id_dispositivo = disp2.id_dispositivo
                WHERE disp1.id_dispositivo = %s
                AND disp2.id_dispositivo = %s
                AND CONCAT(st1.modelo, ' [', v1.descripcion, ' (', v1.unidad, ')]') = %s
                AND CONCAT(st2.modelo, ' [', v2.descripcion, ' (', v2.unidad, ')]') = %s
                AND d1.fecha >= %s AND d1.fecha <= %s
                AND d1.valor IS NOT NULL AND d2.valor IS NOT NULL
                AND d1.valor != '' AND d2.valor != ''
                ORDER BY d1.fecha DESC
            """
            
            cursor.execute(cross_param_query, [
                device_id, device_id, 
                parametro_izq, parametro_der,
                start_time, end_time
            ])
            param_pairs = cursor.fetchall()
            
            if not param_pairs:
                # No hay datos suficientes para comparar ambos parámetros
                continue
            
            rule_violations = []
            
            # Evaluar cada par de valores según la relación especificada
            for fecha, valor_izq, valor_der, id_dato_izq, id_dato_der in param_pairs:
                try:
                    # Convertir valores a float para comparación numérica
                    val_izq = float(valor_izq)
                    val_der = float(valor_der)
                    
                    # Evaluar la relación
                    relation_result = False
                    if relacion == "<":
                        relation_result = val_izq < val_der
                    elif relacion == ">":
                        relation_result = val_izq > val_der
                    elif relacion == "<=":
                        relation_result = val_izq <= val_der
                    elif relacion == ">=":
                        relation_result = val_izq >= val_der
                    elif relacion == "=":
                        relation_result = abs(val_izq - val_der) < 0.0001  # Tolerancia para flotantes
                    elif relacion == "!=":
                        relation_result = abs(val_izq - val_der) >= 0.0001
                    
                    # Si la relación NO se cumple, es una violación
                    if not relation_result:
                        rule_violations.append({
                            "fecha": fecha.isoformat(),
                            "valor_izquierdo": val_izq,
                            "valor_derecho": val_der,
                            "relacion_esperada": f"{parametro_izq} {relacion} {parametro_der}",
                            "relacion_actual": f"{val_izq} {relacion} {val_der}",
                            "cumple_relacion": False,
                            "id_dato_izq": id_dato_izq,
                            "id_dato_der": id_dato_der
                        })
                
                except (ValueError, TypeError):
                    # Error al convertir a float, saltar esta comparación
                    continue
            
            if rule_violations:
                devices_with_issues.append({
                    "dispositivo_id": device_id,
                    "codigo_interno": codigo_interno,
                    "parametro_izquierdo": parametro_izq,
                    "parametro_derecho": parametro_der,
                    "relacion": relacion,
                    "rule_violations_count": len(rule_violations),
                    "total_comparisons": len(param_pairs),
                    "rule_violations": rule_violations[:10]  # Limitar a primeras 10 violaciones
                })
        
        # Enviar correo si hay dispositivos con problemas
        email_results = []
        if devices_with_issues:
            # Obtener destinatarios desde la configuración de la alerta
            alert_emails = alert.get("email", [])
            if isinstance(alert_emails, str):
                alert_emails = [alert_emails]
            
            for device_issue in devices_with_issues:
                email_subject = f"ALERTA: Violación de regla entre parámetros - {device_issue['codigo_interno']}"
                email_body = f"""
ALERTA DE REGLA ENTRE PARÁMETROS

Dispositivo: {device_issue['codigo_interno']} (ID: {device_issue['dispositivo_id']})
Proyecto: {project_id}

REGLA VIOLADA:
{device_issue['parametro_izquierdo']} {device_issue['relacion']} {device_issue['parametro_derecho']}

ESTADÍSTICAS:
- Violaciones detectadas: {device_issue['rule_violations_count']}
- Comparaciones totales realizadas: {device_issue['total_comparisons']}
- Porcentaje de violaciones: {(device_issue['rule_violations_count'] / device_issue['total_comparisons'] * 100):.2f}%

PRIMERAS VIOLACIONES DETECTADAS:
"""
                
                for i, violation in enumerate(device_issue['rule_violations'][:5], 1):
                    email_body += f"""
Violación {i}:
- Fecha: {violation['fecha']}
- {device_issue['parametro_izquierdo']}: {violation['valor_izquierdo']}
- {device_issue['parametro_derecho']}: {violation['valor_derecho']}
- Resultado: {violation['valor_izquierdo']} {device_issue['relacion']} {violation['valor_derecho']} = {violation['cumple_relacion']}
"""
                
                email_body += f"\nFecha de validación: {validation_time.isoformat()}"
                
                # Enviar correo
                email_result = send_email_alert(
                    TITULO=email_subject,
                    PROYECTO_ID=project_id,
                    CODIGO_INTERNO=device_issue['codigo_interno'],
                    FECHA=validation_time.isoformat(),
                    receivers=alert_emails
                )
                
                email_results.append({
                    "dispositivo": device_issue['codigo_interno'],
                    "email_result": email_result
                })
        
        # Actualizar fecha de última validación en la alerta
        _update_alert_last_validation(alert["id"], validation_time)
        
        # Calcular estadísticas de correos enviados
        total_emails_sent = sum(result["email_result"].get("emails_sent", 0) for result in email_results)
        total_emails_failed = sum(result["email_result"].get("emails_failed", 0) for result in email_results)
        
        return {
            "status": "success",
            "alert_id": alert["id"],
            "validation_type": "cross_parameter_rule",
            "project_id": project_id,
            "rule_config": {
                "parametro_izquierdo": parametro_izq,
                "relacion": relacion,
                "parametro_derecho": parametro_der,
                "regla_completa": f"{parametro_izq} {relacion} {parametro_der}"
            },
            "validation_time": validation_time.isoformat(),
            "period_checked": f"{start_time.isoformat()} a {end_time.isoformat()}",
            "total_devices_checked": len(project_devices),
            "devices_with_issues": len(devices_with_issues),
            "total_rule_violations": sum(device["rule_violations_count"] for device in devices_with_issues),
            "issues_found": devices_with_issues,
            "email_notifications": {
                "total_emails_sent": total_emails_sent,
                "total_emails_failed": total_emails_failed,
                "devices_notified": len(email_results),
                "email_details": email_results
            }
        }
        
    except mysql.connector.Error as e:
        return {
            "status": "error",
            "message": f"Error de base de datos: {str(e)}"
        }
    except Exception as e:
        return {
            "status": "error",
            "message": f"Error inesperado: {str(e)}"
        }
    finally:
        try:
            if 'cursor' in locals():
                cursor.close()
            if 'conn' in locals() and conn.is_connected():
                conn.close()
        except Exception:
            pass


@alertas_bp.route("/validarAlerta", methods=["POST"])
def validar_alerta():
    """
    Ejecuta la validación para una alerta específica por su ID
    
    Body JSON esperado:
    {
        "alertId": "alert_1760472961990"
    }
    """
    if not request.is_json:
        return make_response(jsonify({"error": "Se requiere JSON en el cuerpo"}), 400)

    payload = request.get_json()
    if not isinstance(payload, dict):
        return make_response(jsonify({"error": "Se requiere un objeto JSON"}), 400)
    
    alert_id = payload.get("alertId")
    if not alert_id:
        return make_response(jsonify({"error": "Se requiere alertId"}), 400)
    
    # Buscar la alerta en el archivo
    alerts = _read_alerts()
    target_alert = None
    
    for alert in alerts:
        if alert.get("id") == alert_id:
            target_alert = alert
            break
    
    if not target_alert:
        return make_response(jsonify({"error": f"No se encontró alerta con ID: {alert_id}"}), 404)
    
    # Verificar que la alerta esté activa
    if not target_alert.get("active", True):
        return make_response(jsonify({"error": "La alerta está inactiva"}), 400)
    
    # Ejecutar validación según el tipo
    validation_id = target_alert.get("validationId")
    if validation_id == "missing_value":
        result = _validate_missing_value_for_alert(target_alert)
    elif validation_id == "range_threshold":
        result = _validate_range_threshold_for_alert(target_alert)
    elif validation_id == "between_range":
        result = _validate_between_range_for_alert(target_alert)
    elif validation_id == "rate_of_change":
        result = _validate_rate_of_change_for_alert(target_alert)
    elif validation_id == "stuck_value":
        result = _validate_stuck_value_for_alert(target_alert)
    elif validation_id == "cross_parameter_rule":
        result = _validate_cross_parameter_rule_for_alert(target_alert)
    else:
        return make_response(jsonify({"error": f"Tipo de validación no soportado: {validation_id}"}), 400)
    
    if result["status"] == "error":
        return make_response(jsonify(result), 500)
    
    return make_response(jsonify(result), 200)

@alertas_bp.route("/validarTodasLasAlertas", methods=["GET"])
def validar_todas_las_alertas():
    """
    Ejecuta la validación para todas las alertas activas
    """
    alerts = _read_alerts()
    active_alerts = [alert for alert in alerts if alert.get("active", True)]
    
    if not active_alerts:
        return make_response(jsonify({
            "status": "success",
            "message": "No hay alertas activas para validar"
        }), 200)
    
    results = []
    
    for alert in active_alerts:
        validation_id = alert.get("validationId")
        if validation_id == "missing_value":
            result = _validate_missing_value_for_alert(alert)
        elif validation_id == "range_threshold":
            result = _validate_range_threshold_for_alert(alert)
        elif validation_id == "between_range":
            result = _validate_between_range_for_alert(alert)
        elif validation_id == "rate_of_change":
            result = _validate_rate_of_change_for_alert(alert)
        elif validation_id == "stuck_value":
            result = _validate_stuck_value_for_alert(alert)
        elif validation_id == "cross_parameter_rule":
            result = _validate_cross_parameter_rule_for_alert(alert)
        else:
            result = {
                "status": "error",
                "message": f"Tipo de validación no soportado: {validation_id}"
            }
        
        results.append({
            "alert_id": alert["id"],
            "result": result
        })
    
    return make_response(jsonify({
        "status": "success",
        "total_alerts_processed": len(results),
        "validation_results": results
    }), 200)

@alertas_bp.route("/validarAlertasPorProyecto", methods=["POST"])
def validar_alertas_por_proyecto():
    """
    Ejecuta la validación para todas las alertas activas de un proyecto específico
    
    Body JSON esperado:
    {
        "projectId": "1"
    }
    """
    if not request.is_json:
        return make_response(jsonify({"error": "Se requiere JSON en el cuerpo"}), 400)

    payload = request.get_json()
    if not isinstance(payload, dict):
        return make_response(jsonify({"error": "Se requiere un objeto JSON"}), 400)
    
    project_id = payload.get("projectId")
    if not project_id:
        return make_response(jsonify({"error": "Se requiere projectId"}), 400)
    
    alerts = _read_alerts()
    project_alerts = [
        alert for alert in alerts 
        if alert.get("active", True) and alert.get("projectId") == str(project_id)
    ]
    
    if not project_alerts:
        return make_response(jsonify({
            "status": "success",
            "message": f"No hay alertas activas para el proyecto {project_id}"
        }), 200)
    
    results = []
    
    for alert in project_alerts:
        validation_id = alert.get("validationId")
        if validation_id == "missing_value":
            result = _validate_missing_value_for_alert(alert)
        elif validation_id == "range_threshold":
            result = _validate_range_threshold_for_alert(alert)
        elif validation_id == "between_range":
            result = _validate_between_range_for_alert(alert)
        elif validation_id == "rate_of_change":
            result = _validate_rate_of_change_for_alert(alert)
        elif validation_id == "stuck_value":
            result = _validate_stuck_value_for_alert(alert)
        elif validation_id == "cross_parameter_rule":
            result = _validate_cross_parameter_rule_for_alert(alert)
        else:
            result = {
                "status": "error",
                "message": f"Tipo de validación no soportado: {validation_id}"
            }
        
        results.append({
            "alert_id": alert["id"],
            "result": result
        })
    
    return make_response(jsonify({
        "status": "success",
        "project_id": project_id,
        "total_alerts_processed": len(results),
        "validation_results": results
    }), 200)


def send_email_alert(TITULO, PROYECTO_ID, CODIGO_INTERNO, FECHA, receivers=None):
    """
    Envía correo de alerta a múltiples destinatarios
    
    Args:
        TITULO: Título del correo
        PROYECTO_ID: ID del proyecto
        CODIGO_INTERNO: Código interno del dispositivo
        FECHA: Fecha del problema
        receivers: Lista de correos destinatarios (opcional, usa default si no se proporciona)
    """
    # Servidor Cmas
    # servidorcmas@gmail.com
    
    sender = "servidorcmas@gmail.com"
    
    # Si no se proporcionan destinatarios, usar el default
    if receivers is None:
        receivers = ["dkressing@udd.cl"]
    
    # Asegurar que receivers sea una lista
    if isinstance(receivers, str):
        receivers = [receivers]
    
    subject = f"{TITULO}"
    body = f"{TITULO}"
    
    # Crear mensaje para cada destinatario
    success_count = 0
    error_count = 0
    
    try:
        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()
            # server.login(sender, "Investigacion2023")
            server.login(sender, "yniu gfrb bsls digo")
            
            for receiver in receivers:
                try:
                    msg = MIMEText(body)
                    msg["Subject"] = subject
                    msg["From"] = sender
                    msg["To"] = receiver
                    
                    server.sendmail(sender, receiver, msg.as_string())
                    success_count += 1
                    print(f"{datetime.now()}: Correo de alerta enviado a {receiver}")
                    
                except Exception as e:
                    error_count += 1
                    print(f"{datetime.now()}: ERROR al enviar correo a {receiver} ❌ - {e}")
                    
    except Exception as e:
        print(f"{datetime.now()}: ERROR al conectar con servidor de correo ❌ - {e}")
        return {
            "success": False,
            "error": str(e),
            "total_recipients": len(receivers),
            "emails_sent": 0,
            "emails_failed": len(receivers)
        }
    
    return {
        "success": success_count > 0,
        "total_recipients": len(receivers),
        "emails_sent": success_count,
        "emails_failed": error_count,
        "recipients": receivers
    }
