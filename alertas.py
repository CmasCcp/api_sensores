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
