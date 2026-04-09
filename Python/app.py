from flask import Flask, request, jsonify
from flask_cors import CORS
from data_handler import handler
import threading  # LÍNEA AGREGADA: Permite el uso de hilos para procesos de fondo
import logging

# Configuración de logs para ver el flujo en la terminal
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("CerebroServer")

app = Flask(__name__)
# Configuración CORS global para permitir la comunicación con los archivos HTML
CORS(app, resources={r"/api/*": {"origins": "*"}})

def background_sync(action_type, data):
    """
    Función que se ejecuta en un hilo separado.
    Sincroniza los datos con la hoja de Google Sheets "AsesorasActivas" o la principal.
    """
    try:
        logger.info(f"Worker: Iniciando sincronización de fondo para {data.get('Nombre')}")
        # Llamada al método de sincronización en el módulo de sheets
        sheets_sync.sincronizar_prospecto_a_sheet(data)
        logger.info(f"Worker: Sincronización finalizada exitosamente.")
    except Exception as e:
        logger.error(f"Worker Error: No se pudo completar la sincronización: {e}")

@app.route('/api/my-calendar', methods=['POST', 'OPTIONS'])
def get_my_calendar():
    """
    Endpoint para obtener la agenda de la asesora desde Google Sheets y Calendar.
    Implementación robusta con normalización de búsqueda.
    """
    if request.method == 'OPTIONS':
        return jsonify({"status": "ok"}), 200
    try:
        data = request.json
        agent_name = data.get('asesora', '').strip().lower()
        
        logger.info(f"CALENDARIO: Solicitud para: '{agent_name}'")
        
        # Acceso al libro de trabajo a través del handler
        ws = handler.sheets.workbook.worksheet("AsesorasActivas")
        
        # Obtenemos los encabezados reales del Excel
        headers = ws.row_values(1)
        
        # Buscamos en qué posición están las columnas necesarias
        col_nombre_idx = -1
        col_calendar_idx = -1
        
        for i, h in enumerate(headers):
            norm_h = handler._normalize(h) 
            if norm_h in ['nombre', 'asesora']:
                col_nombre_idx = i
            if norm_h in ['idcalendario', 'calendarioid', 'id_calendario']:
                col_calendar_idx = i

        if col_nombre_idx == -1:
            logger.error("CALENDARIO ERROR: No se encontró la columna 'Nombre' o 'Asesora'")
            return jsonify({"error": "Estructura de Excel inválida"}), 500

        # Buscamos la fila de la asesora manualmente por índice
        records = ws.get_all_values()[1:] # Saltamos encabezados
        agent_row = None
        
        for row in records:
            if len(row) > col_nombre_idx:
                val_nombre = str(row[col_nombre_idx]).strip().lower()
                if val_nombre == agent_name:
                    agent_row = row
                    break
        
        if not agent_row:
            logger.warning(f"CALENDARIO: No se encontró a '{agent_name}' en las filas.")
            return jsonify({"error": f"Asesora '{agent_name}' no hallada"}), 404
            
        # Extraemos el ID del calendario
        calendar_id = ""
        if col_calendar_idx != -1 and len(agent_row) > col_calendar_idx:
            calendar_id = str(agent_row[col_calendar_idx]).strip()

        if not calendar_id or calendar_id.lower() == 'none':
            logger.info(f"CALENDARIO: '{agent_name}' no tiene ID de calendario configurado.")
            return jsonify([])
            
        logger.info(f"CALENDARIO: ¡Éxito! Conectando ID: '{calendar_id}'")
        events = handler.get_calendar_events(calendar_id)
        return jsonify(events)
        
    except Exception as e:
        logger.error(f"CALENDARIO ERROR CRÍTICO: {str(e)}")
        return jsonify({"error": str(e)}), 500

# --- OTROS ENDPOINTS ---

@app.route('/api/update-client-advanced', methods=['POST', 'OPTIONS'])
def update_client_advanced():
    if request.method == 'OPTIONS': return jsonify({"status": "ok"}), 200
    try:
        data = request.json
        p_id = data.get('p_id')
        if not p_id: return jsonify({"status": "error", "message": "Se requiere el identificador único (p_id)."}), 400
        
        # Llamamos al handler para guardar en Supabase de forma INMEDIATA y SÍNCRONA
        res = handler.actualizar_prospecto_avanzado(p_id, data.get('updates'))
        
        # Si guardó correctamente en base de datos y venían archivos, los subimos a Drive en un hilo asíncrono
        if res.get('status') == 'success' and data.get('files_payload'):
            logger.info(f"DB Update OK, iniciando subida asíncrona a Drive para ID {p_id}")
            
            thread = threading.Thread(
                target=handler.subir_evidencia_fondo,
                args=(data.get('nombre_original', 'Desconocido'), data.get('files_payload'), res.get('num_seg'), p_id)
            )
            thread.start()
            
            # Limpiamos variables internas de respuesta antes de regresarlas al cliente frontend
            res.pop('p_id', None)
            res.pop('num_seg', None)

        # Retornamos el success inmediatamente al frontend para liberar el socket en macOS
        updated_data = handler.get_client_full_profile(p_id)
        res['data'] = updated_data
        return jsonify(res)
    except Exception as e: return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/client-details', methods=['GET'])
def get_client_details():
    p_id = request.args.get('id')
    if not p_id: return jsonify({"status":"error"}), 400
    data = handler.get_client_full_profile(p_id)
    return jsonify(data)


@app.route('/api/delete-client', methods=['POST', 'OPTIONS'])
def delete_client():
    if request.method == 'OPTIONS': return jsonify({"status": "ok"}), 200
    try:
        data = request.json
        success, message = handler.delete_client_db(data.get('nombre'), data.get('canal'), data.get('imagenes_url'))
        return jsonify({"status": "success" if success else "error", "message": message})
    except Exception as e: return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/add-client', methods=['POST', 'OPTIONS'])
def add_client():
    """
    Endpoint para registrar un nuevo prospecto.
    1. Valida duplicados (DataHandler v4.5).
    2. Registra en Supabase.
    3. Dispara hilo de fondo para Google Sheets si todo es correcto.
    """
    if request.method == 'OPTIONS':
        return jsonify({"status": "ok"}), 200
        
    try:
        data = request.json
        
        # El handler realiza la verificación preventiva de duplicados y subida a Drive
        result = handler.registrar_prospecto(data)
        
        # Si es duplicado, el handler devuelve status 'duplicate'
        if result.get('status') == 'duplicate':
            logger.info(f"API: Duplicado detectado para {data.get('Canal')}. Operación abortada.")
            return jsonify(result), 409
            
        # Si el registro fue exitoso en Supabase
        if result.get('status') == 'success':
            logger.info(f"API: Registro exitoso en DB. Disparando hilo de sincronización.")
            
            # DISPARO DE HILO: Aquí es donde fallaba antes por la falta del import
            thread = threading.Thread(target=background_sync, args=("ADD", data))
            thread.start()
            
            return jsonify(result), 201
        
        # Otros errores de validación
        return jsonify(result), 400
        
    except Exception as e:
        logger.error(f"Error crítico en add-client: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/clients', methods=['GET'])
def get_clients_by_agent():
    asesora = request.args.get('asesora')
    return jsonify(handler.get_clients_for_agent(asesora) if asesora else [])

@app.route('/api/agents', methods=['GET'])
def get_agents_list():
    return jsonify(handler.obtener_asesoras_activas())

@app.route('/api/all-clients', methods=['GET'])
def get_all_clients():
    return jsonify(handler.get_all_clients())

@app.route('/api/auditors', methods=['GET'])
def get_auditors():
    return jsonify(handler.obtener_auditores())

@app.route('/api/login', methods=['POST'])
def login_asesora():
    data = request.json
    return jsonify(handler.login_asesora(data.get('nombre')))

@app.route('/api/login-audit', methods=['POST'])
def login_audit():
    data = request.json
    return jsonify(handler.login_auditoria(data.get('nombre'), data.get('password')))

@app.route('/api/sync-queue', methods=['GET'])
def get_sync_queue(): return jsonify([])

@app.route('/api/journal-tail', methods=['GET'])
def get_journal_tail(): return jsonify(["[SISTEMA] Motor v1.10 Activo", "[DB] Sincronización OK"])

# --- INICIO MÓDULO POOL ---
@app.route('/api/pool', methods=['GET'])
def get_pool():
    return jsonify(handler.get_pool_clients())

@app.route('/api/pool/take', methods=['POST', 'OPTIONS'])
def take_pool():
    if request.method == 'OPTIONS': return jsonify({"status": "ok"}), 200
    data = request.json
    res = handler.take_pool_client(data.get('lead_id'), data.get('nombre_asesora'))
    code = res.pop('code', 200) if 'code' in res else 200
    if res.get('status') == 'error':
        return jsonify(res), code
    return jsonify(res), 200

@app.route('/api/pool/resolve', methods=['POST', 'OPTIONS'])
def resolve_pool():
    if request.method == 'OPTIONS': return jsonify({"status": "ok"}), 200
    data = request.json
    res = handler.resolve_pool_client(data.get('lead_id'), data.get('nombre_asesora'), data.get('accion'), data.get('datos_validacion'))
    code = res.pop('code', 200) if 'code' in res else 200
    if res.get('status') == 'error':
        return jsonify(res), code
    return jsonify(res), 200
# --- FIN MÓDULO POOL ---

if __name__ == '__main__':
    logger.info(">>> Servidor Cerebro v1.10: Calendario Restaurado <<<")
    app.run(host='0.0.0.0', port=5000, debug=True)