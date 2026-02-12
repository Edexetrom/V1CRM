from flask import Flask, request, jsonify, send_file, send_from_directory, abort
from flask_cors import CORS
from data_handler import DataHandler
import logging
import threading
import os
import time
import json

# --- CONFIGURACIÓN DE ENTORNO Y ZONA HORARIA ---
os.environ['TZ'] = 'America/Mexico_City'
if hasattr(time, 'tzset'):
    time.tzset()

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 30 * 1024 * 1024 
CORS(app)

handler = DataHandler()

# --- HILO TRABAJADOR (WORKER) PARA RESPALDO EN LA NUBE ---
def start_worker():
    def run():
        logger.info("Hilo Trabajador (Worker) v10.3 activo - Sincronización en segundo plano iniciada.")
        while True:
            try:
                handler.process_queue_step()
            except Exception as e:
                logger.error(f"Error en worker de sincronización: {e}")
            time.sleep(5)
    
    thread = threading.Thread(target=run, daemon=True)
    thread.start()

start_worker()

# --- ENDPOINTS DE ACCESO Y SEGURIDAD ---

@app.route('/api/agents', methods=['GET'])
def get_agents():
    """Obtiene la lista de nombres de asesoras para los dropdowns."""
    try:
        return jsonify(handler.get_agents_list())
    except Exception as e:
        logger.error(f"Error en get_agents: {e}")
        return jsonify([]), 500

@app.route('/api/auditors', methods=['GET'])
def get_auditors_list_api():
    """SOLUCIÓN AL 404: Obtiene la lista de nombres de auditores para el login."""
    try:
        return jsonify(handler.get_auditors_list())
    except Exception as e:
        logger.error(f"Error en get_auditors_list_api: {e}")
        return jsonify([]), 500

@app.route('/api/login', methods=['POST'])
def login():
    """Validación de credenciales de asesoras con Auto-Registro."""
    try:
        data = request.json
        name = data.get('nombre')
        password = str(data.get('password')).strip()
        
        if not name or not password:
            return jsonify({"status": "error", "message": "Credenciales incompletas"}), 400

        ws = handler.workbook.worksheet("AsesorasActivas")
        agents = ws.get_all_records()
        agent = next((a for a in agents if (a.get('nombre') or a.get('Nombre')) == name), None)
        
        if agent:
            sheet_pass = str(agent.get('password') or agent.get('Contraseña') or '').strip()
            
            # CASO 1: No hay contraseña -> Se registra la nueva
            if not sheet_pass or sheet_pass.lower() == "none" or sheet_pass == "":
                logger.info(f"Registrando primera contraseña para Asesora: {name}")
                if handler.set_agent_password(name, password):
                    return jsonify({"status": "success", "nombre": name, "message": "Clave vinculada exitosamente."})
                else:
                    return jsonify({"status": "error", "message": "Error al vincular clave en la nube."}), 500
            
            # CASO 2: Hay contraseña -> Se valida
            if sheet_pass == password:
                return jsonify({"status": "success", "nombre": name})
                
        return jsonify({"status": "error", "message": "Clave incorrecta o usuario no hallado."}), 401
    except Exception as e:
        logger.error(f"Error en login: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/login-audit', methods=['POST'])
def login_audit():
    """Acceso al Panel de Auditoría con Auto-Registro."""
    try:
        data = request.json
        name = data.get('nombre')
        password = str(data.get('password')).strip()
        
        ws = handler.workbook.worksheet("Auditores")
        auditors = ws.get_all_records()
        auditor = next((a for a in auditors if (a.get('Nombre') or a.get('nombre')) == name), None)
        
        if not auditor: 
            return jsonify({"status": "error", "message": "Auditor no hallado en la lista."}), 404
        
        correct_pass = str(auditor.get('Contraseña') or auditor.get('password') or '').strip()
        permisos = auditor.get('Permisos') or auditor.get('permisos') or 'Visualizador'

        # CASO 1: No hay contraseña en la celda -> Auto-registro
        if not correct_pass or correct_pass.lower() == "none" or correct_pass == "":
            logger.info(f"Registrando primera contraseña para Auditor: {name}")
            if handler.set_auditor_password(name, password):
                return jsonify({"status": "success", "nombre": name, "permisos": permisos, "message": "Clave maestra establecida."})
            else:
                return jsonify({"status": "error", "message": "Error al establecer clave de auditor."}), 500

        # CASO 2: Validación estándar
        if correct_pass == password:
            return jsonify({
                "status": "success", 
                "nombre": name, 
                "permisos": permisos
            })
        return jsonify({"status": "error", "message": "Clave maestra incorrecta."}), 401
    except Exception as e:
        logger.error(f"Error en login-audit: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

# --- ENDPOINTS DE OPERACIÓN (EXPEDIENTES) ---

@app.route('/api/clients', methods=['GET'])
def get_clients():
    asesora = request.args.get('asesora')
    if not asesora:
        return jsonify([]), 400
    return jsonify(handler.get_clients_for_agent(asesora))

@app.route('/api/add-client', methods=['POST'])
def add_client():
    try:
        data = request.json
        success, message = handler.enqueue_client_data("ADD", data)
        return jsonify({"status": "success" if success else "error", "message": message})
    except Exception as e:
        logger.error(f"Error registrando cliente: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/update-client-advanced', methods=['POST'])
def update_client():
    try:
        data = request.json
        success, message = handler.enqueue_client_data("UPDATE", data)
        return jsonify({"status": "success" if success else "error", "message": message})
    except Exception as e:
        logger.error(f"Error actualizando gestión: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/rename-client', methods=['POST', 'OPTIONS'])
def rename_client():
    """Nuevo endpoint para renombrado por parte de usuarios Superior."""
    if request.method == 'OPTIONS':
        return jsonify({"status": "ok"}), 200
    try:
        data = request.json
        old_name = data.get('old_name')
        new_name = data.get('new_name')
        canal = data.get('canal')
        
        if not old_name or not new_name or not canal:
            return jsonify({"status": "error", "message": "Datos de renombrado incompletos."}), 400
            
        success, message = handler.rename_client_db(old_name, new_name, canal)
        return jsonify({"status": "success" if success else "error", "message": message})
    except Exception as e:
        logger.error(f"Error en rename_client: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/delete-client', methods=['POST', 'OPTIONS'])
def delete_client():
    """Nuevo endpoint para borrado espejo (SQLite + Sheets + Drive)."""
    if request.method == 'OPTIONS':
        return jsonify({"status": "ok"}), 200
    try:
        data = request.json
        name = data.get('nombre')
        canal = data.get('canal')
        imagenes_url = data.get('imagenes_url')
        
        if not name or not canal:
            return jsonify({"status": "error", "message": "Identificadores de cliente incompletos."}), 400
            
        success, message = handler.delete_client_db(name, canal, imagenes_url)
        return jsonify({"status": "success" if success else "error", "message": message})
    except Exception as e:
        logger.error(f"Error en delete_client: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

# --- ENDPOINTS DE AUDITORÍA Y MONITOREO ---

@app.route('/api/all-clients', methods=['GET'])
def get_all_clients():
    return jsonify(handler.get_all_clients())

@app.route('/api/sync-queue', methods=['GET'])
def get_sync_queue():
    try:
        conn = handler._get_conn() 
        cursor = conn.cursor()
        cursor.execute("SELECT sync_id, payload, status, created_at, type FROM sync_queue WHERE status != 'SUCCESS' ORDER BY id DESC")
        rows = cursor.fetchall()
        queue = []
        for r in rows:
            p = json.loads(r[1])
            queue.append({
                "sync_id": r[0],
                "asesora": p.get('Asesora') or p.get('asesora') or 'N/A',
                "action": f"{r[4]} - {p.get('etapa', 'Gestión')}",
                "timestamp": r[3]
            })
        conn.close()
        return jsonify(queue)
    except: return jsonify([])

@app.route('/api/journal-tail', methods=['GET'])
def get_journal_tail():
    try:
        path = handler.journal_path
        if not os.path.exists(path): return jsonify(["Journal no inicializado."])
        with open(path, "r", encoding="utf-8") as f:
            lines = f.readlines()
            return jsonify([line.strip() for line in lines[-50:]])
    except Exception as e:
        return jsonify([f"ERROR_LOG: {str(e)}"])

@app.route('/api/download-db', methods=['GET'])
def download_db():
    if os.path.exists(handler.db_path): 
        return send_file(handler.db_path, as_attachment=True)
    return "Base de datos no encontrada.", 404


# --- ENDPOINTS DE CALENDARIO ---
@app.route('/api/my-calendar', methods=['POST'])
def get_my_calendar():
    try:
        data = request.json
        agent_name = data.get('asesora', '').strip().lower()
        
        print(f"\n--- INICIO PETICIÓN CALENDARIO ---")
        print(f"DEBUG APP: Buscando a: '{agent_name}'")
        
        ws = handler.workbook.worksheet("AsesorasActivas")
        # Obtenemos los encabezados reales del Excel
        headers = ws.row_values(1)
        print(f"DEBUG APP: Encabezados detectados en Excel: {headers}")
        
        # Buscamos en qué posición están las columnas necesarias
        col_nombre_idx = -1
        col_calendar_idx = -1
        
        for i, h in enumerate(headers):
            norm_h = handler._normalize(h) # Usamos el normalizador de tu handler
            if norm_h in ['nombre', 'asesora']:
                col_nombre_idx = i
            if norm_h in ['idcalendario', 'calendarioid', 'id_calendario']:
                col_calendar_idx = i

        if col_nombre_idx == -1:
            print("DEBUG APP ERROR: No encontré ninguna columna que se llame 'Nombre' o 'Asesora'")
            return jsonify({"error": "Estructura de Excel inválida (Falta columna Nombre)"}), 500

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
            print(f"DEBUG APP: No se encontró a '{agent_name}' en los datos de las filas.")
            return jsonify({"error": f"Asesora '{agent_name}' no hallada"}), 404
            
        # Extraemos el ID del calendario
        calendar_id = ""
        if col_calendar_idx != -1 and len(agent_row) > col_calendar_idx:
            calendar_id = str(agent_row[col_calendar_idx]).strip()

        if not calendar_id or calendar_id.lower() == 'none':
            print(f"DEBUG APP: '{agent_name}' encontrada, pero su celda de ID_Calendario está vacía.")
            return jsonify([])
            
        print(f"DEBUG APP: ¡Éxito! Conectando calendario: '{calendar_id}'")
        events = handler.get_calendar_events(calendar_id)
        return jsonify(events)
        
    except Exception as e:
        print(f"DEBUG APP ERROR CRÍTICO: {str(e)}")
        return jsonify({"error": str(e)}), 500


# Obtenemos la ruta absoluta de la carpeta donde vive este archivo app.py
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

@app.route('/')
def index():
    path_completo = os.path.join(BASE_DIR, 'index.html')
    
    # --- BLOQUE DE DIAGNÓSTICO ---
    print(f"\n--- VERIFICACIÓN DE ARCHIVO ---")
    print(f"Buscando en: {path_completo}")
    
    if os.path.exists(path_completo):
        print("✅ ARCHIVO ENCONTRADO")
        return send_from_directory(BASE_DIR, 'index.html')
    else:
        print("❌ ERROR: El archivo index.html NO EXISTE en esa carpeta.")
        print(f"Archivos presentes en la carpeta: {os.listdir(BASE_DIR)}")
        # En lugar de romper (error 500), devolvemos un error 404 claro
        return f"Error: No se encontró index.html en {BASE_DIR}. Revisa la consola de Python.", 404

@app.route('/js/<path:filename>')
def serve_js(filename):
    # Buscamos los archivos dentro de la carpeta /js usando la ruta absoluta
    js_path = os.path.join(BASE_DIR, 'js')
    return send_from_directory(js_path, filename)
if __name__ == '__main__':
    logger.info("Iniciando Servidor Flask CRM Handshake v10.3...")
    app.run(host='0.0.0.0', debug=True, port=5000)