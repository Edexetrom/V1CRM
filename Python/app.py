from flask import Flask, request, jsonify, send_file
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

# Configuración de Logging profesional
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)
# Blindaje de capacidad: 30MB para ráfagas de imágenes en alta resolución
app.config['MAX_CONTENT_LENGTH'] = 30 * 1024 * 1024 
CORS(app)

# Inicialización del manejador de datos (SQLite + Sheets)
handler = DataHandler()

# --- HILO TRABAJADOR (WORKER) PARA COLA SQLITE ---
def start_worker():
    def run():
        logger.info("Hilo Trabajador (Worker) LOCALhost iniciado - Procesando Handshake.")
        while True:
            try:
                # Procesa un paso de la cola (Handshake individual)
                handler.process_queue_step()
            except Exception as e:
                logger.error(f"Error en worker local: {e}")
            # Ciclo de 5 segundos para optimizar cuota de API de Google
            time.sleep(5)
    
    thread = threading.Thread(target=run, daemon=True)
    thread.start()

# Iniciamos el worker al arrancar el servidor
start_worker()

# --- ENDPOINTS DE AGENTES Y LOGIN ---

@app.route('/api/agents', methods=['GET'])
def get_agents():
    """Obtiene la lista de nombres de asesoras activas."""
    try:
        agents_data = handler.get_active_agents()
        return jsonify([a['nombre'] for a in agents_data])
    except Exception as e:
        logger.error(f"Error en get_agents: {e}")
        return jsonify([]), 500

@app.route('/api/login', methods=['POST'])
def login():
    """Autenticación para Asesoras con Auto-Registro de Clave Inicial."""
    try:
        data = request.json
        name = data.get('nombre')
        password = str(data.get('password')).strip()
        
        if not name or not password:
            return jsonify({"status": "error", "message": "Datos incompletos"}), 400

        agents = handler.get_active_agents()
        agent = next((a for a in agents if a['nombre'] == name), None)
        
        if agent:
            sheet_pass = str(agent.get('password', '')).strip()
            
            # --- LÓGICA DE AUTO-REGISTRO ---
            # Si en Sheets está vacío o es "None", la clave ingresada es la nueva clave fija.
            if not sheet_pass or sheet_pass == "" or sheet_pass == "None":
                logger.info(f"Registrando clave inicial para {name}")
                success = handler.set_agent_password(name, password)
                if success:
                    return jsonify({"status": "success", "nombre": name, "message": "Clave registrada correctamente"})
                else:
                    return jsonify({"status": "error", "message": "No se pudo registrar la clave en la nube"}), 500
            
            # --- VALIDACIÓN NORMAL ---
            if sheet_pass == password:
                return jsonify({"status": "success", "nombre": name})
                
        return jsonify({"status": "error", "message": "Credenciales inválidas"}), 401
    except Exception as e:
        logger.error(f"Error en login: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

# --- FUNCIÓN: GENERACIÓN DE CONTRASEÑAS (BACKUP AUDITOR) ---

@app.route('/api/generate-passwords', methods=['POST'])
def generate_passwords():
    """Genera contraseñas masivas para asesoras que no tengan una en Sheets."""
    try:
        success, count = handler.generate_missing_passwords()
        if success:
            return jsonify({"status": "success", "message": f"Se generaron {count} contraseñas nuevas."})
        return jsonify({"status": "error", "message": "Error al procesar Sheets."})
    except Exception as e:
        logger.error(f"Error en generate-passwords: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

# --- ENDPOINTS DE CLIENTES Y GESTIÓN ---

@app.route('/api/clients', methods=['GET'])
def get_clients():
    asesora = request.args.get('asesora')
    return jsonify(handler.get_clients_for_agent(asesora))

@app.route('/api/add-client', methods=['POST'])
def add_client():
    try:
        data = request.json
        success, message = handler.enqueue_client_data("ADD", data)
        return jsonify({"status": "success" if success else "error", "message": message})
    except Exception as e:
        logger.error(f"Error en add-client: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/update-client-advanced', methods=['POST'])
def update_client():
    try:
        data = request.json
        success, message = handler.enqueue_client_data("UPDATE", data)
        return jsonify({"status": "success" if success else "error", "message": message})
    except Exception as e:
        logger.error(f"Error en update-client: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

# --- ENDPOINTS DE AUDITORÍA Y CONTROL ---

@app.route('/api/login-audit', methods=['POST'])
def login_audit():
    try:
        data = request.json
        name = data.get('nombre')
        password = str(data.get('password')).strip()
        auditors = handler.get_auditors()
        auditor = next((a for a in auditors if a['Nombre'] == name), None)
        if not auditor: return jsonify({"status": "error", "message": "Auditor no encontrado"}), 404
        if str(auditor.get('Contraseña', '')).strip() == password:
            return jsonify({
                "status": "success", 
                "nombre": name, 
                "permisos": auditor['Permisos']
            })
        return jsonify({"status": "error", "message": "Clave incorrecta"}), 401
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/auditors', methods=['GET'])
def get_auditors_list():
    try:
        data = handler.get_auditors()
        return jsonify([a['Nombre'] for a in data])
    except:
        return jsonify([])

@app.route('/api/all-clients', methods=['GET'])
def get_all_clients():
    return jsonify(handler.get_all_clients())

@app.route('/api/delete-client', methods=['POST'])
def delete_client():
    try:
        data = request.json
        nombre = data.get('nombre')
        if not nombre: return jsonify({"status": "error", "message": "Falta nombre"}), 400
        success, message = handler.delete_client(nombre)
        return jsonify({"status": "success" if success else "error", "message": message})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

# --- ENDPOINTS DE MONITOREO Y RESPALDO ---

@app.route('/api/sync-queue', methods=['GET'])
def get_sync_queue():
    try:
        conn = handler._get_conn() 
        cursor = conn.cursor()
        cursor.execute("SELECT sync_id, payload, status, created_at FROM sync_queue WHERE status != 'SUCCESS' ORDER BY id DESC")
        rows = cursor.fetchall()
        queue = []
        for r in rows:
            data = json.loads(r[1])
            queue.append({
                "sync_id": r[0],
                "asesora": data.get('Asesora', 'N/A'),
                "action": data.get('etapa', 'Registro'),
                "timestamp": r[3]
            })
        conn.close()
        return jsonify(queue)
    except:
        return jsonify([])

@app.route('/api/journal-tail', methods=['GET'])
def get_journal_tail():
    try:
        path = handler.journal_path
        if not os.path.exists(path): return jsonify([])
        with open(path, "r", encoding="utf-8") as f:
            lines = f.readlines()
            return jsonify([line.strip() for line in lines[-50:]])
    except Exception as e:
        return jsonify([f"ERROR: {str(e)}"])

@app.route('/api/system-logs', methods=['GET'])
def get_system_logs():
    try: return jsonify(handler.get_latest_system_logs(limit=50))
    except: return jsonify([]), 500

@app.route('/api/download-journal', methods=['GET'])
def download_journal():
    path = handler.journal_path
    if os.path.exists(path): return send_file(path, as_attachment=True)
    return jsonify({"status": "error", "message": "No hallado"}), 404

@app.route('/api/download-db', methods=['GET'])
def download_db():
    path = handler.db_path
    if os.path.exists(path): return send_file(path, as_attachment=True)
    return jsonify({"status": "error", "message": "No hallado"}), 404

if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True, port=5000)