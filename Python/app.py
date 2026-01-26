from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
from data_handler import DataHandler
import logging
import threading
import os
import time

# Configuración de Zona Horaria para el Servidor
os.environ['TZ'] = 'America/Mexico_City'
if hasattr(time, 'tzset'):
    time.tzset()

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 30 * 1024 * 1024 
CORS(app)

handler = DataHandler()

# --- HILO TRABAJADOR (WORKER) PARA COLA SQLITE ---
def start_worker():
    def run():
        logger.info("Hilo Trabajador (Worker) LOCAL iniciado.")
        while True:
            try:
                handler.process_queue_step()
            except Exception as e:
                logger.error(f"Error en worker local: {e}")
            time.sleep(5)
    
    thread = threading.Thread(target=run, daemon=True)
    thread.start()

start_worker()

@app.route('/api/agents', methods=['GET'])
def get_agents():
    try:
        agents_data = handler.get_active_agents()
        return jsonify([a['nombre'] for a in agents_data])
    except Exception as e:
        return jsonify([]), 500

@app.route('/api/login-audit', methods=['POST'])
def login_audit():
    try:
        data = request.json
        name = data.get('nombre')
        password = str(data.get('password'))
        auditors = handler.get_auditors()
        auditor = next((a for a in auditors if a['Nombre'] == name), None)
        if not auditor: return jsonify({"status": "error", "message": "No encontrado"}), 404
        if str(auditor.get('Contraseña', '')).strip() == password:
            return jsonify({"status": "success", "nombre": name, "permisos": auditor['Permisos']})
        return jsonify({"status": "error", "message": "Clave incorrecta"}), 401
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/all-clients', methods=['GET'])
def get_all_clients():
    return jsonify(handler.get_all_clients())

@app.route('/api/download-journal', methods=['GET'])
def download_journal():
    path = handler.get_journal_path()
    if os.path.exists(path): return send_file(path, as_attachment=True)
    return jsonify({"status": "error"}), 404

@app.route('/api/download-db', methods=['GET'])
def download_db():
    path = handler.get_db_path()
    if os.path.exists(path): return send_file(path, as_attachment=True)
    return jsonify({"status": "error"}), 404

@app.route('/api/add-client', methods=['POST'])
def add_client():
    try:
        data = request.json
        success, message = handler.enqueue_client_data("ADD", data)
        return jsonify({"status": "success" if success else "error", "message": message})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/update-client-advanced', methods=['POST'])
def update_client():
    try:
        data = request.json
        success, message = handler.enqueue_client_data("UPDATE", data)
        return jsonify({"status": "success" if success else "error", "message": message})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/login', methods=['POST'])
def login():
    try:
        data = request.json
        name = data.get('nombre'); password = str(data.get('password'))
        agents = handler.get_active_agents()
        agent = next((a for a in agents if a['nombre'] == name), None)
        if agent and str(agent.get('password', '')).strip() == password:
            return jsonify({"status": "success", "nombre": name})
        return jsonify({"status": "error", "message": "Credenciales inválidas"}), 401
    except: return jsonify({"status": "error"}), 500

@app.route('/api/clients', methods=['GET'])
def get_clients():
    asesora = request.args.get('asesora')
    return jsonify(handler.get_clients_for_agent(asesora))

@app.route('/api/auditors', methods=['GET'])
def get_auditors_list():
    try:
        data = handler.get_auditors()
        return jsonify([a['Nombre'] for a in data])
    except: return jsonify([])

if __name__ == '__main__':
    app.run(debug=True, port=5000)