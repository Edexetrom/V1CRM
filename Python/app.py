from flask import Flask, request, jsonify
from flask_cors import CORS
from data_handler import DataHandler
import logging

# Configuración de logs
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
# Límite de 20MB para permitir múltiples capturas de pantalla
app.config['MAX_CONTENT_LENGTH'] = 20 * 1024 * 1024 
CORS(app)

handler = DataHandler()

@app.route('/api/agents', methods=['GET'])
def get_agents():
    """Obtiene la lista de asesoras"""
    try:
        agents_data = handler.get_active_agents()
        return jsonify([a['nombre'] for a in agents_data])
    except Exception as e:
        logger.error(f"Error agentes: {e}")
        return jsonify([]), 500

@app.route('/api/login', methods=['POST'])
def login():
    """Valida credenciales"""
    try:
        data = request.json
        name = data.get('nombre')
        password = str(data.get('password'))
        
        agents = handler.get_active_agents()
        agent = next((a for a in agents if a['nombre'] == name), None)
        
        if not agent:
            return jsonify({"status": "error", "message": "Asesora no encontrada"}), 404
            
        if not agent.get('password') or str(agent['password']).strip() == "":
            handler.set_agent_password(name, password)
            return jsonify({"status": "success", "nombre": name, "message": "Clave registrada"})

        if str(agent['password']) == password:
            return jsonify({"status": "success", "nombre": name})
        return jsonify({"status": "error", "message": "Clave incorrecta"}), 401
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/clients', methods=['GET'])
def get_clients():
    """Obtiene prospectos filtrados por asesora"""
    asesora = request.args.get('asesora')
    if not asesora: return jsonify([])
    return jsonify(handler.get_clients_for_agent(asesora))

@app.route('/api/add-client', methods=['POST'])
def add_client():
    """Alta de nuevo cliente con soporte multi-imagen"""
    data = request.json
    files_payload = data.get('files_payload', [])
    if handler.add_new_client(data, files_payload):
        return jsonify({"status": "success"})
    return jsonify({"status": "error", "message": "Fallo al guardar en Sheets"}), 500

@app.route('/api/update-client-advanced', methods=['POST'])
def update_client():
    """Actualización de seguimientos con soporte multi-imagen"""
    data = request.json
    if handler.update_client_advanced(data):
        return jsonify({"status": "success"})
    return jsonify({"status": "error", "message": "Error en actualización"}), 500

if __name__ == '__main__':
    app.run(debug=True, port=5000)