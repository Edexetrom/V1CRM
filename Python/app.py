from flask import Flask, request, jsonify
from flask_cors import CORS
from data_handler import DataHandler
import logging

# Configuración de logs para mantener visibilidad de procesos
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
# Límite aumentado para payloads de imágenes base64
app.config['MAX_CONTENT_LENGTH'] = 20 * 1024 * 1024 
CORS(app)

handler = DataHandler()

@app.route('/api/agents', methods=['GET'])
def get_agents():
    """Retorna la lista de asesoras desde la hoja AsesorasActivas"""
    try:
        agents_data = handler.get_active_agents()
        return jsonify([a['nombre'] for a in agents_data])
    except Exception as e:
        logger.error(f"Error obteniendo asesoras: {e}")
        return jsonify([]), 500

@app.route('/api/login', methods=['POST'])
def login():
    """Valida credenciales o registra nueva contraseña"""
    try:
        data = request.json
        name = data.get('nombre')
        password = str(data.get('password'))
        
        agents = handler.get_active_agents()
        agent = next((a for a in agents if a['nombre'] == name), None)
        
        if not agent:
            return jsonify({"status": "error", "message": "Asesora no encontrada"}), 404
            
        # Si la asesora no tiene password en la hoja, la guardamos
        if not agent.get('password') or str(agent['password']).strip() == "":
            handler.set_agent_password(name, password)
            return jsonify({"status": "success", "nombre": name, "message": "Contraseña creada"})

        if str(agent['password']) == password:
            return jsonify({"status": "success", "nombre": name})
        else:
            return jsonify({"status": "error", "message": "Contraseña incorrecta"}), 401
    except Exception as e:
        logger.error(f"Error en login: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/clients', methods=['GET'])
def get_clients():
    """Obtiene prospectos filtrados por la asesora activa"""
    asesora = request.args.get('asesora')
    if not asesora: return jsonify([])
    return jsonify(handler.get_clients_for_agent(asesora))

@app.route('/api/add-client', methods=['POST'])
def add_client():
    """Registro de nuevo expediente con link de imagen/carpeta"""
    data = request.json
    file_payload = data.get('file_payload')
    
    # El handler ahora retorna el URL de la carpeta de Drive si se subió imagen
    success = handler.add_new_client(data, file_payload)
    if success:
        return jsonify({"status": "success"})
    return jsonify({"status": "error", "message": "Error al guardar en Google Sheets"}), 500

@app.route('/api/update-client-advanced', methods=['POST'])
def update_client():
    """Actualización de seguimientos y estados"""
    data = request.json
    if handler.update_client_advanced(data):
        return jsonify({"status": "success"})
    return jsonify({"status": "error", "message": "Error al actualizar expediente"}), 500

if __name__ == '__main__':
    app.run(debug=True, port=5000)