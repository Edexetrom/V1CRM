from flask import Flask, request, jsonify
from flask_cors import CORS
from data_handler import DataHandler
import logging

# Configuración de Logging para monitoreo de tráfico
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 25 * 1024 * 1024  # Soporte para ráfagas de imágenes
CORS(app)

handler = DataHandler()

@app.route('/api/agents', methods=['GET'])
def get_agents():
    try:
        agents_data = handler.get_active_agents()
        return jsonify([a['nombre'] for a in agents_data])
    except Exception as e:
        logger.error(f"Error en get_agents: {e}")
        return jsonify([]), 500

@app.route('/api/stats', methods=['GET'])
def get_stats():
    asesora = request.args.get('asesora')
    if not asesora: return jsonify({"status": "error", "message": "Falta parámetro asesora"}), 400
    return jsonify(handler.get_agent_stats(asesora))

@app.route('/api/auditors', methods=['GET'])
def get_auditors():
    try:
        audit_data = handler.get_auditors()
        return jsonify([a['Nombre'] for a in audit_data])
    except:
        return jsonify([]), 500

@app.route('/api/login-audit', methods=['POST'])
def login_audit():
    try:
        data = request.json
        name = data.get('nombre')
        password = str(data.get('password'))
        auditors = handler.get_auditors()
        auditor = next((a for a in auditors if a['Nombre'] == name), None)
        
        if not auditor: 
            return jsonify({"status": "error", "message": "Auditor no encontrado"}), 404
        
        stored_password = str(auditor.get('Contraseña', '')).strip()
        if not stored_password or stored_password == '0' or stored_password.lower() == 'none':
            if handler.set_auditor_password(name, password, "Visualizador"):
                return jsonify({"status": "success", "nombre": name, "permisos": "Visualizador", "message": "Auto-registro exitoso"})
            else:
                return jsonify({"status": "error", "message": "Error al registrar credenciales"}), 500

        if stored_password == password:
            return jsonify({"status": "success", "nombre": name, "permisos": auditor['Permisos']})
            
        return jsonify({"status": "error", "message": "Clave incorrecta"}), 401
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/all-clients', methods=['GET'])
def get_all_clients():
    try:
        data = handler.get_all_clients()
        return jsonify(data)
    except Exception as e:
        return jsonify({"status": "error", "message": "Error al obtener clientes"}), 500

@app.route('/api/delete-client', methods=['POST'])
def delete_client():
    try:
        data = request.json
        nombre = data.get('nombre')
        if handler.delete_client_and_folder(nombre):
            return jsonify({"status": "success", "message": "Expediente eliminado correctamente"})
        return jsonify({"status": "error", "message": "No se pudo eliminar el expediente"}), 500
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/login', methods=['POST'])
def login():
    try:
        data = request.json
        name = data.get('nombre')
        password = str(data.get('password'))
        agents = handler.get_active_agents()
        agent = next((a for a in agents if a['nombre'] == name), None)
        
        if not agent: 
            return jsonify({"status": "error", "message": "Asesora no encontrada"}), 404
        
        stored_password = str(agent.get('password', '')).strip()
        if not stored_password or stored_password == '0' or stored_password.lower() == 'none':
            if handler.set_agent_password(name, password):
                return jsonify({"status": "success", "nombre": name, "message": "Clave registrada correctamente"})
            else:
                return jsonify({"status": "error", "message": "Fallo en auto-registro de clave"}), 500

        if stored_password == password: 
            return jsonify({"status": "success", "nombre": name})
            
        return jsonify({"status": "error", "message": "Clave incorrecta"}), 401
    except Exception as e: 
        return jsonify({"status": "error", "message": "Error interno del servidor"}), 500

@app.route('/api/clients', methods=['GET'])
def get_clients():
    asesora = request.args.get('asesora')
    if not asesora: return jsonify([])
    return jsonify(handler.get_clients_for_agent(asesora))

@app.route('/api/add-client', methods=['POST'])
def add_client():
    try:
        data = request.json
        success, message = handler.add_new_client(data, data.get('files_payload', []))
        if success:
            return jsonify({"status": "success", "message": message})
        return jsonify({"status": "error", "message": message}), 500
    except Exception as e:
        return jsonify({"status": "error", "message": f"Error crítico: {str(e)}"}), 500

@app.route('/api/update-client-advanced', methods=['POST'])
def update_client():
    try:
        data = request.json
        success, message = handler.update_client_advanced(data)
        if success:
            return jsonify({"status": "success", "message": message})
        return jsonify({"status": "error", "message": message}), 500
    except Exception as e:
        return jsonify({"status": "error", "message": f"Error crítico en actualización: {str(e)}"}), 500

if __name__ == '__main__':
    # Mantenemos localhost para desarrollo, ajustable para Dockploy/Producción
    app.run(debug=True, port=5000)