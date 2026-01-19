import gspread
import os
import json
import base64
import requests
import unicodedata
import logging
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

class DataHandler:
    # Configuración de URLs y Drive
    SCRIPT_URL = "https://script.google.com/macros/s/AKfycbxyr3lAA-Xykuy1S-mvGp3SdAb1ghDpdWsbHeURupBfJlO9D1xmGP12td1R7VZDAziV/exec"
    PARENT_FOLDER_ID = "1duPIhtA9Z6IObDxmANSLKA0Hw-R5Iidl"

    def __init__(self):
        self.client = None
        self.workbook = None
        self.SHEET_URL = "https://docs.google.com/spreadsheets/d/1PGyE1TN5q1tEtoH5A-wxqS27DkONkNzp-hreL3OMJZw/edit#gid=0"
        self.connect()

    def connect(self):
        """Conexión robusta a Google Sheets usando Service Account b64"""
        try:
            creds_b64 = os.getenv("GOOGLE_CREDS_BASE64")
            if creds_b64:
                missing_padding = len(creds_b64) % 4
                if missing_padding: creds_b64 += '=' * (4 - missing_padding)
                info = json.loads(base64.b64decode(creds_b64).decode('utf-8'))
                self.client = gspread.service_account_from_dict(info)
                self.workbook = self.client.open_by_url(self.SHEET_URL)
                logger.info("Conexión exitosa a Google Sheets")
        except Exception as e:
            logger.error(f"Error crítico de conexión: {e}")

    def _normalize(self, text):
        """Normaliza texto para comparaciones robustas (sin acentos, minúsculas, sin espacios extra)"""
        if not text: return ""
        text = str(text).lower().strip()
        text = "".join(c for c in unicodedata.normalize('NFD', text) if unicodedata.category(c) != 'Mn')
        # Eliminar caracteres no alfanuméricos para máxima resiliencia en encabezados
        return "".join(e for e in text if e.isalnum())

    def _ensure_columns(self, sheet, headers, required_columns):
        """Asegura que las columnas necesarias existan en la hoja"""
        updated_headers = list(headers)
        new_cols = []
        
        # Mapeo de búsqueda normalizada para evitar duplicados por formato
        norm_headers = [self._normalize(h) for h in updated_headers]
        
        for col in required_columns:
            if self._normalize(col) not in norm_headers:
                new_cols.append(col)
                updated_headers.append(col)
                norm_headers.append(self._normalize(col))
        
        if new_cols:
            logger.info(f"Auto-expansión: Agregando columnas {new_cols}")
            sheet.update('A1', [updated_headers])
            return updated_headers
        return headers

    def get_active_agents(self):
        try:
            sheet = self.workbook.worksheet("AsesorasActivas")
            return sheet.get_all_records()
        except: return []

    def get_clients_for_agent(self, agent_name):
        try:
            sheet = self.workbook.worksheet("Seguimientos")
            all_data = sheet.get_all_records()
            return [row for row in all_data if str(row.get('Asesora', '')).lower() == agent_name.lower()]
        except: return []

    def add_new_client(self, data, file_payload):
        """Registra un nuevo cliente con mapeo de columnas inteligente"""
        try:
            sheet = self.workbook.worksheet("Seguimientos")
            headers = sheet.row_values(1)
            
            # Definir qué campo del JSON va a qué columna de la hoja
            data_map = {
                'Nombre': data.get('Nombre'),
                'Canal (Tel/WhatsApp)': data.get('Canal'),
                'Fecha 1er Contacto': data.get('Fecha 1er Contacto'),
                'Nivel de Interés': data.get('Nivel de Interés'),
                'Resumen Conversación': data.get('Resumen Conversación'),
                'Fecha Próx. Contacto': data.get('Fecha Próx. Contacto'),
                'Estado Final': data.get('Estado Final', 'Seguimiento'),
                'Asesora': data.get('Asesora'),
                'Comentarios': data.get('Comentarios', '')
            }

            # Asegurar que las columnas básicas existan
            headers = self._ensure_columns(sheet, headers, data_map.keys())

            folder_url = ""
            if file_payload and file_payload.get('base64Data'):
                res = self._send_to_script(data['Nombre'], file_payload, "Inicio")
                if res and res.get('status') == 'success':
                    folder_url = res.get('folderUrl', '')
                    data_map['Imagenes'] = folder_url

            # Construir la fila emparejando por normalización
            new_row = []
            for h in headers:
                norm_h = self._normalize(h)
                found_val = ""
                for key, val in data_map.items():
                    if self._normalize(key) == norm_h:
                        found_val = val
                        break
                new_row.append(str(found_val))

            sheet.append_row(new_row)
            return True
        except Exception as e:
            logger.error(f"Error al agregar cliente: {e}")
            return False

    def update_client_advanced(self, data):
        """Actualiza expediente con mapeo de columnas inteligente"""
        try:
            sheet = self.workbook.worksheet("Seguimientos")
            headers = sheet.row_values(1)
            name = data.get('nombre_original')
            
            cell = sheet.find(name)
            if not cell: return False
            
            row_idx = cell.row
            final_updates = data.get('updates', {})
            file_payload = data.get('file_payload')

            # Asegurar que las nuevas columnas (seguimientos extra) existan
            headers = self._ensure_columns(sheet, headers, final_updates.keys())

            if file_payload and file_payload.get('base64Data'):
                res = self._send_to_script(name, file_payload, datetime.now().strftime("%H%M%S"))
                if res and res.get('status') == 'success':
                    final_updates['Imagenes'] = res.get('folderUrl')

            batch_list = []
            for key, value in final_updates.items():
                norm_key = self._normalize(key)
                # Buscar el índice de la columna mediante normalización
                col_i = next((i + 1 for i, h in enumerate(headers) if self._normalize(h) == norm_key), None)
                
                if col_i:
                    batch_list.append({
                        'range': gspread.utils.rowcol_to_a1(row_idx, col_i),
                        'values': [[str(value)]]
                    })
            
            if batch_list:
                sheet.batch_update(batch_list)
            return True
        except Exception as e:
            logger.error(f"Error actualizando expediente: {e}")
            return False

    def _send_to_script(self, client_name, file_payload, suffix):
        """Conexión con Apps Script para almacenamiento en Drive"""
        try:
            payload = {
                "parentFolderId": self.PARENT_FOLDER_ID,
                "clientName": client_name,
                "filename": f"{client_name}_{suffix}.png",
                "contentType": file_payload.get('contentType', 'image/png'),
                "base64Data": file_payload.get('base64Data')
            }
            r = requests.post(self.SCRIPT_URL, json=payload, timeout=25)
            return r.json()
        except: return None