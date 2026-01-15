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
    SCRIPT_URL = "https://script.google.com/macros/s/AKfycbxyr3lAA-Xykuy1S-mvGp3SdAb1ghDpdWsbHeURupBfJlO9D1xmGP12td1R7VZDAziV/exec"
    PARENT_FOLDER_ID = "1duPIhtA9Z6IObDxmANSLKA0Hw-R5Iidl"

    def __init__(self):
        self.client = None
        self.workbook = None
        self.SHEET_URL = "https://docs.google.com/spreadsheets/d/1PGyE1TN5q1tEtoH5A-wxqS27DkONkNzp-hreL3OMJZw/edit#gid=0"
        self.connect()

    def connect(self):
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
        if not text: return ""
        text = str(text).lower().strip()
        text = "".join(c for c in unicodedata.normalize('NFD', text) if unicodedata.category(c) != 'Mn')
        return text

    def _ensure_columns(self, sheet, headers, required_columns):
        """
        Verifica si las columnas existen. Si no, las agrega al final de la hoja.
        """
        updated_headers = list(headers)
        new_cols = []
        
        for col in required_columns:
            if col not in updated_headers:
                new_cols.append(col)
                updated_headers.append(col)
        
        if new_cols:
            logger.info(f"Agregando nuevas columnas: {new_cols}")
            # Actualizar encabezados en la fila 1
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
        try:
            sheet = self.workbook.worksheet("Seguimientos")
            headers = sheet.row_values(1)
            
            folder_url = ""
            if file_payload and file_payload.get('base64Data'):
                res = self._send_to_script(data['Nombre'], file_payload, "Inicio")
                if res and res.get('status') == 'success':
                    folder_url = res.get('folderUrl', '')

            row_map = {
                'Nombre': data.get('Nombre'),
                'Canal (Tel/WhatsApp)': data.get('Canal'),
                'Fecha 1er Contacto': data.get('Fecha 1er Contacto'),
                'Nivel de Interés': data.get('Nivel de Interés'),
                'Resumen Conversación': data.get('Resumen Conversación'),
                'Fecha Próx. Contacto': data.get('Fecha Próx. Contacto'),
                'Estado Final': data.get('Estado Final', 'Seguimiento'),
                'Asesora': data.get('Asesora'),
                'Imagenes': folder_url,
                'Comentarios': data.get('Comentarios', '')
            }

            new_row = [row_map.get(h, "") for h in headers]
            sheet.append_row(new_row)
            return True
        except Exception as e:
            logger.error(f"Error al agregar cliente: {e}")
            return False

    def update_client_advanced(self, data):
        try:
            sheet = self.workbook.worksheet("Seguimientos")
            headers = sheet.row_values(1)
            name = data.get('nombre_original')
            
            cell = sheet.find(name)
            if not cell: return False
            
            row_idx = cell.row
            final_updates = data.get('updates', {})
            file_payload = data.get('file_payload')

            # --- AUTO-EXPANSIÓN DE COLUMNAS ---
            # Verificamos si las llaves enviadas existen en headers
            headers = self._ensure_columns(sheet, headers, final_updates.keys())

            if file_payload and file_payload.get('base64Data'):
                res = self._send_to_script(name, file_payload, datetime.now().strftime("%H%M%S"))
                if res and res.get('status') == 'success':
                    final_updates['Imagenes'] = res.get('folderUrl')

            batch_list = []
            for key, value in final_updates.items():
                try:
                    col_idx = headers.index(key) + 1
                    batch_list.append({
                        'range': gspread.utils.rowcol_to_a1(row_idx, col_idx),
                        'values': [[str(value)]]
                    })
                except ValueError:
                    continue
            
            if batch_list:
                sheet.batch_update(batch_list)
            return True
        except Exception as e:
            logger.error(f"Error actualizando expediente: {e}")
            return False

    def _send_to_script(self, client_name, file_payload, suffix):
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