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
            else:
                self.client = gspread.service_account(filename='Asesoras.json')
            self.workbook = self.client.open_by_url(self.SHEET_URL)
            logger.info("✅ Conexión establecida con Sheets.")
        except Exception as e:
            logger.error(f"❌ Error en conexión: {e}")

    def get_active_agents(self):
        """Retorna lista de dicts con nombre y contraseña de las asesoras"""
        try:
            sheet = self.workbook.worksheet("AsesorasActivas")
            data = sheet.get_all_values()
            # Asume Col 1: Nombre, Col 2: Password
            return [{"nombre": r[0].strip(), "password": r[1].strip() if len(r) > 1 else ""} 
                    for r in data[1:] if r and r[0].strip()]
        except Exception as e:
            logger.error(f"Error en get_active_agents: {e}")
            return []

    def set_agent_password(self, name, password):
        """Asigna una contraseña a una asesora en la hoja de cálculo"""
        try:
            sheet = self.workbook.worksheet("AsesorasActivas")
            data = sheet.get_all_values()
            for i, row in enumerate(data):
                if i > 0 and self._normalize(row[0]) == self._normalize(name):
                    # Actualiza la columna 2 (B) de la fila correspondiente
                    sheet.update_cell(i + 1, 2, str(password))
                    logger.info(f"✅ Contraseña asignada correctamente para: {name}")
                    return True
            return False
        except Exception as e:
            logger.error(f"Error asignando contraseña a {name}: {e}")
            return False

    def get_clients_for_agent(self, agent_name):
        try:
            sheet = self.workbook.worksheet("Seguimientos")
            data = sheet.get_all_records()
            target = self._normalize(agent_name)
            return [row for row in data if self._normalize(str(row.get('Asesora', ''))) == target]
        except Exception as e:
            logger.error(f"Error obteniendo clientes: {e}")
            return []

    def add_new_client(self, client_data, file_payload=None):
        try:
            sheet = self.workbook.worksheet("Seguimientos")
            headers = [h.strip() for h in sheet.row_values(1)]
            
            name = client_data.get("Nombre", "Sin_Nombre")
            final_data = {**client_data}

            if file_payload and file_payload.get('base64Data'):
                res = self._send_to_script(name, file_payload, "NUEVO")
                if res and res.get('status') == 'success':
                    final_data['Imagenes'] = res.get('folderUrl')

            row_to_append = []
            norm_payload = {self._normalize(k): v for k, v in final_data.items()}
            
            for h in headers:
                val = norm_payload.get(self._normalize(h), "")
                row_to_append.append(str(val))
            
            sheet.append_row(row_to_append)
            return True
        except Exception as e:
            logger.error(f"Error añadiendo cliente: {e}")
            return False

    def update_client_full(self, client_name, updates, file_payload=None):
        try:
            sheet = self.workbook.worksheet("Seguimientos")
            all_v = sheet.get_all_values()
            headers = [h.strip() for h in all_v[0]]
            
            row_idx = -1
            name_norm = self._normalize(client_name)
            for i, r in enumerate(all_v):
                if i > 0 and self._normalize(r[0]) == name_norm:
                    row_idx = i + 1
                    break
            
            if row_idx == -1: return False

            final_updates = {**updates}
            
            if file_payload and file_payload.get('base64Data'):
                res = self._send_to_script(client_name, file_payload, datetime.now().strftime("%H%M%S"))
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
            r = requests.post(self.SCRIPT_URL, json=payload, timeout=20)
            return r.json()
        except Exception as e:
            logger.error(f"Error en script de Google: {e}")
            return None

    def _normalize(self, text):
        if not text: return ""
        return "".join(c for c in unicodedata.normalize('NFD', str(text)) if unicodedata.category(c) != 'Mn').lower().strip()