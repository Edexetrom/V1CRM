import gspread
import os
import json
import base64
import requests
import unicodedata
import logging
import re
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

class DataHandler:
    """
    Versión 4.8: Manejador centralizado con auto-registro de contraseñas.
    Soporta Panel de Asesoras (Alta/Update) y Panel de Auditoría (Filtros/Borrado).
    Actualizado con nomenclatura de imágenes dinámica (Nuevo/S1/S2...).
    """
    SCRIPT_URL = "https://script.google.com/macros/s/AKfycbxyr3lAA-Xykuy1S-mvGp3SdAb1ghDpdWsbHeURupBfJlO9D1xmGP12td1R7VZDAziV/exec"
    PARENT_FOLDER_ID = "1duPIhtA9Z6IObDxmANSLKA0Hw-R5Iidl"
    SHEET_URL = "https://docs.google.com/spreadsheets/d/1PGyE1TN5q1tEtoH5A-wxqS27DkONkNzp-hreL3OMJZw/edit#gid=0"

    def __init__(self):
        self.client = None
        self.workbook = None
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
                logger.info("Conexión Sheets OK - Versión 4.8")
        except Exception as e:
            logger.error(f"Error conexión: {e}")

    # --- GESTIÓN DE SEGURIDAD (AUTO-REGISTRO) ---
    def set_agent_password(self, agent_name, new_password):
        try:
            sheet = self.workbook.worksheet("AsesorasActivas")
            cell = sheet.find(agent_name)
            if cell:
                # Columna 2 es 'password' en AsesorasActivas
                sheet.update_cell(cell.row, 2, str(new_password))
                return True
            return False
        except Exception as e:
            logger.error(f"Error al registrar clave de asesora: {e}")
            return False

    def set_auditor_password(self, auditor_name, new_password, permissions="Visualizador"):
        try:
            sheet = self.workbook.worksheet("Auditores")
            cell = sheet.find(auditor_name)
            if cell:
                # Columna 2 es 'Contraseña', Columna 3 es 'Permisos'
                sheet.update_cell(cell.row, 2, str(new_password))
                sheet.update_cell(cell.row, 3, permissions)
                return True
            return False
        except Exception as e:
            logger.error(f"Error al registrar clave de auditor: {e}")
            return False

    # --- AUDITORÍA ---
    def get_auditors(self):
        try: return self.workbook.worksheet("Auditores").get_all_records()
        except: return []

    def get_all_clients(self):
        try: return self.workbook.worksheet("Seguimientos").get_all_records()
        except: return []

    def delete_client_and_folder(self, name):
        try:
            sheet = self.workbook.worksheet("Seguimientos")
            headers = sheet.row_values(1)
            norm_headers = [self._normalize(h) for h in headers]
            cell = sheet.find(name)
            if not cell: return False
            
            row_idx = cell.row
            row_data = sheet.row_values(row_idx)
            
            img_col_idx = next((i for i, h in enumerate(norm_headers) if "imagen" in h), None)
            if img_col_idx is not None and img_col_idx < len(row_data):
                url = row_data[img_col_idx]
                match = re.search(r'([a-zA-Z0-9\-_]{25,50})', url)
                if match:
                    id_drive = match.group(1)
                    try:
                        requests.post(self.SCRIPT_URL, json={"action": "delete", "folderId": id_drive}, timeout=20)
                    except Exception as err:
                        logger.error(f"Fallo contactando Script: {err}")

            sheet.delete_rows(row_idx)
            return True
        except Exception as e:
            logger.error(f"Error borrado: {e}")
            return False

    # --- ASESORAS ---
    def get_active_agents(self):
        try: return self.workbook.worksheet("AsesorasActivas").get_all_records()
        except: return []

    def get_clients_for_agent(self, agent_name):
        try:
            sheet = self.workbook.worksheet("Seguimientos")
            data = sheet.get_all_records()
            return [row for row in data if str(row.get('Asesora', '')).lower() == agent_name.lower()]
        except: return []

    def add_new_client(self, data, files_payload):
        try:
            sheet = self.workbook.worksheet("Seguimientos")
            headers = sheet.row_values(1)
            row_map = {
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
            headers = self._ensure_columns(sheet, headers, row_map.keys())
            folder_url = ""
            if files_payload:
                for i, file in enumerate(files_payload):
                    # Nomenclatura para Registro Nuevo
                    res = self._send_to_script(data['Nombre'], file, f"Nuevo_{i}")
                    if res and res.get('status') == 'success' and not folder_url:
                        folder_url = res.get('folderUrl', '')
                if folder_url: row_map['Imagenes'] = folder_url
            new_row = [str(row_map.get(h, "")) for h in headers]
            sheet.append_row(new_row)
            return True
        except: return False

    def update_client_advanced(self, data):
        try:
            sheet = self.workbook.worksheet("Seguimientos")
            headers = sheet.row_values(1)
            name = data.get('nombre_original')
            cell = sheet.find(name)
            if not cell: return False
            row_idx = cell.row
            updates = data.get('updates', {})
            files_payload = data.get('files_payload', [])
            headers = self._ensure_columns(sheet, headers, updates.keys())
            
            # Detectar el tag de seguimiento (S1, S2, etc.) basándose en las columnas actualizadas
            tag = "Update"
            for k in updates.keys():
                if "Fecha Seguimiento" in k:
                    match = re.search(r'(\d+)', k)
                    if match:
                        tag = f"S{match.group(1)}"
                        break

            if files_payload:
                folder_url = ""
                for i, file in enumerate(files_payload):
                    # Nomenclatura para Seguimientos (S1, S2...)
                    res = self._send_to_script(name, file, f"{tag}_{i}")
                    if res and res.get('status') == 'success' and not folder_url:
                        folder_url = res.get('folderUrl', '')
                if folder_url: updates['Imagenes'] = folder_url
            
            batch = []
            for k, v in updates.items():
                col_i = next((i+1 for i, h in enumerate(headers) if self._normalize(h) == self._normalize(k)), None)
                if col_i: batch.append({'range': gspread.utils.rowcol_to_a1(row_idx, col_i), 'values': [[str(v)]]})
            if batch: sheet.batch_update(batch)
            return True
        except: return False

    # --- UTILS ---
    def _normalize(self, text):
        if not text: return ""
        text = str(text).lower().strip()
        text = "".join(c for c in unicodedata.normalize('NFD', text) if unicodedata.category(c) != 'Mn')
        return "".join(e for e in text if e.isalnum())

    def _ensure_columns(self, sheet, headers, required_columns):
        updated_headers = list(headers)
        new_cols = []
        norm_headers = [self._normalize(h) for h in updated_headers]
        for col in required_columns:
            if self._normalize(col) not in norm_headers:
                new_cols.append(col)
                updated_headers.append(col)
                norm_headers.append(self._normalize(col))
        if new_cols:
            sheet.update('A1', [updated_headers])
            return updated_headers
        return headers

    def _send_to_script(self, client_name, file_payload, suffix):
        """Construye el nombre exacto: Nombre_Apellido_Tipo_Indice.png"""
        try:
            # Reemplazar espacios por guiones bajos para el nombre del archivo
            clean_name = client_name.strip().replace(" ", "_")
            filename = f"{clean_name}_{suffix}.png"
            
            payload = {
                "parentFolderId": self.PARENT_FOLDER_ID,
                "clientName": client_name,
                "filename": filename,
                "contentType": file_payload.get('contentType', 'image/png'),
                "base64Data": file_payload.get('base64Data')
            }
            return requests.post(self.SCRIPT_URL, json=payload, timeout=25).json()
        except: return None