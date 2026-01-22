import gspread
import os
import json
import base64
import requests
import unicodedata
import logging
import re
import time
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

class DataHandler:
    """
    Versión 5.2: Blindaje de tráfico pesado e informes de validación detallados.
    Implementa reintentos automáticos para manejar ráfagas de hasta 50 usuarios.
    """
    SCRIPT_URL = "https://script.google.com/macros/s/AKfycbxyr3lAA-Xykuy1S-mvGp3SdAb1ghDpdWsbHeURupBfJlO9D1xmGP12td1R7VZDAziV/exec"
    PARENT_FOLDER_ID = "1duPIhtA9Z6IObDxmANSLKA0Hw-R5Iidl"
    SHEET_URL = "https://docs.google.com/spreadsheets/d/1PGyE1TN5q1tEtoH5A-wxqS27DkONkNzp-hreL3OMJZw/edit#gid=0"

    def __init__(self):
        self.client = None
        self.workbook = None
        self.connect()

    def connect(self):
        """Conexión robusta con manejo de errores."""
        try:
            creds_b64 = os.getenv("GOOGLE_CREDS_BASE64")
            if creds_b64:
                missing_padding = len(creds_b64) % 4
                if missing_padding: creds_b64 += '=' * (4 - missing_padding)
                info = json.loads(base64.b64decode(creds_b64).decode('utf-8'))
                self.client = gspread.service_account_from_dict(info)
                self.workbook = self.client.open_by_url(self.SHEET_URL)
                logger.info("Conexión Sheets OK - Blindaje Activo v5.2")
        except Exception as e:
            logger.error(f"Error conexión Sheets: {e}")

    def _retry_operation(self, func, *args, **kwargs):
        """Blindaje: Reintenta operaciones de Sheets si la cuota está saturada."""
        max_retries = 5
        for i in range(max_retries):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                # Error 429 es cuota excedida en Google API
                if "429" in str(e) or "Quota exceeded" in str(e):
                    wait = (2 ** i) # Espera exponencial: 1, 2, 4, 8, 16 segundos
                    logger.warning(f"Saturación de tráfico detectada. Reintentando en {wait}s... (Intento {i+1})")
                    time.sleep(wait)
                else:
                    logger.error(f"Error no recuperable: {e}")
                    raise e
        return None

    # --- REPORTES Y ESTADÍSTICAS ---
    def get_agent_stats(self, agent_name):
        try:
            clients = self.get_clients_for_agent(agent_name)
            total = len(clients)
            ventas = len([c for c in clients if c.get('Estado Final') == 'Venta'])
            no_int = len([c for c in clients if c.get('Estado Final') == 'No interesado'])
            seguimiento = total - ventas - no_int
            return {
                "total": total,
                "ventas": ventas,
                "seguimiento": seguimiento,
                "conversion": round((ventas/total)*100, 2) if total > 0 else 0
            }
        except: return {"total": 0, "ventas": 0, "seguimiento": 0, "conversion": 0}

    # --- SEGURIDAD ---
    def set_agent_password(self, agent_name, new_password):
        try:
            sheet = self.workbook.worksheet("AsesorasActivas")
            cell = self._retry_operation(sheet.find, agent_name)
            if cell:
                self._retry_operation(sheet.update_cell, cell.row, 2, str(new_password))
                return True
            return False
        except Exception as e:
            logger.error(f"Error al registrar clave asesora: {e}")
            return False

    def set_auditor_password(self, auditor_name, new_password, permissions="Visualizador"):
        try:
            sheet = self.workbook.worksheet("Auditores")
            cell = self._retry_operation(sheet.find, auditor_name)
            if cell:
                self._retry_operation(sheet.update_cell, cell.row, 2, str(new_password))
                self._retry_operation(sheet.update_cell, cell.row, 3, permissions)
                return True
            return False
        except Exception as e:
            logger.error(f"Error al registrar clave auditor: {e}")
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
            cell = self._retry_operation(sheet.find, name)
            if not cell: return False
            
            row_idx = cell.row
            row_data = sheet.row_values(row_idx)
            
            # Buscar link de Drive para limpiar también la nube
            headers = sheet.row_values(1)
            norm_headers = [self._normalize(h) for h in headers]
            img_col_idx = next((i for i, h in enumerate(norm_headers) if "imagen" in h), None)
            
            if img_col_idx is not None and img_col_idx < len(row_data):
                url = row_data[img_col_idx]
                match = re.search(r'([a-zA-Z0-9\-_]{25,50})', url)
                if match:
                    id_drive = match.group(1)
                    try: requests.post(self.SCRIPT_URL, json={"action": "delete", "folderId": id_drive}, timeout=20)
                    except: pass

            self._retry_operation(sheet.delete_rows, row_idx)
            return True
        except Exception as e:
            logger.error(f"Error en borrado: {e}")
            return False

    # --- ASESORAS (ALTA Y ACTUALIZACIÓN) ---
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
        """Alta de cliente con validación detallada."""
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
            
            # Subida de evidencias a Drive
            if files_payload:
                for i, file in enumerate(files_payload):
                    res = self._send_to_script(data['Nombre'], file, f"Nuevo_{i}")
                    if res and res.get('status') == 'success' and not folder_url:
                        folder_url = res.get('folderUrl', '')
                if folder_url: row_map['Imagenes'] = folder_url

            new_row = [str(row_map.get(h, "")) for h in headers]
            # Usar retry en la inserción final
            self._retry_operation(sheet.append_row, new_row)
            
            return True, "Cliente registrado y escrito correctamente en base de datos."
        except Exception as e:
            return False, f"Fallo en la escritura: {str(e)}"

    def update_client_advanced(self, data):
        """Actualización avanzada con blindaje de colisiones y validación."""
        try:
            sheet = self.workbook.worksheet("Seguimientos")
            headers = sheet.row_values(1)
            name = data.get('nombre_original')
            
            # Localizar cliente con blindaje
            cell = self._retry_operation(sheet.find, name)
            if not cell: return False, "El cliente no fue localizado en la base de datos."
            
            row_idx = cell.row
            current_row_values = sheet.row_values(row_idx)
            while len(current_row_values) < len(headers): current_row_values.append("")
            
            row_data_actual = dict(zip(headers, current_row_values))
            status_actual = str(row_data_actual.get('Estado Final', '')).strip()
            
            updates = data.get('updates', {})
            files_payload = data.get('files_payload', [])
            
            # Asegurar que las columnas existan si se añadieron nuevos seguimientos
            headers = self._ensure_columns(sheet, headers, updates.keys())
            
            # Lógica de congelación de rendimiento
            if status_actual in ["Venta", "No interesado"] and 'Nivel de Interés' in updates:
                del updates['Nivel de Interés']
            
            tag = "Update"
            for k in updates.keys():
                if "Fecha Seguimiento" in k:
                    match = re.search(r'(\d+)', k)
                    if match: tag = f"S{match.group(1)}"; break

            if files_payload:
                folder_url = ""
                for i, file in enumerate(files_payload):
                    res = self._send_to_script(name, file, f"{tag}_{i}")
                    if res and res.get('status') == 'success' and not folder_url:
                        folder_url = res.get('folderUrl', '')
                if folder_url: updates['Imagenes'] = folder_url
            
            batch = []
            for k, v in updates.items():
                col_i = next((i+1 for i, h in enumerate(headers) if self._normalize(h) == self._normalize(k)), None)
                if col_i: batch.append({'range': gspread.utils.rowcol_to_a1(row_idx, col_i), 'values': [[str(v)]]})
            
            if batch:
                # Blindaje en la actualización por lotes
                self._retry_operation(sheet.batch_update, batch)
                return True, "Sincronización exitosa. Datos actualizados."
            
            return False, "No se detectaron cambios para actualizar."
        except Exception as e:
            logger.error(f"Error en update_client_advanced: {e}")
            return False, f"Error al sincronizar: {str(e)}"

    # --- UTILIDADES ---
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
                new_cols.append(col); updated_headers.append(col); norm_headers.append(self._normalize(col))
        if new_cols: 
            self._retry_operation(sheet.update, 'A1', [updated_headers])
            return updated_headers
        return headers

    def _send_to_script(self, client_name, file_payload, suffix):
        """Comunicación con Drive con timeout extendido para archivos pesados."""
        try:
            clean_name = client_name.strip().replace(" ", "_")
            filename = f"{clean_name}_{suffix}.png"
            payload = { 
                "parentFolderId": self.PARENT_FOLDER_ID, 
                "clientName": client_name, 
                "filename": filename, 
                "contentType": file_payload.get('contentType', 'image/png'), 
                "base64Data": file_payload.get('base64Data') 
            }
            return requests.post(self.SCRIPT_URL, json=payload, timeout=30).json()
        except Exception as e:
            logger.error(f"Error enviando a Drive: {e}")
            return None