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
    Versión 5.4: Blindaje Nivel 3 - Escudo Anti-Duplicados (Idempotencia).
    Utiliza el sync_id para prevenir registros dobles en caso de reintentos de red.
    """
    SCRIPT_URL = "https://script.google.com/macros/s/AKfycbxyr3lAA-Xykuy1S-mvGp3SdAb1ghDpdWsbHeURupBfJlO9D1xmGP12td1R7VZDAziV/exec"
    PARENT_FOLDER_ID = "1duPIhtA9Z6IObDxmANSLKA0Hw-R5Iidl"
    SHEET_URL = "https://docs.google.com/spreadsheets/d/1PGyE1TN5q1tEtoH5A-wxqS27DkONkNzp-hreL3OMJZw/edit#gid=0"

    def __init__(self):
        self.client = None
        self.workbook = None
        self.app_id = os.getenv("APP_ID", "crm-asesoras")
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
                logger.info("Conexión Sheets OK - Blindaje Activo v5.4")
        except Exception as e:
            logger.error(f"Error conexión Sheets: {e}")

    def _write_to_journal(self, action, data, status="PENDING"):
        """
        PROPUESTA 1: Caja Negra.
        Escribe la transacción en el almacenamiento local con estado de proceso.
        """
        try:
            if os.path.exists(f"/artifacts/{self.app_id}"):
                journal_dir = f"/artifacts/{self.app_id}/public/data"
            else:
                journal_dir = os.path.join(os.getcwd(), "data")
            
            if not os.path.exists(journal_dir):
                os.makedirs(journal_dir, exist_ok=True)
            
            journal_path = os.path.join(journal_dir, "journal.jsonl")
            
            entry = {
                "timestamp": datetime.now().isoformat(),
                "action": action,
                "status": status,
                "sync_id": data.get('sync_id', 'anonymous'),
                "asesora": data.get('Asesora') or data.get('nombre_original', 'unknown'),
                "payload_summary": {k: v for k, v in data.items() if k != 'files_payload'} 
            }
            
            with open(journal_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(entry, ensure_ascii=False) + "\n")
            
            logger.info(f"[CAJA NEGRA] Transacción registrada [{status}]: {entry['sync_id']}")
        except Exception as e:
            logger.error(f"Fallo crítico al escribir en el diario: {e}")

    def _is_already_processed(self, sheet, sync_id):
        """
        FASE 3: Escudo Anti-Duplicados.
        Verifica si el ID de sincronización ya existe en la base de datos.
        """
        if not sync_id or sync_id == 'anonymous':
            return False
            
        try:
            headers = sheet.row_values(1)
            # Normalizamos para encontrar la columna de ID
            id_col_index = next((i + 1 for i, h in enumerate(headers) if self._normalize(h) == self._normalize("ID Sincronización")), None)
            
            if not id_col_index:
                return False # Si la columna no existe, no puede estar duplicado aún
                
            # Obtenemos todos los valores de esa columna
            existing_ids = sheet.col_values(id_col_index)
            if sync_id in existing_ids:
                logger.warning(f"[ESCUDO] Intento de duplicado bloqueado: {sync_id}")
                return True
        except Exception as e:
            logger.error(f"Error verificando duplicados: {e}")
            
        return False

    def _retry_operation(self, func, *args, **kwargs):
        """Blindaje: Reintenta operaciones de Sheets con backoff exponencial."""
        max_retries = 5
        for i in range(max_retries):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if "429" in str(e) or "Quota exceeded" in str(e):
                    wait = (2 ** i)
                    logger.warning(f"Saturación de tráfico detectada. Reintentando en {wait}s... (Intento {i+1})")
                    time.sleep(wait)
                else:
                    logger.error(f"Error no recuperable: {e}")
                    raise e
        return None

    def add_new_client(self, data, files_payload):
        """Alta de cliente con blindaje Nivel 3."""
        try:
            self._write_to_journal("add_client", data, "START")

            sheet = self.workbook.worksheet("Seguimientos")
            
            # PASO DE SEGURIDAD FASE 3: Verificar duplicado
            if self._is_already_processed(sheet, data.get('sync_id')):
                self._write_to_journal("add_client", data, "BLOCKED_DUPLICATE")
                return True, "Esta gestión ya fue procesada anteriormente."

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
                'Comentarios': data.get('Comentarios', ''),
                'ID Sincronización': data.get('sync_id') # Guardamos el ID para futuras validaciones
            }
            
            # Aseguramos que existan todas las columnas, incluyendo la nueva de ID
            headers = self._ensure_columns(sheet, headers, row_map.keys())
            folder_url = ""
            
            if files_payload:
                for i, file in enumerate(files_payload):
                    res = self._send_to_script(data['Nombre'], file, f"Nuevo_{i}")
                    if res and res.get('status') == 'success' and not folder_url:
                        folder_url = res.get('folderUrl', '')
                if folder_url: row_map['Imagenes'] = folder_url

            new_row = [str(row_map.get(h, "")) for h in headers]
            self._retry_operation(sheet.append_row, new_row)
            
            self._write_to_journal("add_client", data, "SUCCESS")
            return True, "Cliente registrado correctamente."
        except Exception as e:
            logger.error(f"Error en add_new_client: {e}")
            self._write_to_journal("add_client", data, f"ERROR: {str(e)}")
            return False, f"Fallo en la escritura: {str(e)}"

    def update_client_advanced(self, data):
        """Actualización avanzada con blindaje Nivel 3."""
        try:
            self._write_to_journal("update_client", data, "START")

            sheet = self.workbook.worksheet("Seguimientos")
            
            # PASO DE SEGURIDAD FASE 3: Verificar duplicado
            if self._is_already_processed(sheet, data.get('sync_id')):
                self._write_to_journal("update_client", data, "BLOCKED_DUPLICATE")
                return True, "Actualización ya aplicada previamente."

            headers = sheet.row_values(1)
            name = data.get('nombre_original')
            
            cell = self._retry_operation(sheet.find, name)
            if not cell: return False, "El cliente no fue localizado en la base de datos."
            
            row_idx = cell.row
            updates = data.get('updates', {})
            updates['ID Sincronización'] = data.get('sync_id') # Registramos el ID de la transacción
            
            files_payload = data.get('files_payload', [])
            headers = self._ensure_columns(sheet, headers, updates.keys())

            if files_payload:
                folder_url = ""
                for i, file in enumerate(files_payload):
                    res = self._send_to_script(name, file, f"Update_{i}")
                    if res and res.get('status') == 'success' and not folder_url:
                        folder_url = res.get('folderUrl', '')
                if folder_url: updates['Imagenes'] = folder_url
            
            batch = []
            for k, v in updates.items():
                col_i = next((i+1 for i, h in enumerate(headers) if self._normalize(h) == self._normalize(k)), None)
                if col_i: batch.append({'range': gspread.utils.rowcol_to_a1(row_idx, col_i), 'values': [[str(v)]]})
            
            if batch:
                self._retry_operation(sheet.batch_update, batch)
                self._write_to_journal("update_client", data, "SUCCESS")
                return True, "Sincronización exitosa."
            
            return False, "No se detectaron cambios."
        except Exception as e:
            logger.error(f"Error en update_client_advanced: {e}")
            self._write_to_journal("update_client", data, f"ERROR: {str(e)}")
            return False, f"Error al sincronizar: {str(e)}"

    def _normalize(self, text):
        if not text: return ""
        text = str(text).lower().strip()
        text = "".join(c for c in unicodedata.normalize('NFD', text) if unicodedata.category(c) != 'Mn')
        return "".join(e for e in text if e.isalnum())

    def _ensure_columns(self, sheet, headers, required_columns):
        """Garantiza la existencia de columnas, incluyendo la de ID Sincronización."""
        updated_headers = list(headers)
        new_cols = []
        norm_headers = [self._normalize(h) for h in updated_headers]
        for col in required_columns:
            if self._normalize(col) not in norm_headers:
                new_cols.append(col)
                updated_headers.append(col)
                norm_headers.append(self._normalize(col))
        if new_cols: 
            self._retry_operation(sheet.update, 'A1', [updated_headers])
            logger.info(f"[SISTEMA] Nuevas columnas creadas: {new_cols}")
            return updated_headers
        return headers

    # Resto de métodos de apoyo (get_agent_stats, get_active_agents, etc.) se mantienen igual
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

    def _send_to_script(self, client_name, file_payload, suffix):
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

    def get_active_agents(self):
        try: return self.workbook.worksheet("AsesorasActivas").get_all_records()
        except: return []

    def get_clients_for_agent(self, agent_name):
        try:
            sheet = self.workbook.worksheet("Seguimientos")
            data = sheet.get_all_records()
            return [row for row in data if str(row.get('Asesora', '')).lower() == agent_name.lower()]
        except: return []

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
            self._retry_operation(sheet.delete_rows, row_idx)
            return True
        except Exception as e:
            logger.error(f"Error en borrado: {e}")
            return False

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