import gspread
import os
import json
import base64
import requests
import unicodedata
import logging
import sqlite3
import time
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

class DataHandler:
    """
    Versión 6.0: Blindaje Total - SQLite Queue + Phone Validation + Single Thread Worker.
    """
    SCRIPT_URL = "https://script.google.com/macros/s/AKfycbxyr3lAA-Xykuy1S-mvGp3SdAb1ghDpdWsbHeURupBfJlO9D1xmGP12td1R7VZDAziV/exec"
    PARENT_FOLDER_ID = "1duPIhtA9Z6IObDxmANSLKA0Hw-R5Iidl"
    SHEET_URL = "https://docs.google.com/spreadsheets/d/1PGyE1TN5q1tEtoH5A-wxqS27DkONkNzp-hreL3OMJZw/edit#gid=0"

    def __init__(self):
        self.app_id = os.getenv("APP_ID", "crm-asesoras")
        self.db_path = f"/artifacts/{self.app_id}/prospectos.db" if os.path.exists(f"/artifacts/{self.app_id}") else "prospectos.db"
        self.journal_path = f"/artifacts/{self.app_id}/public/data/journal.jsonl" if os.path.exists(f"/artifacts/{self.app_id}") else "journal.jsonl"
        self._init_db()
        self.connect_sheets()

    def _init_db(self):
        """Inicializa la base de datos local para la cola y auditoría."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS sync_queue (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sync_id TEXT UNIQUE,
                type TEXT,
                phone TEXT,
                payload TEXT,
                status TEXT DEFAULT 'PENDING',
                drive_url TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                synced_at TIMESTAMP,
                error_message TEXT
            )
        ''')
        conn.commit()
        conn.close()

    def connect_sheets(self):
        try:
            creds_b64 = os.getenv("GOOGLE_CREDS_BASE64")
            if creds_b64:
                info = json.loads(base64.b64decode(creds_b64).decode('utf-8'))
                self.client = gspread.service_account_from_dict(info)
                self.workbook = self.client.open_by_url(self.SHEET_URL)
                logger.info("Conexión Sheets OK - Motor de Cola Activo")
        except Exception as e:
            logger.error(f"Error conexión Sheets: {e}")

    def get_db_path(self): return self.db_path
    def get_journal_path(self): return self.journal_path

    # --- LÓGICA DE COLA (SQLITE) ---

    def enqueue_client_data(self, action_type, data):
        """Guarda la petición en SQLite para que el worker la procese."""
        try:
            sync_id = data.get('sync_id', f"MANUAL_{int(time.time())}")
            phone = data.get('Canal', '0000000000')
            payload = json.dumps(data)
            
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO sync_queue (sync_id, type, phone, payload) 
                VALUES (?, ?, ?, ?)
            ''', (sync_id, action_type, phone, payload))
            conn.commit()
            conn.close()
            
            self._write_to_journal(action_type, data, "ENQUEUED")
            return True, "Encolado exitosamente"
        except sqlite3.IntegrityError:
            return True, "Ya estaba en cola"
        except Exception as e:
            logger.error(f"Error encolando: {e}")
            return False, str(e)

    def process_queue_step(self):
        """Toma el registro PENDING más antiguo y lo sube a Google."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT id, type, phone, payload, sync_id FROM sync_queue WHERE status = 'PENDING' ORDER BY id ASC LIMIT 1")
        row = cursor.fetchone()
        conn.close()

        if not row: return

        q_id, q_type, q_phone, q_payload, q_sync_id = row
        data = json.loads(q_payload)

        # 1. Validación por Teléfono (Si es un alta nueva)
        if q_type == "ADD" and self._check_phone_exists_local(q_phone):
            self._update_queue_status(q_id, "DUPLICATE", error="Teléfono ya registrado anteriormente.")
            return

        # 2. Subida a Google (Sheets + Drive)
        success, message, drive_url = self._upload_to_google(q_type, data)

        if success:
            # 3. Limpieza de Payload (Eliminar base64 para ahorrar espacio)
            if 'files_payload' in data:
                data['files_payload'] = "[CLEANED_AFTER_SUCCESS]"
            
            self._update_queue_status(q_id, "SUCCESS", drive_url=drive_url, clean_payload=json.dumps(data))
            self._write_to_journal(q_type, data, "SUCCESS")
        else:
            self._update_queue_status(q_id, "ERROR", error=message)
            self._write_to_journal(q_type, data, f"RETRY_ERROR: {message}")

    def _check_phone_exists_local(self, phone):
        """Chequeo rápido en base local."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM sync_queue WHERE phone = ? AND status = 'SUCCESS'", (phone,))
        exists = cursor.fetchone()
        conn.close()
        return exists is not None

    def _update_queue_status(self, q_id, status, drive_url=None, error=None, clean_payload=None):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        now = datetime.now().isoformat()
        if clean_payload:
            cursor.execute("UPDATE sync_queue SET status=?, drive_url=?, error_message=?, synced_at=?, payload=? WHERE id=?", 
                          (status, drive_url, error, now, clean_payload, q_id))
        else:
            cursor.execute("UPDATE sync_queue SET status=?, drive_url=?, error_message=?, synced_at=? WHERE id=?", 
                          (status, drive_url, error, now, q_id))
        conn.commit()
        conn.close()

    def _upload_to_google(self, q_type, data):
        """Lógica original de Google Sheets adaptada para el Worker."""
        try:
            sheet = self.workbook.worksheet("Seguimientos")
            folder_url = ""

            # Procesar Imágenes primero
            files = data.get('files_payload', [])
            if files:
                for i, file in enumerate(files):
                    res = self._send_to_script(data.get('Nombre') or data.get('nombre_original'), file, f"Sync_{i}")
                    if res and res.get('status') == 'success' and not folder_url:
                        folder_url = res.get('folderUrl', '')

            if q_type == "ADD":
                headers = sheet.row_values(1)
                row_map = {
                    'Nombre': data.get('Nombre'),
                    'Canal (Tel/WhatsApp)': data.get('Canal'),
                    'Fecha 1er Contacto': data.get('Fecha 1er Contacto'),
                    'Nivel de Interés': data.get('Nivel de Interés'),
                    'Resumen Conversación': data.get('Resumen Conversación'),
                    'Fecha Próx. Contacto': data.get('Fecha Próx. Contacto'),
                    'Asesora': data.get('Asesora'),
                    'Estado Final': data.get('Estado Final', 'Seguimiento'),
                    'Imagenes': folder_url,
                    'ID Sincronización': data.get('sync_id')
                }
                headers = self._ensure_columns(sheet, headers, row_map.keys())
                new_row = [str(row_map.get(h, "")) for h in headers]
                sheet.append_row(new_row)
                return True, "Ok", folder_url

            else: # UPDATE
                name = data.get('nombre_original')
                cell = sheet.find(name)
                if not cell: return False, "No encontrado", None
                
                updates = data.get('updates', {})
                if folder_url: updates['Imagenes'] = folder_url
                updates['ID Sincronización'] = data.get('sync_id')
                
                headers = sheet.row_values(1)
                headers = self._ensure_columns(sheet, headers, updates.keys())
                
                batch = []
                for k, v in updates.items():
                    col_i = next((i+1 for i, h in enumerate(headers) if self._normalize(h) == self._normalize(k)), None)
                    if col_i: batch.append({'range': gspread.utils.rowcol_to_a1(cell.row, col_i), 'values': [[str(v)]]})
                
                if batch: sheet.batch_update(batch)
                return True, "Ok", folder_url

        except Exception as e:
            return False, str(e), None

    # --- MÉTODOS DE APOYO (REUTILIZADOS) ---

    def _write_to_journal(self, action, data, status):
        try:
            os.makedirs(os.path.dirname(self.journal_path), exist_ok=True)
            entry = {
                "t": datetime.now().isoformat(),
                "act": action,
                "st": status,
                "sid": data.get('sync_id'),
                "name": data.get('Nombre') or data.get('nombre_original')
            }
            with open(self.journal_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(entry) + "\n")
        except: pass

    def _normalize(self, text):
        if not text: return ""
        text = str(text).lower().strip()
        text = "".join(c for c in unicodedata.normalize('NFD', text) if unicodedata.category(c) != 'Mn')
        return "".join(e for e in text if e.isalnum())

    def _ensure_columns(self, sheet, headers, required):
        updated = list(headers)
        norm_h = [self._normalize(h) for h in updated]
        added = False
        for col in required:
            if self._normalize(col) not in norm_h:
                updated.append(col)
                norm_h.append(self._normalize(col))
                added = True
        if added: sheet.update('A1', [updated])
        return updated

    def _send_to_script(self, client_name, file_payload, suffix):
        try:
            payload = { 
                "parentFolderId": self.PARENT_FOLDER_ID, 
                "clientName": client_name, 
                "filename": f"{suffix}.png", 
                "contentType": file_payload.get('contentType', 'image/png'), 
                "base64Data": file_payload.get('base64Data') 
            }
            return requests.post(self.SCRIPT_URL, json=payload, timeout=30).json()
        except: return None

    def get_active_agents(self):
        try: return self.workbook.worksheet("AsesorasActivas").get_all_records()
        except: return []

    def get_auditors(self):
        try: return self.workbook.worksheet("Auditores").get_all_records()
        except: return []

    def get_clients_for_agent(self, agent_name):
        try:
            data = self.workbook.worksheet("Seguimientos").get_all_records()
            return [row for row in data if str(row.get('Asesora', '')).lower() == agent_name.lower()]
        except: return []

    def get_all_clients(self):
        try: return self.workbook.worksheet("Seguimientos").get_all_records()
        except: return []