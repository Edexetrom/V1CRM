import gspread
import os
import json
import base64
import requests
import unicodedata
import logging
import sqlite3
import time
import random
import string
import re
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

class DataHandler:
    """
    Versión 8.2: Eliminación de Duplicados (Locking) + Borrado Espejo en SQLite.
    Garantiza que no se suban registros duplicados y limpia la base de datos local al borrar.
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

    def _get_conn(self):
        return sqlite3.connect(self.db_path)

    def _init_db(self):
        conn = self._get_conn()
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
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS system_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sync_id TEXT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                level TEXT,
                message TEXT,
                agent TEXT
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
                logger.info("Conexión Sheets OK - Modo Localhost")
        except Exception as e:
            logger.error(f"Error conexión Sheets: {e}")

    def set_agent_password(self, agent_name, new_password):
        try:
            ws = self.workbook.worksheet("AsesorasActivas")
            cell = ws.find(agent_name)
            if not cell: return False
            headers = ws.row_values(1)
            try:
                pass_col_idx = next(i+1 for i, h in enumerate(headers) if self._normalize(h) == 'password')
            except StopIteration:
                pass_col_idx = len(headers) + 1
                ws.update_cell(1, pass_col_idx, "password")
            ws.update_cell(cell.row, pass_col_idx, str(new_password).strip())
            return True
        except Exception as e:
            logger.error(f"Error registrando clave inicial: {e}")
            return False

    def generate_missing_passwords(self):
        try:
            ws = self.workbook.worksheet("AsesorasActivas")
            records = ws.get_all_records()
            headers = ws.row_values(1)
            try:
                pass_col_idx = next(i+1 for i, h in enumerate(headers) if self._normalize(h) == 'password')
            except StopIteration:
                ws.update_cell(1, len(headers) + 1, "password")
                pass_col_idx = len(headers) + 1
            updated_count = 0
            for i, row in enumerate(records):
                current_pass = str(row.get('password', '')).strip()
                if not current_pass or current_pass == "" or current_pass == "None":
                    new_pass = ''.join(random.choices(string.digits, k=4))
                    ws.update_cell(i + 2, pass_col_idx, new_pass)
                    updated_count += 1
            return True, updated_count
        except Exception as e:
            logger.error(f"Error generando contraseñas: {e}")
            return False, 0

    def _log_event(self, sync_id, level, message, agent="SYSTEM"):
        try:
            conn = self._get_conn()
            cursor = conn.cursor()
            now = datetime.now().isoformat()
            cursor.execute('''INSERT INTO system_logs (sync_id, timestamp, level, message, agent) VALUES (?, ?, ?, ?, ?)''', (sync_id, now, level, message, agent))
            conn.commit()
            conn.close()
        except: pass

    def get_latest_system_logs(self, limit=50):
        try:
            conn = self._get_conn()
            cursor = conn.cursor()
            cursor.execute("SELECT timestamp, level, message, sync_id, agent FROM system_logs ORDER BY id DESC LIMIT ?", (limit,))
            logs = [{"t": r[0], "lv": r[1], "msg": r[2], "sid": r[3], "ag": r[4]} for r in cursor.fetchall()]
            conn.close()
            return logs
        except: return []

    def enqueue_client_data(self, action_type, data):
        try:
            sync_id = data.get('sync_id') or f"AUTO_{int(time.time())}"
            data['sync_id'] = sync_id
            agent = data.get('Asesora', 'SYSTEM')
            self._log_event(sync_id, "INFO", f"Recibiendo {action_type}", agent)
            if self._is_already_success(sync_id): return True, "Ya procesado anteriormente (ID duplicado)"
            self._write_to_journal(action_type, data, "RECEIVED")
            conn = self._get_conn()
            cursor = conn.cursor()
            try:
                cursor.execute('INSERT INTO sync_queue (sync_id, type, phone, payload) VALUES (?, ?, ?, ?)', (sync_id, action_type, data.get('Canal', '000'), json.dumps(data)))
                conn.commit()
                self._log_event(sync_id, "INFO", "Encolado en SQLite", agent)
            except sqlite3.IntegrityError:
                conn.close()
                return True, "Ya se encuentra en cola de espera"
            conn.close()
            return True, "Registrado correctamente"
        except Exception as e: return False, str(e)

    def process_queue_step(self):
        """
        Versión Corregida (8.2): Implementa Locking para evitar duplicados.
        """
        conn = self._get_conn()
        cursor = conn.cursor()
        # 1. Seleccionar el siguiente pendiente
        cursor.execute("SELECT id, type, payload, sync_id FROM sync_queue WHERE status = 'PENDING' ORDER BY id ASC LIMIT 1")
        row = cursor.fetchone()
        
        if not row:
            conn.close()
            return
            
        q_id, q_type, q_payload, q_sync_id = row
        
        # 2. BLOQUEO: Marcar como SYNCING inmediatamente para que otro ciclo no lo tome
        cursor.execute("UPDATE sync_queue SET status = 'SYNCING' WHERE id = ?", (q_id,))
        conn.commit()
        conn.close() # Liberamos conexión para el proceso largo de subida

        data = json.loads(q_payload)
        success, message, drive_url = self._upload_to_google(q_type, data)
        
        if success:
            if 'files_payload' in data: data['files_payload'] = "[CLEANED]"
            self._update_queue_status(q_id, "SUCCESS", drive_url=drive_url, clean_payload=json.dumps(data))
            self._write_to_journal(q_type, data, "SUCCESS")
        else:
            # Si falla, se devuelve a PENDING para reintento en el siguiente ciclo
            self._update_queue_status(q_id, "PENDING", error=message)

    def _upload_to_google(self, q_type, data):
        try:
            sheet = self.workbook.worksheet("Seguimientos")
            sync_id = data.get('sync_id')
            try:
                if sheet.find(sync_id): return True, "Ya sincronizado", ""
            except: pass

            prefix = "Registro_Inicial"
            if q_type == "UPDATE":
                updates = data.get('updates', {})
                nums = [int(k.split()[-1]) for k in updates.keys() if "Seguimiento" in k and k.split()[-1].isdigit()]
                n_seguimiento = max(nums) if nums else "X"
                prefix = f"Seguimiento_{n_seguimiento}"

            folder_url = ""
            files = data.get('files_payload', [])
            if files and isinstance(files, list):
                for i, f in enumerate(files):
                    custom_name = f"{prefix}_{sync_id}_{i}"
                    res = self._send_to_script(data.get('Nombre') or data.get('nombre_original'), f, custom_name)
                    if res and res.get('status') == 'success':
                        folder_url = res.get('folderUrl', folder_url)

            if q_type == "ADD":
                headers = sheet.row_values(1)
                row_map = {
                    'Nombre': data.get('Nombre'), 'Canal (Tel/WhatsApp)': data.get('Canal'),
                    'Fecha 1er Contacto': data.get('Fecha 1er Contacto'), 'Nivel de Interés': data.get('Nivel de Interés'),
                    'Resumen Conversación': data.get('Resumen Conversación'), 'Fecha Próx. Contacto': data.get('Fecha Próx. Contacto'),
                    'Asesora': data.get('Asesora'), 'Estado Final': data.get('Estado Final', 'Seguimiento'),
                    'Imagenes': folder_url, 'ID Sincronización': sync_id
                }
                headers = self._ensure_columns(sheet, headers, row_map.keys())
                sheet.append_row([str(row_map.get(h, "")) for h in headers])
                return True, "Ok", folder_url
            else:
                name = data.get('nombre_original')
                cell = sheet.find(name)
                if not cell: return False, "No encontrado en Sheets", None
                updates = data.get('updates', {})
                if folder_url: updates['Imagenes'] = folder_url
                updates['ID Sincronización'] = sync_id
                headers = sheet.row_values(1)
                headers = self._ensure_columns(sheet, headers, updates.keys())
                batch = []
                for k, v in updates.items():
                    col_i = next((i+1 for i, h in enumerate(headers) if self._normalize(h) == self._normalize(k)), None)
                    if col_i: batch.append({'range': gspread.utils.rowcol_to_a1(cell.row, col_i), 'values': [[str(v)]]})
                if batch: sheet.batch_update(batch)
                return True, "Ok", folder_url
        except Exception as e: return False, str(e), None

    def delete_client(self, nombre):
        """
        Recuperación Crítica (8.2): Elimina en Sheets, Drive y Borrado Espejo en SQLite.
        """
        try:
            sheet = self.workbook.worksheet("Seguimientos")
            cell = sheet.find(nombre)
            if cell:
                # 1. Intentar eliminar la carpeta de Drive
                try:
                    headers = sheet.row_values(1)
                    img_col_idx = next(i+1 for i, h in enumerate(headers) if self._normalize(h) == 'imagenes')
                    folder_url = sheet.cell(cell.row, img_col_idx).value
                    if folder_url:
                        match = re.search(r'([a-zA-Z0-9\-_]{25,50})', folder_url)
                        if match:
                            folder_id = match.group(1)
                            requests.post(self.SCRIPT_URL, json={"action": "delete", "folderId": folder_id}, timeout=30)
                except Exception as e_drive:
                    logger.warning(f"No se pudo eliminar la carpeta de Drive para {nombre}: {e_drive}")

                # 2. Borrar la fila en Sheets
                sheet.delete_rows(cell.row)

                # 3. BORRADO ESPEJO: Limpiar la base de datos local de cualquier rastro del cliente
                try:
                    conn = self._get_conn()
                    cursor = conn.cursor()
                    # Borramos registros cuyo payload contenga el nombre del cliente
                    cursor.execute("DELETE FROM sync_queue WHERE payload LIKE ?", (f'%"{nombre}"%',))
                    conn.commit()
                    conn.close()
                    logger.info(f"Limpieza SQLite exitosa para: {nombre}")
                except Exception as e_db:
                    logger.error(f"Error en limpieza SQLite: {e_db}")

                self._write_to_journal("DELETE", {"Nombre": nombre}, "SUCCESS")
                return True, "Eliminado y Limpiado"
            return False, "No hallado"
        except Exception as e: return False, str(e)

    def _is_already_success(self, sync_id):
        conn = self._get_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM sync_queue WHERE sync_id = ? AND status = 'SUCCESS'", (sync_id,))
        res = cursor.fetchone()
        conn.close()
        return res is not None

    def _update_queue_status(self, q_id, status, drive_url=None, error=None, clean_payload=None):
        conn = self._get_conn()
        cursor = conn.cursor()
        now = datetime.now().isoformat()
        if clean_payload:
            cursor.execute("UPDATE sync_queue SET status=?, drive_url=?, error_message=?, synced_at=?, payload=? WHERE id=?", (status, drive_url, error, now, clean_payload, q_id))
        else:
            cursor.execute("UPDATE sync_queue SET status=?, drive_url=?, error_message=?, synced_at=? WHERE id=?", (status, drive_url, error, now, q_id))
        conn.commit()
        conn.close()

    def _write_to_journal(self, action, data, status):
        try:
            os.makedirs(os.path.dirname(self.journal_path), exist_ok=True)
            entry = {"t": datetime.now().isoformat(), "act": action, "st": status, "sid": data.get('sync_id'), "name": data.get('Nombre') or data.get('nombre_original')}
            with open(self.journal_path, "a", encoding="utf-8") as f: f.write(json.dumps(entry) + "\n")
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

    def _send_to_script(self, client_name, file_payload, custom_filename):
        try:
            payload = {"parentFolderId": self.PARENT_FOLDER_ID, "clientName": client_name, "filename": f"{custom_filename}.png", "contentType": file_payload.get('contentType', 'image/png'), "base64Data": file_payload.get('base64Data')}
            return requests.post(self.SCRIPT_URL, json=payload, timeout=60).json()
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