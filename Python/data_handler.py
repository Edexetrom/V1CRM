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
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("DataHandler")

class DataHandler:
    """
    Versión 10.11: Restauración de Prefijos de Imagen y Mapeo PascalCase para UI.
    - _upload_to_google: Recupera prefijos dinámicos (registro/seguimientoN) para Drive.
    - _map_db_to_ui: Asegura que 'asesora' se entregue como 'Asesora' para el CRM.
    """
    SCRIPT_URL = "https://script.google.com/macros/s/AKfycbxyr3lAA-Xykuy1S-mvGp3SdAb1ghDpdWsbHeURupBfJlO9D1xmGP12td1R7VZDAziV/exec"
    PARENT_FOLDER_ID = "1duPIhtA9Z6IObDxmANSLKA0Hw-R5Iidl"
    SHEET_URL = "https://docs.google.com/spreadsheets/d/1PGyE1TN5q1tEtoH5A-wxqS27DkONkNzp-hreL3OMJZw/edit#gid=0"

    FIELD_MAP = {
        'nombre': 'Nombre',
        'canal': 'Canal (Tel/WhatsApp)',
        'fecha_registro': 'Fecha 1er Contacto',
        'nivel_interes': 'Nivel de Interés',
        'resumen': 'Resumen Conversación',
        'rendimiento': 'Rendimiento',
        'fecha_proxima': 'Fecha Próx. Contacto',
        'estado_final': 'Estado Final',
        'comentarios': 'Comentarios',
        'asesora': 'Asesora',
        'imagenes_url': 'Imagenes',
        'id_unico': 'ID Sincronización'
    }

    def __init__(self):
        self.app_id = os.getenv("APP_ID", "crm-asesoras")
        self.db_path = f"/artifacts/{self.app_id}/prospectos.db" if os.path.exists(f"/artifacts/{self.app_id}") else "prospectos.db"
        self.journal_path = f"/artifacts/{self.app_id}/journal.jsonl" if os.path.exists(f"/artifacts/{self.app_id}") else "journal.jsonl"
        
        self._init_db()
        self.connect_sheets()
        
        if self._is_db_empty() and hasattr(self, 'workbook'):
            self.run_initial_migration()

    def _get_conn(self):
        return sqlite3.connect(self.db_path)

    def _init_db(self):
        conn = self._get_conn()
        cursor = conn.cursor()
        
        seguimientos_cols = ""
        for i in range(1, 31):
            seguimientos_cols += f"fecha_seguimiento_{i} TEXT, notas_seguimiento_{i} TEXT, "

        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS prospectos (
                id_unico TEXT PRIMARY KEY,
                validacion TEXT DEFAULT 'OK',
                nombre TEXT,
                canal TEXT,
                fecha_registro TEXT,
                nivel_interes TEXT,
                resumen TEXT,
                rendimiento TEXT,
                fecha_proxima TEXT,
                estado_final TEXT,
                comentarios TEXT,
                asesora TEXT,
                imagenes_url TEXT,
                {seguimientos_cols}
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS sync_queue (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sync_id TEXT UNIQUE,
                type TEXT,
                phone_id TEXT,
                payload TEXT,
                status TEXT DEFAULT 'PENDING',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        conn.commit()
        conn.close()

    def connect_sheets(self):
        try:
            creds_b64 = os.getenv("GOOGLE_CREDS_BASE64")
            if creds_b64:
                info = json.loads(base64.b64decode(creds_b64).decode('utf-8'))
                creds = Credentials.from_service_account_info(
                    info, scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
                )
                self.client = gspread.authorize(creds)
                self.workbook = self.client.open_by_url(self.SHEET_URL)
                logger.info("Conexión Sheets OK")
        except Exception as e:
            logger.error(f"Error conexión Sheets: {e}")

    def _is_db_empty(self):
        conn = self._get_conn()
        res = conn.execute("SELECT count(*) FROM prospectos").fetchone()[0]
        conn.close()
        return res == 0

    def run_initial_migration(self):
        logger.info("Migrando herencia desde Google Sheets a Master Local...")
        try:
            ws = self.workbook.worksheet("Seguimientos")
            records = ws.get_all_records()
            conn = self._get_conn()
            cursor = conn.cursor()

            for r in records:
                canal_raw = str(r.get(self.FIELD_MAP['canal']) or r.get('Canal') or '')
                canal = "".join(filter(str.isdigit, canal_raw))[-10:]
                if not canal: continue
                id_unico = f"ID_{canal}"
                
                base_data = [
                    id_unico, 'OK', r.get(self.FIELD_MAP['nombre']), canal, 
                    r.get(self.FIELD_MAP['fecha_registro']), r.get(self.FIELD_MAP['nivel_interes']), 
                    r.get(self.FIELD_MAP['resumen']), r.get(self.FIELD_MAP['rendimiento']),
                    r.get(self.FIELD_MAP['fecha_proxima']), r.get(self.FIELD_MAP['estado_final']),
                    r.get(self.FIELD_MAP['comentarios']), r.get(self.FIELD_MAP['asesora']), 
                    r.get(self.FIELD_MAP['imagenes_url'])
                ]
                
                seguimientos_data = []
                for i in range(1, 31):
                    seguimientos_data.append(r.get(f'Fecha Seguimiento {i}', ''))
                    seguimientos_data.append(r.get(f'Notas Seguimiento {i}', ''))

                total_params = base_data + seguimientos_data
                placeholders = ",".join(["?"] * len(total_params))
                cursor.execute(f"INSERT OR REPLACE INTO prospectos VALUES ({placeholders}, CURRENT_TIMESTAMP)", total_params)

            conn.commit()
            conn.close()
            logger.info("Migración exitosa.")
        except Exception as e:
            logger.error(f"Error migración: {e}")

    def set_agent_password(self, name, password):
        try:
            ws = self.workbook.worksheet("AsesorasActivas")
            cell = ws.find(name)
            if not cell: return False
            headers = ws.row_values(1)
            col_idx = next((i+1 for i, h in enumerate(headers) if self._normalize(h) in ['password', 'contrasena']), None)
            if col_idx:
                ws.update_cell(cell.row, col_idx, str(password))
                return True
            return False
        except: return False

    def set_auditor_password(self, name, password):
        try:
            ws = self.workbook.worksheet("Auditores")
            cell = ws.find(name)
            if not cell: return False
            headers = ws.row_values(1)
            col_idx = next((i+1 for i, h in enumerate(headers) if self._normalize(h) in ['password', 'contrasena']), None)
            if col_idx:
                ws.update_cell(cell.row, col_idx, str(password))
                return True
            return False
        except: return False

    def get_auditors_list(self):
        try:
            ws = self.workbook.worksheet("Auditores")
            return [v for v in ws.col_values(1) if v and v.lower() not in ['nombre', 'auditor', 'auditores']]
        except: return []

    def get_agents_list(self):
        try:
            ws = self.workbook.worksheet("AsesorasActivas")
            return [v for v in ws.col_values(1) if v and v.lower() not in ['nombre', 'asesora', 'asesoras']]
        except: return []

    def rename_client_db(self, old_name, new_name, canal):
        try:
            clean_canal = "".join(filter(str.isdigit, str(canal)))[-10:]
            id_unico = f"ID_{clean_canal}"
            sync_id = f"REN_{int(time.time())}_{clean_canal}"
            conn = self._get_conn()
            cursor = conn.cursor()
            cursor.execute("UPDATE prospectos SET nombre = ?, validacion = 'PENDIENTE', updated_at = CURRENT_TIMESTAMP WHERE id_unico = ?", (new_name, id_unico))
            payload = {"old_name": old_name, "new_name": new_name, "canal": canal, "sync_id": sync_id}
            cursor.execute('INSERT INTO sync_queue (sync_id, type, phone_id, payload) VALUES (?, ?, ?, ?)', 
                           (sync_id, "RENAME", id_unico, json.dumps(payload)))
            conn.commit(); conn.close()
            return True, "Ok"
        except: return False, "Error"

    def delete_client_db(self, name, canal, imagenes_url):
        try:
            clean_canal = "".join(filter(str.isdigit, str(canal)))[-10:]
            id_unico = f"ID_{clean_canal}"
            sync_id = f"DEL_{int(time.time())}_{clean_canal}"
            conn = self._get_conn()
            cursor = conn.cursor()
            cursor.execute("DELETE FROM prospectos WHERE id_unico = ?", (id_unico,))
            payload = {"nombre": name, "canal": canal, "imagenes_url": imagenes_url, "sync_id": sync_id}
            cursor.execute('INSERT INTO sync_queue (sync_id, type, phone_id, payload) VALUES (?, ?, ?, ?)', 
                           (sync_id, "DELETE", id_unico, json.dumps(payload)))
            conn.commit(); conn.close()
            return True, "Ok"
        except: return False, "Error"

    def enqueue_client_data(self, action_type, data):
        sync_id = data.get('sync_id') or f"AUTO_{int(time.time())}"
        data['sync_id'] = sync_id
        canal_raw = str(data.get('Canal') or data.get('canal_original') or '')
        canal = "".join(filter(str.isdigit, canal_raw))[-10:]
        id_unico = f"ID_{canal}" if canal else None
        
        # Lógica de Rendimiento
        rendimiento = "AL DIA"
        prox = data.get('Fecha Próx. Contacto')
        if prox and '/' in prox:
            try:
                d, m, y = map(int, prox.split('/'))
                if datetime(y, m, d).date() < datetime.now().date(): rendimiento = "VENCIDO"
            except: pass
        data['Rendimiento'] = rendimiento

        try:
            conn = self._get_conn()
            cursor = conn.cursor()
            if action_type == "ADD":
                cursor.execute('''
                    INSERT OR REPLACE INTO prospectos (id_unico, validacion, nombre, canal, fecha_registro, nivel_interes, resumen, rendimiento, fecha_proxima, estado_final, comentarios, asesora)
                    VALUES (?, 'PENDIENTE', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (id_unico, data.get('Nombre'), canal, data.get('Fecha 1er Contacto'), data.get('Nivel de Interés'), data.get('Resumen Conversación'), rendimiento, data.get('Fecha Próx. Contacto'), data.get('Estado Final', 'Seguimiento'), data.get('Comentarios', ''), data.get('Asesora')))
            
            elif action_type == "UPDATE":
                updates = data.get('updates', {})
                sql_parts = ["validacion = 'PENDIENTE'", "updated_at = CURRENT_TIMESTAMP", "rendimiento = ?"]
                params = [rendimiento]
                
                mapping_db = {'Estado Final': 'estado_final', 'Nivel de Interés': 'nivel_interes', 'Fecha Próx. Contacto': 'fecha_proxima', 'Comentarios': 'comentarios', 'nombre': 'nombre'}
                for k, v in mapping_db.items():
                    if k in updates:
                        sql_parts.append(f"{v} = ?")
                        params.append(updates[k])
                
                for i in range(1, 31):
                    if f'Fecha Seguimiento {i}' in updates:
                        sql_parts.append(f"fecha_seguimiento_{i} = ?")
                        params.append(updates[f'Fecha Seguimiento {i}'])
                    if f'Notas Seguimiento {i}' in updates:
                        sql_parts.append(f"notas_seguimiento_{i} = ?")
                        params.append(updates[f'Notas Seguimiento {i}'])

                ref = id_unico if id_unico else data.get('nombre_original')
                params.append(ref)
                cursor.execute(f"UPDATE prospectos SET {', '.join(sql_parts)} WHERE id_unico = ? OR nombre = ?", (params[-1], params[-1]) if not id_unico else (id_unico, data.get('nombre_original')))

            cursor.execute('INSERT INTO sync_queue (sync_id, type, phone_id, payload) VALUES (?, ?, ?, ?)', (sync_id, action_type, id_unico, json.dumps(data)))
            conn.commit(); conn.close()
            return True, "Ok"
        except Exception as e:
            logger.error(f"Error local: {e}")
            return False, str(e)

    def process_queue_step(self):
        conn = self._get_conn(); cursor = conn.cursor()
        cursor.execute("SELECT id, type, payload, sync_id, phone_id FROM sync_queue WHERE status = 'PENDING' ORDER BY id ASC LIMIT 1")
        row = cursor.fetchone()
        if not row: conn.close(); return
        
        q_id, q_type, q_payload, q_sync_id, q_phone_id = row
        cursor.execute("UPDATE sync_queue SET status = 'PROCESSING' WHERE id = ?", (q_id,))
        conn.commit()
        
        data = json.loads(q_payload)
        success, message, folder_url = self._upload_to_google(q_type, data)
        
        if success:
            cursor.execute("UPDATE sync_queue SET status = 'SUCCESS' WHERE id = ?", (q_id,))
            if q_type != "DELETE":
                f_url = folder_url or data.get('imagenes_url') or ""
                cursor.execute("UPDATE prospectos SET imagenes_url = ?, validacion = 'OK' WHERE id_unico = ?", (f_url, q_phone_id))
        else:
            cursor.execute("UPDATE sync_queue SET status = 'PENDING' WHERE id = ?", (q_id,))
        
        conn.commit(); conn.close()

    def _upload_to_google(self, q_type, data):
        try:
            ws = self.workbook.worksheet("Seguimientos")
            sync_id = data.get('sync_id')
            headers = ws.row_values(1)
            
            if q_type == "DELETE":
                canal_val = "".join(filter(str.isdigit, str(data.get('canal'))))[-10:]
                cell = ws.find(canal_val)
                if cell: ws.delete_rows(cell.row)
                return True, "Ok", None

            if q_type == "RENAME":
                cell = ws.find("".join(filter(str.isdigit, str(data.get('canal'))))[-10:])
                if cell:
                    col_n = next((i+1 for i, h in enumerate(headers) if self._normalize(h) == 'nombre'), 1)
                    ws.update_cell(cell.row, col_n, data.get('new_name'))
                return True, "Ok", None

            # DETERMINAR PREFIJO DINÁMICO DE IMAGEN
            prefix = "evidencia"
            if q_type == "ADD":
                prefix = "registro"
            elif q_type == "UPDATE":
                updates = data.get('updates', {})
                for i in range(1, 31):
                    if f'Fecha Seguimiento {i}' in updates or f'Notas Seguimiento {i}' in updates:
                        prefix = f"seguimiento{i}"
                        break

            folder_url = None
            if data.get('files_payload'):
                for idx, f in enumerate(data['files_payload']):
                    # Usar el prefijo dinámico para el nombre del archivo
                    res = self._send_to_script(data, f, f"{prefix}_{idx}.png")
                    if res and res.get('status') == 'success': folder_url = res.get('folderUrl')

            row_map = {
                self.FIELD_MAP['nombre']: data.get('Nombre'),
                self.FIELD_MAP['canal']: data.get('Canal'),
                self.FIELD_MAP['fecha_registro']: data.get('Fecha 1er Contacto'),
                self.FIELD_MAP['nivel_interes']: data.get('Nivel de Interés'),
                self.FIELD_MAP['resumen']: data.get('Resumen Conversación'),
                self.FIELD_MAP['rendimiento']: data.get('Rendimiento'),
                self.FIELD_MAP['fecha_proxima']: data.get('Fecha Próx. Contacto'),
                self.FIELD_MAP['estado_final']: data.get('Estado Final', 'Seguimiento'),
                self.FIELD_MAP['comentarios']: data.get('Comentarios', ''),
                self.FIELD_MAP['asesora']: data.get('Asesora'),
                self.FIELD_MAP['imagenes_url']: folder_url or data.get('imagenes_url') or "",
                self.FIELD_MAP['id_unico']: sync_id
            }

            if q_type == "ADD":
                headers = self._ensure_columns(ws, headers, row_map.keys())
                norm_map = {self._normalize(k): v for k, v in row_map.items()}
                row_vals = [str(norm_map.get(self._normalize(h), "")) for h in headers]
                ws.append_row(row_vals)
                return True, "Ok", folder_url
            else:
                cell = ws.find(data.get('nombre_original'))
                if not cell: return False, "No hallado", None
                updates = data.get('updates', {})
                sheet_updates = { self.FIELD_MAP.get(k, k): v for k, v in updates.items() }
                if 'Rendimiento' in data: sheet_updates[self.FIELD_MAP['rendimiento']] = data['Rendimiento']
                if folder_url: sheet_updates[self.FIELD_MAP['imagenes_url']] = folder_url
                sheet_updates[self.FIELD_MAP['id_unico']] = sync_id
                
                headers = self._ensure_columns(ws, headers, sheet_updates.keys())
                batch = []
                for k, v in sheet_updates.items():
                    col_i = next((i+1 for i, h in enumerate(headers) if self._normalize(h) == self._normalize(k)), None)
                    if col_i: batch.append({'range': gspread.utils.rowcol_to_a1(cell.row, col_i), 'values': [[str(v)]]})
                if batch: ws.batch_update(batch)
                return True, "Ok", folder_url
        except Exception as e:
            logger.error(f"Error Google: {e}")
            return False, str(e), None

    def _send_to_script(self, data, file_item, filename):
        try:
            payload = {"parentFolderId": self.PARENT_FOLDER_ID, "clientName": data.get('Nombre') or data.get('nombre_original'), "filename": filename, "base64Data": file_item['base64Data'], "contentType": file_item['contentType']}
            return requests.post(self.SCRIPT_URL, json=payload, timeout=30).json()
        except: return None

    def _map_db_to_ui(self, db_row):
        """Traduce quirúrgicamente las llaves de la DB (minúsculas) a llaves de UI (PascalCase)."""
        d = dict(db_row)
        ui_row = {}
        # Mapeamos campos base usando FIELD_MAP
        for db_key, ui_label in self.FIELD_MAP.items():
            if db_key in d:
                ui_row[ui_label] = d[db_key]
        
        # Mapeamos seguimientos dinámicos (1-30)
        for i in range(1, 31):
            f_db, n_db = f"fecha_seguimiento_{i}", f"notas_seguimiento_{i}"
            if f_db in d: ui_row[f"Fecha Seguimiento {i}"] = d[f_db]
            if n_db in d: ui_row[f"Notas Seguimiento {i}"] = d[n_db]
        
        # Aseguramos que el ID Sincronización (id_unico) esté presente si el UI lo requiere
        if 'id_unico' in d: ui_row['ID Sincronización'] = d['id_unico']
        
        return ui_row

    def get_clients_for_agent(self, agent_name):
        conn = self._get_conn(); conn.row_factory = sqlite3.Row; cursor = conn.cursor()
        cursor.execute("SELECT * FROM prospectos WHERE LOWER(asesora) = LOWER(?) ORDER BY updated_at DESC", (agent_name,))
        rows = [self._map_db_to_ui(r) for r in cursor.fetchall()]
        conn.close(); return rows

    def get_all_clients(self):
        try:
            conn = self._get_conn(); conn.row_factory = sqlite3.Row; cursor = conn.cursor()
            cursor.execute("SELECT * FROM prospectos ORDER BY updated_at DESC")
            rows = [self._map_db_to_ui(r) for r in cursor.fetchall()]
            conn.close(); return rows
        except: return []

    def _normalize(self, text):
        if not text: return ""
        text = str(text).lower().strip()
        text = "".join(c for c in unicodedata.normalize('NFD', text) if unicodedata.category(c) != 'Mn')
        return "".join(e for e in text if e.isalnum())

    def _ensure_columns(self, sheet, headers, required):
        updated = list(headers); norm_h = [self._normalize(h) for h in updated]; added = False
        for col in required:
            s_name = self.FIELD_MAP.get(col, col)
            if self._normalize(s_name) not in norm_h:
                updated.append(s_name); norm_h.append(self._normalize(s_name)); added = True
        if added: sheet.update('A1', [updated])
        return updated

    def _write_to_journal(self, action, data, status):
        try:
            os.makedirs(os.path.dirname(self.journal_path), exist_ok=True)
            entry = {"t": datetime.now().isoformat(), "act": action, "st": status, "sid": data.get('sync_id'), "name": data.get('Nombre') or data.get('nombre_original')}
            with open(self.journal_path, "a", encoding="utf-8") as f: f.write(json.dumps(entry) + "\n")
        except: pass

handler = DataHandler()