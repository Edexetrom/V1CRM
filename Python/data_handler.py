import os
import logging
import requests
import json
import base64
import gspread
import unicodedata
from datetime import datetime, timedelta
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv, find_dotenv
from supabase import create_client, Client
import re
import sqlite3
import time
import pytz
# Carga de variables de entorno desde el directorio del proyecto o padres
dotenv_path = find_dotenv()
if dotenv_path:
    load_dotenv(dotenv_path)
else:
    load_dotenv()

# Configuración de logs modular
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("DataHandler")

# --- CONFIGURACIÓN DE CONEXIÓN ---
SUPABASE_URL = "https://qldrdljyuqlyqwoauwyd.supabase.co"
SUPABASE_KEY = os.getenv("SUPABASE_KEY") or ""

class GoogleSheetsSync:
    """
    Módulo especializado en la comunicación con Google Sheets y Drive.
    """
    SCRIPT_URL = "https://script.google.com/macros/s/AKfycbxyr3lAA-Xykuy1S-mvGp3SdAb1ghDpdWsbHeURupBfJlO9D1xmGP12td1R7VZDAziV/exec"
    SHEET_URL = "https://docs.google.com/spreadsheets/d/1PGyE1TN5q1tEtoH5A-wxqS27DkONkNzp-hreL3OMJZw/edit#gid=0"
    PARENT_FOLDER_ID = "1duPIhtA9Z6IObDxmANSLKA0Hw-R5Iidl"

    def __init__(self):
        self.client = None
        self.workbook = None
        self.creds = None
        self._sheet_cache = {}
        self._cache_ttl = 60
        self._authenticate()

    def _authenticate(self):
        try:
            creds_b64 = os.getenv("GOOGLE_CREDS_BASE64")
            if creds_b64:
                info = json.loads(base64.b64decode(creds_b64).decode('utf-8'))
                self.creds = Credentials.from_service_account_info(
                    info, scopes=[
                        "https://www.googleapis.com/auth/spreadsheets",
                        "https://www.googleapis.com/auth/drive",
                        "https://www.googleapis.com/auth/calendar.readonly"
                    ]
                )
                self.client = gspread.authorize(self.creds)
                self.workbook = self.client.open_by_url(self.SHEET_URL)
                logger.info("GOOGLE CLOUD: Autenticación exitosa.")
        except Exception as e:
            logger.error(f"GOOGLE AUTH ERROR: {e}")

    def subir_evidencia_drive(self, nombre_cliente, base64_data, filename):
        try:
            payload = {
                "action": "upload",
                "parentFolderId": self.PARENT_FOLDER_ID,
                "clientName": nombre_cliente,
                "filename": filename,
                "base64Data": base64_data,
                "contentType": "image/png"
            }
            response = requests.post(self.SCRIPT_URL, json=payload, timeout=30)
            return response.json()
        except Exception as e:
            logger.error(f"DRIVE UPLOAD ERROR: {e}")
            return {"status": "error", "message": str(e)}

    def borrar_carpeta_drive(self, folder_url):
        if not folder_url or not isinstance(folder_url, str): return {"status": "skipped"}
        try:
            match = re.search(r'([a-zA-Z0-9-_]{25,})', folder_url)
            if not match: return {"status": "error"}
            folder_id = match.group(1)
            payload = {"action": "delete", "folderId": folder_id}
            res = requests.post(self.SCRIPT_URL, json=payload, timeout=20)
            return res.json()
        except: return {"status": "error"}

    def obtener_datos_hoja(self, nombre_hoja, force_refresh=False):
        if not self.workbook: return []
        now = time.time()
        cached = self._sheet_cache.get(nombre_hoja)
        if not force_refresh and cached and (now - cached[0]) < self._cache_ttl:
            return cached[1]
        try:
            ws = self.workbook.worksheet(nombre_hoja)
            records = ws.get_all_records()
            self._sheet_cache[nombre_hoja] = (now, records)
            return records
        except:
            return cached[1] if cached else []

    def verificar_o_guardar_password(self, nombre_norm, display_name, password, normalize_fn):
        """
        Busca la contraseña de la asesora en la pestaña 'Contraseñas' (o 'AsesorasActivas' si no existe).
        Si el valor de contraseña está en blanco, guarda la primera contraseña que ingrese.
        """
        if not self.workbook:
            return {"status": "error", "message": "Google Sheets no autenticado."}

        ws = None
        for sheet_name in ("Contraseñas", "AsesorasActivas"):
            try:
                candidate = self.workbook.worksheet(sheet_name)
                rows = candidate.get_all_values()
                if not rows:
                    continue
                headers_norm = [normalize_fn(h) for h in rows[0]]
                pwd_col = next((i for i, h in enumerate(headers_norm) if h in ("contrasena", "password", "clave")), None)
                if pwd_col is not None:
                    ws = candidate
                    break
            except Exception:
                continue

        if not ws:
            return {"status": "error", "message": "No se encontró columna de contraseñas en Google Sheets."}

        rows = ws.get_all_values()
        headers_norm = [normalize_fn(h) for h in rows[0]]
        name_col = next((i for i, h in enumerate(headers_norm) if h in ("nombre", "asesora")), 0)
        pwd_col = next((i for i, h in enumerate(headers_norm) if h in ("contrasena", "password", "clave")), None)

        for idx, row in enumerate(rows[1:], start=2):
            row_name = row[name_col] if name_col < len(row) else ""
            if normalize_fn(row_name) == nombre_norm:
                stored_pwd = str(row[pwd_col]).strip() if pwd_col < len(row) else ""
                if not stored_pwd:
                    ws.update_cell(idx, pwd_col + 1, password)
                    self._sheet_cache.pop(ws.title, None)
                    logger.info(f"LOGIN: Primera contraseña registrada para {display_name} en {ws.title}")
                    return {"status": "success", "nombre": display_name}
                if stored_pwd == password:
                    return {"status": "success", "nombre": display_name}
                return {"status": "error", "message": "Contraseña incorrecta."}

        # Si la asesora está activa pero aún no tiene fila en la pestaña de contraseñas, la agregamos
        if ws.title == "Contraseñas":
            new_row = [""] * max(name_col + 1, pwd_col + 1)
            new_row[name_col] = display_name
            new_row[pwd_col] = password
            ws.append_row(new_row)
            self._sheet_cache.pop(ws.title, None)
            logger.info(f"LOGIN: Fila y primera contraseña creadas para {display_name} en {ws.title}")
            return {"status": "success", "nombre": display_name}

        return {"status": "error", "message": "Asesora no encontrada en contraseñas."}

    def sincronizar_prospecto_a_sheet(self, data):
        if not self.workbook:
            logger.warning("GOOGLE SHEETS: no hay libro abierto para sincronizar.")
            return {"status": "error", "message": "Google Sheets no autenticado"}
        logger.info(f"GOOGLE SHEETS: sincronizando prospecto {data.get('Nombre')}")
        return {"status": "success"}

class PoolLocalStore:
    """Almacén SQLite local en el servidor para congelar locked_at y estado de mensaje enviado."""
    def __init__(self, db_path=None):
        default_path = os.getenv("POOL_SQLITE_PATH", "/app/data/pool_state.db")
        self.db_path = db_path or default_path
        try:
            parent = os.path.dirname(self.db_path)
            if parent:
                os.makedirs(parent, exist_ok=True)
            self._init_db()
        except Exception:
            self.db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "pool_state.db")
            self._init_db()

    def _connect(self):
        conn = sqlite3.connect(self.db_path, timeout=10)
        conn.execute("PRAGMA journal_mode=WAL;")
        return conn

    def _init_db(self):
        with self._connect() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS pool_locks (
                    folio_i TEXT PRIMARY KEY,
                    asesora TEXT NOT NULL,
                    locked_at TEXT NOT NULL,
                    contacted INTEGER NOT NULL DEFAULT 0,
                    contacted_at TEXT
                )
            """)
            conn.commit()

    def record_lock(self, folio_i, asesora, locked_at_iso):
        try:
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO pool_locks (folio_i, asesora, locked_at, contacted, contacted_at)
                    VALUES (?, ?, ?, 0, NULL)
                    ON CONFLICT(folio_i) DO UPDATE SET
                        asesora = excluded.asesora,
                        locked_at = excluded.locked_at,
                        contacted = 0,
                        contacted_at = NULL
                    """,
                    (str(folio_i), str(asesora), str(locked_at_iso))
                )
                conn.commit()
        except Exception as e:
            logger.error(f"SQLITE record_lock error: {e}")

    def ensure_lock(self, folio_i, asesora, fallback_iso):
        try:
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT OR IGNORE INTO pool_locks (folio_i, asesora, locked_at, contacted)
                    VALUES (?, ?, ?, 0)
                    """,
                    (str(folio_i), str(asesora), str(fallback_iso))
                )
                conn.commit()
        except Exception as e:
            logger.error(f"SQLITE ensure_lock error: {e}")

    def get_locks_map(self, folios):
        if not folios:
            return {}
        try:
            folio_strs = [str(f) for f in folios if f is not None]
            if not folio_strs:
                return {}
            result = {}
            with self._connect() as conn:
                # Consultar en lotes de 500 para no exceder límite de parámetros SQLite
                for i in range(0, len(folio_strs), 500):
                    chunk = folio_strs[i:i + 500]
                    placeholders = ",".join("?" for _ in chunk)
                    cur = conn.execute(
                        f"SELECT folio_i, asesora, locked_at, contacted, contacted_at FROM pool_locks WHERE folio_i IN ({placeholders})",
                        chunk
                    )
                    for row in cur.fetchall():
                        result[row[0]] = {
                            "asesora": row[1],
                            "locked_at": row[2],
                            "contacted": bool(row[3]),
                            "contacted_at": row[4]
                        }
            return result
        except Exception as e:
            logger.error(f"SQLITE get_locks_map error: {e}")
            return {}

    def set_contacted(self, folio_i, asesora, contacted, now_iso):
        try:
            val = 1 if contacted else 0
            ts = str(now_iso) if contacted else None
            with self._connect() as conn:
                cur = conn.execute(
                    "UPDATE pool_locks SET contacted = ?, contacted_at = ? WHERE folio_i = ?",
                    (val, ts, str(folio_i))
                )
                if cur.rowcount == 0:
                    conn.execute(
                        "INSERT INTO pool_locks (folio_i, asesora, locked_at, contacted, contacted_at) VALUES (?, ?, ?, ?, ?)",
                        (str(folio_i), str(asesora or ""), str(now_iso), val, ts)
                    )
                conn.commit()
            return True
        except Exception as e:
            logger.error(f"SQLITE set_contacted error: {e}")
            return False

    def remove_lock(self, folio_i):
        try:
            with self._connect() as conn:
                conn.execute("DELETE FROM pool_locks WHERE folio_i = ?", (str(folio_i),))
                conn.commit()
        except Exception as e:
            logger.error(f"SQLITE remove_lock error: {e}")


class DataHandler:
    """
    Gestor de persistencia v4.0.
    Implementación de servicios de Calendario Maestro.
    """

    def __init__(self):
        url = os.getenv("SUPABASE_URL") or SUPABASE_URL
        key = os.getenv("SUPABASE_KEY") or SUPABASE_KEY
        if not key:
            self.supabase = None
            logger.warning("DATA HANDLER: SUPABASE_KEY no configurado. Operaciones de DB estarán deshabilitadas.")
        else:
            self.supabase: Client = create_client(url, key)
        self.sheets = GoogleSheetsSync()
        self.pool_store = PoolLocalStore()
        logger.info("DATA HANDLER v4.0: Servicios de Calendario y Pool SQLite habilitados.")

    def _normalize(self, text):
        """Normaliza texto eliminando acentos y convirtiendo a minúsculas."""
        if not text: return ""
        text = str(text).lower().strip()
        text = ''.join(c for c in unicodedata.normalize('NFD', text) if unicodedata.category(c) != 'Mn')
        return text

    def get_calendar_events(self, calendar_id):
        """Consulta la API de Google Calendar para obtener los próximos eventos."""
        try:
            # Obtenemos token de acceso de las credenciales de la cuenta de servicio
            if not self.sheets.creds.valid:
                from google.auth.transport.requests import Request
                self.sheets.creds.refresh(Request())
            
            token = self.sheets.creds.token
            url = f"https://www.googleapis.com/calendar/v3/calendars/{calendar_id}/events"
            params = {
                "timeMin": datetime.utcnow().isoformat() + "Z",
                "maxResults": 10,
                "singleEvents": "true",
                "orderBy": "startTime"
            }
            headers = {"Authorization": f"Bearer {token}"}
            
            res = requests.get(url, params=params, headers=headers)
            items = res.json().get('items', [])
            
            events = []
            for item in items:
                events.append({
                    "summary": item.get('summary', 'Cita sin título'),
                    "start": item.get('start', {}).get('dateTime') or item.get('start', {}).get('date'),
                    "description": item.get('description', '')
                })
            return events
        except Exception as e:
            logger.error(f"CALENDAR API ERROR: {e}")
            return []

    def _limpiar_canal(self, telefono):
        if telefono is None: return None
        str_num = "".join(filter(str.isdigit, str(telefono)))
        return int(str_num[-10:]) if len(str_num) >= 10 else None

    def _folio_r_rank(self, folio_str):
        """Extrae el valor numérico de un Folio R (ej. 'R-048561' -> 48561) para ordenar del más actual al más antiguo."""
        if not folio_str:
            return -1
        digits = "".join(filter(str.isdigit, str(folio_str)))
        return int(digits) if digits else -1

    def lookup_folio_r_by_phone(self, telefono):
        """Consulta en modo SOLO LECTURA la tabla IDREPORTE_AGENDA_OBSOLETA por teléfono."""
        if not self.supabase:
            return {"found": False, "folio_r": "", "folios_r": [], "nombre": ""}
        canal_num = self._limpiar_canal(telefono)
        if not canal_num:
            return {"found": False, "folio_r": "", "folios_r": [], "nombre": ""}

        rows = []
        try:
            res = self.supabase.table("IDREPORTE_AGENDA_OBSOLETA") \
                .select("folio_r, nombre, telefono") \
                .ilike("telefono", f"%{canal_num}") \
                .execute()
            rows = res.data or []
        except Exception:
            try:
                res = self.supabase.table("IDREPORTE_AGENDA_OBSOLETA") \
                    .select("folio_r, nombre, telefono") \
                    .eq("telefono", canal_num) \
                    .execute()
                rows = res.data or []
            except Exception as e:
                logger.error(f"LOOKUP IDREPORTE ERROR: {e}")
                return {"found": False, "folio_r": "", "folios_r": [], "nombre": ""}

        if not rows:
            return {"found": False, "folio_r": "", "folios_r": [], "nombre": ""}

        # Ordenar por número de folio_r descendente (más alto = más actual)
        rows_sorted = sorted(rows, key=lambda r: self._folio_r_rank(r.get("folio_r")), reverse=True)

        seen = set()
        folios_r = []
        best_nombre = ""
        for r in rows_sorted:
            f_val = str(r.get("folio_r") or "").strip()
            if f_val and f_val not in seen:
                seen.add(f_val)
                folios_r.append(f_val)
            if not best_nombre:
                n_val = str(r.get("nombre") or "").strip()
                if n_val:
                    best_nombre = n_val

        return {
            "found": bool(folios_r or best_nombre),
            "folio_r": folios_r[0] if folios_r else "",
            "folios_r": folios_r,
            "nombre": best_nombre
        }

    def _parse_fecha_hora_asesoria(self, fecha_str, hora_str):
        """Combina fecha_de_la_asesoria y hora_cita en un datetime aware en America/Mexico_City."""
        if not fecha_str or not hora_str:
            return None
        f_clean = str(fecha_str).strip()
        h_clean = str(hora_str).strip().lower().replace('p. m.', 'pm').replace('a. m.', 'am').replace('p.m.', 'pm').replace('a.m.', 'am')

        # 1. Parsear fecha
        date_obj = None
        for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%Y/%m/%d", "%d-%m-%Y"):
            try:
                date_obj = datetime.strptime(f_clean[:10], fmt).date()
                break
            except Exception:
                continue
        if not date_obj:
            return None

        # 2. Parsear hora
        time_obj = None
        for h_fmt in ("%H:%M:%S", "%H:%M", "%I:%M:%S %p", "%I:%M %p", "%I:%M%p", "%I %p"):
            try:
                time_obj = datetime.strptime(h_clean.upper(), h_fmt).time()
                break
            except Exception:
                continue
        if not time_obj:
            return None

        naive_dt = datetime.combine(date_obj, time_obj)
        return self.TZ_MEX.localize(naive_dt)

    def _get_asesor_aliases(self, asesora_nombre):
        """Obtiene los términos equivalentes desde la columna 'diccionario' de AsesorasActivas."""
        norm_name = self._normalize(asesora_nombre)
        if not norm_name:
            return []
        aliases = {norm_name}

        try:
            rows = self.sheets.obtener_datos_hoja("AsesorasActivas")
            match = next(
                (r for r in rows if self._normalize(r.get('nombre') or r.get('Nombre') or '') == norm_name),
                None
            )
            if match:
                raw_dict = match.get('diccionario') or match.get('Diccionario') or match.get('DICCIONARIO') or ''
                for part in str(raw_dict).split(','):
                    p = self._normalize(part)
                    if p:
                        aliases.add(p)
        except Exception as e:
            logger.error(f"DICCIONARIO ASESORAS ERROR: {e}")

        return list(aliases)

    AGENDA_PILOT_USERS = {"viri", "viridiana", "bety", "beatriz"}

    def get_upcoming_agenda_for_agent(self, asesora_nombre):
        """Obtiene próximas asesorías de IDREPORTE_AGENDA_OBSOLETA con margen de +1 hora (solo lectura)."""
        if not self.supabase or not asesora_nombre:
            return []
        if self._normalize(asesora_nombre) not in self.AGENDA_PILOT_USERS:
            return []
        try:
            now_mx = datetime.now(self.TZ_MEX)
            cutoff = now_mx - timedelta(hours=1)
            alias_list = self._get_asesor_aliases(asesora_nombre)
            alias_set = set(alias_list)

            rows = []
            seen_keys = set()
            for token in alias_list:
                offset, limit = 0, 1000
                while True:
                    res = self.supabase.table("IDREPORTE_AGENDA_OBSOLETA") \
                        .select("folio_r, nombre, telefono, fecha_de_la_asesoria, hora_cita, asesor") \
                        .ilike("asesor", f"%{token}%") \
                        .range(offset, offset + limit - 1) \
                        .execute()
                    batch = res.data or []
                    for r in batch:
                        dedup_key = (r.get("folio_r"), r.get("fecha_de_la_asesoria"), r.get("hora_cita"))
                        if dedup_key not in seen_keys:
                            seen_keys.add(dedup_key)
                            rows.append(r)
                    if len(batch) < limit:
                        break
                    offset += limit

            events = []
            for r in rows:
                if self._normalize(r.get("asesor") or "") not in alias_set:
                    continue
                ev_dt = self._parse_fecha_hora_asesoria(r.get("fecha_de_la_asesoria"), r.get("hora_cita"))
                if not ev_dt or ev_dt < cutoff:
                    continue
                folio_r = str(r.get("folio_r") or "").strip()
                nombre = str(r.get("nombre") or "").strip()
                telefono = str(r.get("telefono") or "").strip()
                events.append((
                    ev_dt,
                    {
                        "summary": nombre or folio_r or "Asesoría",
                        "folio_r": folio_r,
                        "nombre": nombre,
                        "telefono": telefono,
                        "start": ev_dt.isoformat(),
                        "description": f"Tel: {telefono}" if telefono else ""
                    }
                ))

            events.sort(key=lambda pair: pair[0])
            return [ev for _, ev in events[:20]]
        except Exception as e:
            logger.error(f"AGENDA IDREPORTE ERROR: {e}")
            return []

    def validate_asesor_names(self):
        """Compara nombres de AsesorasActivas (Sheets) vs asesor en IDREPORTE_AGENDA_OBSOLETA."""
        try:
            active_names = self.obtener_asesoras_activas()
            active_map = {self._normalize(n): n for n in active_names}

            idrep_map = {}
            offset, limit = 0, 1000
            while True:
                res = self.supabase.table("IDREPORTE_AGENDA_OBSOLETA") \
                    .select("asesor") \
                    .range(offset, offset + limit - 1) \
                    .execute()
                batch = res.data or []
                for r in batch:
                    raw = str(r.get("asesor") or "").strip()
                    if raw:
                        idrep_map[self._normalize(raw)] = raw
                if len(batch) < limit:
                    break
                offset += limit

            coinciden = sorted([active_map[k] for k in active_map if k in idrep_map], key=self._normalize)
            solo_en_sheets = sorted([active_map[k] for k in active_map if k not in idrep_map], key=self._normalize)
            solo_en_idreporte = sorted([idrep_map[k] for k in idrep_map if k not in active_map], key=self._normalize)

            return {
                "coinciden": coinciden,
                "solo_en_sheets": solo_en_sheets,
                "solo_en_idreporte": solo_en_idreporte
            }
        except Exception as e:
            logger.error(f"VALIDATE ASESORES ERROR: {e}")
            return {"status": "error", "message": str(e)}

    def _formatear_fecha_sql(self, fecha_str):
        if not fecha_str or fecha_str in ['--', '', None]: return None
        try:
            date_obj = datetime.strptime(str(fecha_str), "%d/%m/%Y")
            return date_obj.strftime("%Y-%m-%d")
        except: return fecha_str

    def _formatear_fecha_ui(self, fecha_db):
        if not fecha_db: return None
        try:
            date_obj = datetime.strptime(str(fecha_db), "%Y-%m-%d")
            return date_obj.strftime("%d/%m/%Y")
        except: return fecha_db

    def registrar_prospecto(self, datos):
        """
        Registra prospecto con validación preventiva.
        Si es duplicado, el proceso se aborta AQUÍ para evitar colas de sincronización.
        """
        canal_num = self._limpiar_canal(datos.get('Canal'))
        if not canal_num: 
            return {"status": "error", "message": "Canal inválido."}
        
        try:
            # 1. VERIFICACIÓN PREVENTIVA (PRIMERA LÍNEA)
            # Buscamos el canal en Supabase antes de cualquier otra acción
            check = self.supabase.table("prospectos").select("id, asesora").eq("canal", canal_num).execute()
            if check.data and len(check.data) > 0:
                existing_asesora = check.data[0].get('asesora') or ''
                is_same_agent = self._normalize(existing_asesora) == self._normalize(datos.get('Asesora') or '')
                logger.warning(f"REGISTRO DESCARTADO: El número {canal_num} ya existe (asesora: {existing_asesora}).")
                return {
                    "status": "duplicate",
                    "message": (
                        "Este número ya está registrado en tu propia lista de prospectos."
                        if is_same_agent
                        else "El número que se intenta registrar ya pertenece a otra persona."
                    )
                }

            # 2. PROCESAMIENTO (Solo si no es duplicado)
            drive_url = ""
            files = datos.get('files_payload', [])
            if files:
                nombre_archivo = f"Registro - {files[0]['name']}"
                res_drive = self.sheets.subir_evidencia_drive(datos.get('Nombre'), files[0]['base64Data'], nombre_archivo)
                drive_url = res_drive.get('folderUrl', "")
            
            tz_mex = pytz.timezone('America/Mexico_City')
            fecha_prox = datos.get('Fecha Próx. Contacto')
            rendimiento = "Sin Cita"
            if fecha_prox and fecha_prox != '--':
                try:
                    fp = datetime.strptime(str(fecha_prox), "%d/%m/%Y").date()
                    now_mx = datetime.now(tz_mex).date()
                    if fp >= now_mx: rendimiento = "AL DIA"
                    else: rendimiento = "VENCIDO"
                except: pass
            
            lookup_r = self.lookup_folio_r_by_phone(canal_num)
            folio_r_actual = lookup_r.get("folio_r") or None
            folios_r_hist = ", ".join(lookup_r.get("folios_r") or []) or None

            payload = {
                "canal": canal_num, 
                "nombre": datos.get('Nombre'),
                "nivel_interes": datos.get('Nivel de Interés'), 
                "resumen": datos.get('Resumen Conversación'),
                "estado_final": datos.get('Estado Final'), 
                "asesora": datos.get('Asesora'),
                "fecha_registro": self._formatear_fecha_sql(datos.get('Fecha 1er Contacto')),
                "fecha_proxima": self._formatear_fecha_sql(datos.get('Fecha Próx. Contacto')),
                "imagenes_url": drive_url, 
                "updated_at": datetime.now(tz_mex).isoformat(),
                "rendimiento": rendimiento
            }
            if folio_r_actual:
                payload["folio_r"] = folio_r_actual
            if folios_r_hist:
                payload["folios_r_historial"] = folios_r_hist
            
            try:
                self.supabase.table("prospectos").insert(payload).execute()
            except Exception as ins_err:
                # Si las columnas folio_r / folios_r_historial aún no existen en Supabase, reintentar sin ellas
                if "folio_r" in str(ins_err):
                    payload.pop("folio_r", None)
                    payload.pop("folios_r_historial", None)
                    self.supabase.table("prospectos").insert(payload).execute()
                else:
                    raise ins_err
            logger.info(f"REGISTRO EXITOSO: {datos.get('Nombre')} ({canal_num})")
            return {"status": "success", "message": "Prospecto registrado correctamente.", "folio_r": folio_r_actual or ""}
            
        except Exception as e:
            err_str = str(e)
            if "23505" in err_str or "duplicate key" in err_str.lower():
                return {"status": "duplicate", "message": "El número que se intenta registrar ya pertenece a otra persona."}
            logger.error(f"FALLO REGISTRO: {err_str}")
            return {"status": "error", "message": err_str}

    def subir_evidencia_fondo(self, nombre_original, files_payload, num_seg, p_id):
        """
        Método asíncrono para subir archivos a Google Drive.
        Evita que las conexiones desde Google Chrome (Mac) se corten por Timeouts.
        Actualiza el registro en Supabase solo si logró obtener el folderUrl de la evidencia.
        """
        try:
            if files_payload:
                logger.info(f"HILO FONDO: Iniciando subida de Drive para '{nombre_original}'")
                nombre_archivo = f"Seguimiento {num_seg} - {files_payload[0]['name']}"
                res_drive = self.sheets.subir_evidencia_drive(nombre_original, files_payload[0]['base64Data'], nombre_archivo)
                
                if res_drive and res_drive.get('folderUrl'):
                    # Si exitoso, actualizamos solo la URL de imagen en el registro prospecto
                    self.supabase.table("prospectos").update({"imagenes_url": res_drive.get('folderUrl')}).eq("id", p_id).execute()
                    logger.info(f"HILO FONDO OK: Imagen guardada en BD para '{nombre_original}'")
                else:
                    logger.warning(f"HILO FONDO: Subida falló o sin URL para '{nombre_original}'")
        except Exception as e:
            logger.error(f"HILO FONDO ERROR: No se subió archivo para '{nombre_original}' -> {e}")

    def actualizar_prospecto_avanzado(self, p_id, updates, files_payload=None):
        try:
            p_res = self.supabase.table("prospectos").select("id, canal, fecha_proxima, estado_final").eq("id", p_id).execute()
            if not p_res.data: return {"status": "error", "message": "Registro no encontrado usando ID principal."}
            
            estado_actual = p_res.data[0].get('estado_final')
            if estado_actual in ["Venta", "No interesado"]:
                return {"status": "error", "message": f"Seguridad: El prospecto ya está como '{estado_actual}' y no puede ser modificado."}
            
            p_canal = p_res.data[0]['canal']
            fecha_ant_db = p_res.data[0].get('fecha_proxima')
            fecha_ant_str = self._formatear_fecha_ui(fecha_ant_db) if fecha_ant_db else "Sin cita previa"
            
            tz_mex = pytz.timezone('America/Mexico_City')
            now_mx = datetime.now(tz_mex).date()
            rendimiento_str = "Sin Cita"
            if fecha_ant_db:
                try:
                    date_db = datetime.strptime(str(fecha_ant_db), "%Y-%m-%d").date()
                    diff = (now_mx - date_db).days
                    if diff <= 0: rendimiento_str = "AL DIA"
                    elif diff == 1: rendimiento_str = "ALERTA"
                    else: rendimiento_str = "VENCIDO"
                except: pass
            
            num_seg = "Gral"
            for k in updates.keys():
                if "Notas Seguimiento" in k:
                    try: num_seg = k.split(" ")[-1]; break
                    except: pass
                    
            maestro_payload = {
                "estado_final": updates.get('Estado Final'), "nivel_interes": updates.get('Nivel de Interés'),
                "fecha_proxima": self._formatear_fecha_sql(updates.get('Fecha Próx. Contacto')),
                "comentarios": updates.get('Comentarios'), "updated_at": datetime.now(tz_mex).isoformat(),
                "rendimiento": rendimiento_str
            }
            # Se ha removido la lógica síncrona de subida de Drive aquí, favoreciendo la BD
            self.supabase.table("prospectos").update(maestro_payload).eq("id", p_id).execute()
            
            for key, val in updates.items():
                if "Notas Seguimiento" in key and val:
                    try:
                        num_paso = int(key.split(" ")[-1])
                        fecha_seg = updates.get(f"Fecha Seguimiento {num_paso}")
                        nota_modificada = f"Cita anterior programada: {fecha_ant_str}\n{val}"
                        seg_payload = {
                            "prospecto_id": p_id, "prospecto_canal": p_canal,
                            "numero_paso": num_paso, "fecha_seguimiento": self._formatear_fecha_sql(fecha_seg),
                            "nota_seguimiento": nota_modificada, "created_at": datetime.now(tz_mex).isoformat()
                        }
                        self.supabase.table("seguimientos").insert(seg_payload).execute()
                    except Exception as e:
                        logger.error(f"Fallo al insertar seguimiento #{num_paso} para prospecto {p_id}: {e}")
                        return {"status": "error", "message": f"Fallo al registrar seguimiento #{num_paso}. Puede que ya exista o haya conflicto."}
            
            # Retornamos p_id y num_seg para que si se requiere subida a drive, el app.py tenga la información necesaria
            return {"status": "success", "message": "Expediente sincronizado.", "p_id": p_id, "num_seg": num_seg}
        except Exception as e: return {"status": "error", "message": str(e)}

    def delete_client_db(self, name, canal, imagenes_url=None):
        canal_limpio = self._limpiar_canal(canal)
        try:
            url_final = imagenes_url
            if not url_final:
                res = self.supabase.table("prospectos").select("imagenes_url").eq("canal", canal_limpio).execute()
                if res.data: url_final = res.data[0].get('imagenes_url')
            if url_final: self.sheets.borrar_carpeta_drive(url_final)
            self.supabase.table("prospectos").delete().eq("canal", canal_limpio).execute()
            return True, "Borrado con éxito."
        except Exception as e: return False, str(e)

    TZ_MEX = pytz.timezone('America/Mexico_City')

    def _parse_timestamptz(self, ts_str):
        """Parsea timestamptz de Supabase/Postgres (incluyendo offsets cortos como +00) de forma segura."""
        if not ts_str:
            return None
        s = str(ts_str).strip().replace('Z', '+00:00')
        if re.search(r'[+-]\d{2}$', s):
            s = s + ':00'
        dt = datetime.fromisoformat(s)
        if not dt.tzinfo:
            dt = self.TZ_MEX.localize(dt)
        return dt

    def get_all_clients(self):
        if not self.supabase: return []
        all_data = []
        offset, limit = 0, 1000
        cols = "id, nombre, asesora, canal, fecha_registro, nivel_interes, fecha_proxima, estado_final, rendimiento, folio_r"
        fallback_cols = "id, nombre, asesora, canal, fecha_registro, nivel_interes, fecha_proxima, estado_final, rendimiento"
        try:
            today_mx = datetime.now(self.TZ_MEX).date()
            while True:
                try:
                    res = self.supabase.table("prospectos") \
                        .select(cols) \
                        .order("updated_at", desc=True) \
                        .range(offset, offset + limit - 1).execute()
                except Exception as col_err:
                    if "folio_r" in str(col_err):
                        cols = fallback_cols
                        res = self.supabase.table("prospectos") \
                            .select(cols) \
                            .order("updated_at", desc=True) \
                            .range(offset, offset + limit - 1).execute()
                    else:
                        raise col_err
                batch = res.data
                if not batch: break
                for item in batch: all_data.append(self._reconstruir_objeto_prospecto(item, today_mx))
                if len(batch) < limit: break
                offset += limit
            return all_data
        except: return []

    def get_client_full_profile(self, p_id):
        try:
            res = self.supabase.table("prospectos").select("*, seguimientos!seguimientos_prospecto_id_fkey(numero_paso, fecha_seguimiento, nota_seguimiento)").eq("id", p_id).execute()
            if not res.data: return None
            profile = self._reconstruir_objeto_prospecto(res.data[0])
            # Enriquecer en modo solo lectura con todos los Folios R de IDREPORTE_AGENDA_OBSOLETA
            lookup_r = self.lookup_folio_r_by_phone(profile.get('canal'))
            if lookup_r.get('folios_r'):
                profile['folio_r'] = lookup_r['folio_r']
                profile['folios_r_list'] = lookup_r['folios_r']
            else:
                hist_str = profile.get('folios_r_historial') or ''
                hist_list = [s.strip() for s in str(hist_str).split(',') if s.strip()]
                if not hist_list and profile.get('folio_r'):
                    hist_list = [profile.get('folio_r')]
                profile['folios_r_list'] = hist_list
            return profile
        except: return None

    def _reconstruir_objeto_prospecto(self, p, today_mx=None):
        p['id_db'] = p.get('id')
        p['nombre'] = p.get('nombre') or "Sin Nombre"
        p['fecha_registro'] = self._formatear_fecha_ui(p.get('fecha_registro'))
        
        fp_db = p.get('fecha_proxima')
        p['fecha_proxima'] = self._formatear_fecha_ui(fp_db)
        
        rend = p.get('rendimiento')
        if not rend or rend == "Sin Cita":
            rend = "Sin Cita"
            if fp_db:
                try:
                    fp = datetime.strptime(str(fp_db), "%Y-%m-%d").date()
                    now_mx = today_mx or datetime.now(self.TZ_MEX).date()
                    diff = (now_mx - fp).days
                    if diff <= 0: rend = "AL DIA"
                    elif diff == 1: rend = "ALERTA"
                    else: rend = "VENCIDO"
                except: pass
        p['rendimiento'] = rend
        
        if 'imagenes_url' in p:
            p['Imagenes'] = p.get('imagenes_url') or ""
        segs = p.get('seguimientos!seguimientos_prospecto_id_fkey') or p.get('seguimientos') or []
        segs.sort(key=lambda x: x.get('numero_paso', 0))
        for s in segs:
            paso = s.get('numero_paso')
            if paso and 1 <= paso <= 30:
                p[f'fecha_seguimiento_{paso}'] = self._formatear_fecha_ui(s.get('fecha_seguimiento'))
                p[f'notas_seguimiento_{paso}'] = s.get('nota_seguimiento')
        p.pop('seguimientos', None); p.pop('seguimientos!seguimientos_prospecto_id_fkey', None)
        return p

    def login_asesora(self, nombre, password=None):
        """Valida el acceso de la asesora en AsesorasActivas y verifica/registra su contraseña."""
        if not nombre or not password or not str(password).strip():
            return {"status": "error", "message": "Nombre y contraseña requeridos."}
        try:
            asesoras = self.sheets.obtener_datos_hoja("AsesorasActivas")
            nombre_norm = self._normalize(nombre)
            pwd_input = str(password).strip()

            match = next(
                (a for a in asesoras if self._normalize(a.get('nombre') or a.get('Nombre') or '') == nombre_norm),
                None
            )
            if not match:
                logger.warning(f"LOGIN: Intento fallido para {nombre}")
                return {"status": "error", "message": "Asesora no autorizada."}

            display_name = match.get('nombre') or match.get('Nombre')
            res = self.sheets.verificar_o_guardar_password(nombre_norm, display_name, pwd_input, self._normalize)
            if res.get("status") == "success":
                logger.info(f"LOGIN: Acceso concedido a {display_name}")
            else:
                logger.warning(f"LOGIN: Acceso denegado para {display_name}: {res.get('message')}")
            return res
        except Exception as e:
            logger.error(f"LOGIN ERROR: {e}")
            return {"status": "error", "message": "Error de conexión con base de asesoras."}

    def obtener_asesoras_activas(self):
        """Retorna una lista simple de nombres en orden alfabético para el dropdown del CRM."""
        try:
            data = self.sheets.obtener_datos_hoja("AsesorasActivas")
            # Extraer solo la columna 'Nombre' que no esté vacía
            names = [str(a.get('nombre') or a.get('Nombre')).strip() for a in data if (a.get('nombre') or a.get('Nombre'))]
            return sorted(names, key=lambda x: self._normalize(x))
        except Exception as e:
            logger.error(f"DROPDOWN ERROR: {e}")
            return []

    def obtener_auditores(self):
        try:
            data = self.sheets.obtener_datos_hoja("Auditores")
            return [str(a.get('Nombre')) for a in data if a.get('Nombre')]
        except: return []

    def login_auditoria(self, nombre, password):
        try:
            auditores = self.sheets.obtener_datos_hoja("Auditores")
            match = next((a for a in auditores if str(a.get('Nombre') or '').strip().lower() == str(nombre).strip().lower()), None)
            if match and str(match.get('Contraseña') or '').strip() == str(password).strip():
                return {"status": "success", "nombre": match.get('Nombre'), "permisos": match.get('Permisos') or "Visualizador"}
            return {"status": "error", "message": "Invalido."}
        except: return {"status": "error"}

    def get_clients_for_agent(self, agent_name):
        if not self.supabase or not agent_name: return []
        all_data = []
        offset, limit = 0, 1000
        cols = "id, nombre, asesora, canal, fecha_registro, nivel_interes, fecha_proxima, estado_final, rendimiento, folio_r"
        fallback_cols = "id, nombre, asesora, canal, fecha_registro, nivel_interes, fecha_proxima, estado_final, rendimiento"
        try:
            today_mx = datetime.now(self.TZ_MEX).date()
            while True:
                try:
                    res = self.supabase.table("prospectos") \
                        .select(cols) \
                        .ilike("asesora", f"%{agent_name}%") \
                        .order("updated_at", desc=True) \
                        .range(offset, offset + limit - 1).execute()
                except Exception as col_err:
                    if "folio_r" in str(col_err):
                        cols = fallback_cols
                        res = self.supabase.table("prospectos") \
                            .select(cols) \
                            .ilike("asesora", f"%{agent_name}%") \
                            .order("updated_at", desc=True) \
                            .range(offset, offset + limit - 1).execute()
                    else:
                        raise col_err
                batch = res.data
                if not batch: break
                for item in batch:
                    all_data.append(self._reconstruir_objeto_prospecto(item, today_mx))
                if len(batch) < limit: break
                offset += limit
            return all_data
        except: return []

    # --- INICIO MÓDULO POOL ---
    def get_pool_clients(self, asesora_nombre=None):
        try:
            # 1. Establecer hora actual en base a timezone local
            tz_mex = self.TZ_MEX
            now_mx = datetime.now(tz_mex)
            pool = []
            
            # --- CONSULTA 1: Obtener registros BLOQUEADOS (solo de la asesora si se especifica) ---
            blocked_data = []
            offset, limit = 0, 1000
            while True:
                query = self.supabase.table("AGENDA_OBSOLETA") \
                    .select("folio_i, telefono, status, hora, fecha, updated_at")
                if asesora_nombre:
                    query = query.eq("status", f"BLOQUEADO_{asesora_nombre}")
                else:
                    query = query.like("status", "BLOQUEADO_%")
                res = query.range(offset, offset + limit - 1).execute()
                batch = res.data or []
                blocked_data.extend(batch)
                if len(batch) < limit:
                    break
                offset += limit
                
            locks_map = self.pool_store.get_locks_map([item.get('folio_i') for item in blocked_data])
            blocked_pool = []
            for item in blocked_data:
                folio_key = str(item.get('folio_i'))
                st_str = str(item.get('status') or '')
                lock_owner = st_str[len("BLOQUEADO_"):] if st_str.startswith("BLOQUEADO_") else (asesora_nombre or "")
                lock_info = locks_map.get(folio_key)
                if lock_info and lock_info.get('locked_at'):
                    locked_at = lock_info['locked_at']
                    contacted = lock_info.get('contacted', False)
                else:
                    locked_at = item.get('updated_at') or now_mx.isoformat()
                    contacted = False
                    self.pool_store.ensure_lock(folio_key, lock_owner, locked_at)

                dt_sort = None
                if locked_at:
                    try:
                        dt = self._parse_timestamptz(locked_at)
                        dt_sort = dt
                        if dt and (now_mx - dt).days > 30:
                            # Si el apartado caducó, se modifica el status a vacío
                            item['status'] = ''
                    except Exception:
                        pass
                
                blocked_pool.append((
                    dt_sort.timestamp() if dt_sort else 0,
                    {
                        "folio_i": item.get('folio_i'),
                        "telefono": item.get('telefono'),
                        "status": item.get('status'),
                        "hora": item.get('hora'),
                        "fecha": item.get('fecha'),
                        "updated_at": item.get('updated_at'),
                        "locked_at": locked_at,
                        "contacted": contacted
                    }
                ))

            # Ordenar bloqueados: más nuevos arriba, más viejos abajo
            blocked_pool.sort(key=lambda pair: pair[0], reverse=True)
            for _, entry in blocked_pool:
                pool.append(entry)
            
            # --- CONSULTA 2: Obtener registros públicos (ISABELA) con antigüedad >= 4 horas en updated_at ---
            # Como updated_at es timestamptz (almacenado en UTC en Postgres), enviamos el corte en UTC para comparación exacta
            four_hours_ago_utc = (now_mx - timedelta(hours=4)).astimezone(pytz.UTC).isoformat()
            public_res = self.supabase.table("AGENDA_OBSOLETA") \
                .select("folio_i, telefono, status, hora, fecha, updated_at") \
                .eq("status", "ISABELA") \
                .lte("updated_at", four_hours_ago_utc) \
                .order("updated_at", desc=True) \
                .limit(100) \
                .execute()
                
            for item in (public_res.data or []):
                if item.get('status') == 'ISABELA':
                    pool.append({
                        "folio_i": item.get('folio_i'),
                        "telefono": item.get('telefono'),
                        "status": item.get('status'),
                        "hora": item.get('hora'),
                        "fecha": item.get('fecha'),
                        "updated_at": item.get('updated_at')
                    })
                        
            return pool
        except Exception as e:
            logger.error(f"Error GET POOL: {str(e)}")
            return []

    def take_pool_client(self, lead_id, asesora_nombre):
        try:
            tz_mex = pytz.timezone('America/Mexico_City')
            now_mx = datetime.now(tz_mex)
            limit_check = self.supabase.table("AGENDA_OBSOLETA") \
                .select("folio_i, updated_at") \
                .eq("status", f"BLOQUEADO_{asesora_nombre}") \
                .execute()
            blocked_rows = limit_check.data or []
            if len(blocked_rows) >= 250:
                return {"status": "error", "message": "Límite de 250 prospectos en posesión alcanzado.", "code": 403}

            today_mx = now_mx.date()
            locks_map = self.pool_store.get_locks_map([row.get('folio_i') for row in blocked_rows])
            taken_today = 0
            for row in blocked_rows:
                folio_key = str(row.get('folio_i'))
                ref_ts = (locks_map.get(folio_key) or {}).get('locked_at') or row.get('updated_at')
                if ref_ts:
                    try:
                        dt = self._parse_timestamptz(ref_ts)
                        if dt and dt.astimezone(tz_mex).date() == today_mx:
                            taken_today += 1
                    except Exception:
                        pass
            if taken_today >= 10:
                return {"status": "error", "message": "Límite diario de 10 prospectos alcanzado. Descarta alguno de hoy para tomar más.", "code": 403}
                
            lead_check = self.supabase.table("AGENDA_OBSOLETA").select("status, updated_at").eq("folio_i", lead_id).execute()
            if not lead_check.data:
                return {"status": "error", "message": "No encontrado", "code": 404}
            
            st = (lead_check.data[0].get('status') or '').strip()
            up = lead_check.data[0].get('updated_at')
            available = False
            if st == 'ISABELA':
                if up:
                    try:
                        dt = self._parse_timestamptz(up)
                        if dt and (now_mx - dt) >= timedelta(hours=4):
                            available = True
                    except Exception:
                        pass
                else:
                    available = True
            elif st.startswith('BLOQUEADO_'):
                if up:
                    try:
                        dt = self._parse_timestamptz(up)
                        if dt and (now_mx - dt).days > 30:
                            available = True
                    except Exception:
                        pass
            
            if not available:
                return {"status": "error", "message": "El prospecto ya fue tomado", "code": 409}
                
            self.supabase.table("AGENDA_OBSOLETA").update({
                "status": f"BLOQUEADO_{asesora_nombre}",
                "updated": True,
                "updated_at": now_mx.isoformat()
            }).eq("folio_i", lead_id).execute()
            self.pool_store.record_lock(lead_id, asesora_nombre, now_mx.isoformat())
            
            return {"status": "success"}
        except Exception as e:
            return {"status": "error", "message": str(e), "code": 500}

    def resolve_pool_client(self, lead_id, asesora_nombre, accion, datos_validacion):
        try:
            tz_mex = pytz.timezone('America/Mexico_City')
            now_mx = datetime.now(tz_mex)
            
            new_status = ""
            if accion == 'descartar':
                new_status = datos_validacion
            elif accion == 'nsh':
                new_status = 'NSH'
            elif accion == 'agendar':
                new_status = f"AGENDAx{asesora_nombre}"
            else:
                return {"status": "error", "message": "Acción inválida", "code": 400}
                
            self.supabase.table("AGENDA_OBSOLETA").update({
                "status": new_status,
                "updated": True,
                "updated_at": now_mx.isoformat()
            }).eq("folio_i", lead_id).execute()
            self.pool_store.remove_lock(lead_id)
            return {"status": "success"}
        except Exception as e:
            return {"status": "error", "message": str(e), "code": 500}

    def toggle_pool_contacted(self, lead_id, asesora_nombre, contacted):
        if not lead_id:
            return {"status": "error", "message": "lead_id requerido", "code": 400}
        now_mx = datetime.now(self.TZ_MEX)
        ok = self.pool_store.set_contacted(lead_id, asesora_nombre, bool(contacted), now_mx.isoformat())
        if not ok:
            return {"status": "error", "message": "No se pudo guardar la marca.", "code": 500}
        return {"status": "success", "contacted": bool(contacted)}
    # --- FIN MÓDULO POOL ---

    def get_pool_audit_summary(self):
        """Retorna únicamente conteos numéricos de clientes en Pool por cada asesora activa."""
        try:
            asesoras = self.obtener_asesoras_activas()
            tz_mex = pytz.timezone('America/Mexico_City')
            now_mx = datetime.now(tz_mex)
            today_mx = now_mx.date()

            blocked_rows = []
            offset, limit = 0, 1000
            while True:
                res = self.supabase.table("AGENDA_OBSOLETA") \
                    .select("folio_i, status, updated_at") \
                    .like("status", "BLOQUEADO_%") \
                    .range(offset, offset + limit - 1) \
                    .execute()
                batch = res.data or []
                blocked_rows.extend(batch)
                if len(batch) < limit:
                    break
                offset += limit

            locks_map = self.pool_store.get_locks_map([item.get('folio_i') for item in blocked_rows])
            counts = {}
            for a in asesoras:
                counts[self._normalize(a)] = {"asesora": a, "tiene": 0, "hoy": 0}

            for item in blocked_rows:
                st = str(item.get('status') or '')
                if not st.startswith("BLOQUEADO_"):
                    continue
                asesora_raw = st[len("BLOQUEADO_"):]
                key = self._normalize(asesora_raw)
                if key not in counts:
                    continue

                counts[key]["tiene"] += 1
                folio_key = str(item.get('folio_i'))
                ref_ts = (locks_map.get(folio_key) or {}).get('locked_at') or item.get('updated_at')
                if ref_ts:
                    try:
                        dt = self._parse_timestamptz(ref_ts)
                        if dt and dt.astimezone(tz_mex).date() == today_mx:
                            counts[key]["hoy"] += 1
                    except Exception:
                        pass

            summary = []
            for entry in counts.values():
                tiene = entry["tiene"]
                hoy = entry["hoy"]
                summary.append({
                    "asesora": entry["asesora"],
                    "tiene": tiene,
                    "faltan": max(0, 250 - tiene),
                    "hoy": hoy,
                    "faltan_hoy": max(0, 10 - hoy)
                })
            summary.sort(key=lambda x: self._normalize(x["asesora"]))
            return summary
        except Exception as e:
            logger.error(f"Error POOL AUDIT SUMMARY: {e}")
            return []

    def edit_client_field(self, p_id, canal, field, new_value, auditor_nombre=None, password=None):
        """Permite editar campos de un prospecto. Campos sensibles requieren Super Admin y contraseña."""
        if not self.supabase:
            return {"status": "error", "message": "Base de datos no disponible.", "code": 500}

        field_map = {
            "nombre": "nombre",
            "canal": "canal",
            "telefono": "canal",
            "asesora": "asesora",
            "estado_final": "estado_final",
            "status": "estado_final",
            "resumen": "resumen",
            "comentarios": "comentarios"
        }
        col = field_map.get(str(field or "").strip().lower())
        if not col:
            return {"status": "error", "message": "Campo no permitido.", "code": 400}

        protected_cols = {"nombre", "canal", "asesora", "estado_final", "resumen"}
        if col in protected_cols:
            if not auditor_nombre or not password:
                return {"status": "error", "message": "Se requiere contraseña de Super Admin.", "code": 401}
            auditores = self.sheets.obtener_datos_hoja("Auditores")
            match = next(
                (a for a in auditores if self._normalize(a.get('Nombre') or '') == self._normalize(auditor_nombre)),
                None
            )
            if not match or str(match.get('Contraseña') or '').strip() != str(password).strip():
                return {"status": "error", "message": "Contraseña de Super Admin incorrecta.", "code": 401}
            permiso_norm = self._normalize(match.get('Permisos') or '')
            if permiso_norm not in ("superior", "super admin", "superadmin"):
                return {"status": "error", "message": "Solo Super Admin puede modificar este campo.", "code": 403}

        try:
            tz_mex = pytz.timezone('America/Mexico_City')
            val_to_save = str(new_value).strip() if new_value is not None else ""

            if col == "canal":
                cleaned = self._limpiar_canal(val_to_save)
                if not cleaned:
                    return {"status": "error", "message": "Teléfono/Canal inválido.", "code": 400}
                dup = self.supabase.table("prospectos").select("id").eq("canal", cleaned).execute()
                if dup.data:
                    for row in dup.data:
                        if str(row.get('id')) != str(p_id):
                            return {"status": "error", "message": "Ese teléfono ya está registrado en otro expediente.", "code": 409}
                val_to_save = cleaned

            payload = {
                col: val_to_save,
                "updated_at": datetime.now(tz_mex).isoformat()
            }

            if p_id:
                self.supabase.table("prospectos").update(payload).eq("id", p_id).execute()
            elif canal:
                canal_limpio = self._limpiar_canal(canal)
                self.supabase.table("prospectos").update(payload).eq("canal", canal_limpio).execute()
            else:
                return {"status": "error", "message": "Identificador de prospecto requerido.", "code": 400}

            return {"status": "success", "field": col, "new_value": val_to_save}
        except Exception as e:
            logger.error(f"Error EDIT CLIENT FIELD: {e}")
            return {"status": "error", "message": str(e), "code": 500}

# Instancia global
handler = DataHandler()