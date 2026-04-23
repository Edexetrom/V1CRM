import os
import logging
import requests
import json
import base64
import gspread
import unicodedata
from datetime import datetime, timedelta
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv
from supabase import create_client, Client
import re
import pytz
# Carga de variables de entorno
load_dotenv()

# Configuración de logs modular
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("DataHandler")

# --- CONFIGURACIÓN DE CONEXIÓN ---
SUPABASE_URL = "https://qldrdljyuqlyqwoauwyd.supabase.co"
SUPABASE_KEY = os.getenv("SUPABASE_KEY") or "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InFsZHJkbGp5dXFseXF3b2F1d3lkIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3MTQ0MDI3MCwiZXhwIjoyMDg3MDE2MjcwfQ.Ic0ExvTXGFFr69BmWkirjF4GrjFGeuOeYMaWPXsTIAk"

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

    def obtener_datos_hoja(self, nombre_hoja):
        if not self.workbook: return []
        try:
            ws = self.workbook.worksheet(nombre_hoja)
            return ws.get_all_records()
        except: return []

class DataHandler:
    """
    Gestor de persistencia v4.0.
    Implementación de servicios de Calendario Maestro.
    """

    def __init__(self):
        url = os.getenv("SUPABASE_URL") or SUPABASE_URL
        key = os.getenv("SUPABASE_KEY") or SUPABASE_KEY
        self.supabase: Client = create_client(url, key)
        self.sheets = GoogleSheetsSync()
        logger.info("DATA HANDLER v4.0: Servicios de Calendario habilitados.")

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
            check = self.supabase.table("prospectos").select("id").eq("canal", canal_num).execute()
            if check.data and len(check.data) > 0:
                logger.warning(f"REGISTRO DESCARTADO: El número {canal_num} ya existe en la base de datos.")
                # Retornamos un status específico 'duplicate' para que el servidor lo identifique
                return {
                    "status": "duplicate", 
                    "message": "El número que se intenta registrar ya pertenece a otra persona."
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
            
            self.supabase.table("prospectos").insert(payload).execute()
            logger.info(f"REGISTRO EXITOSO: {datos.get('Nombre')} ({canal_num})")
            return {"status": "success", "message": "Prospecto registrado correctamente."}
            
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

    def get_all_clients(self):
        if not self.supabase: return []
        all_data = []
        offset, limit = 0, 1000
        try:
            while True:
                res = self.supabase.table("prospectos") \
                    .select("id, nombre, asesora, canal, fecha_registro, nivel_interes, fecha_proxima, estado_final, rendimiento", count='exact') \
                    .order("updated_at", desc=True) \
                    .range(offset, offset + limit - 1).execute()
                batch = res.data
                if not batch: break
                for item in batch: all_data.append(self._reconstruir_objeto_prospecto(item))
                if len(batch) < limit: break
                offset += limit
            return all_data
        except: return []

    def get_client_full_profile(self, p_id):
        try:
            res = self.supabase.table("prospectos").select("*, seguimientos!seguimientos_prospecto_id_fkey(*)").eq("id", p_id).execute()
            if not res.data: return None
            return self._reconstruir_objeto_prospecto(res.data[0])
        except: return None

    def _reconstruir_objeto_prospecto(self, p):
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
                    now_mx = datetime.now(pytz.timezone('America/Mexico_City')).date()
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

    def login_asesora(self, nombre):
        """Valida el acceso de la asesora comparando con la pestaña AsesorasActivas."""
        if not nombre: return {"status": "error", "message": "Nombre requerido."}
        try:
            # Recuperar datos de Sheets
            asesoras = self.sheets.obtener_datos_hoja("AsesorasActivas")
            nombre_norm = self._normalize(nombre)
            
            # Buscar coincidencia normalizada
            match = next((a for a in asesoras if self._normalize(a.get('Nombre', '')) == nombre_norm), None)
            
            if match:
                logger.info(f"LOGIN: Acceso concedido a {match.get('Nombre')}")
                return {"status": "success", "nombre": match.get('Nombre')}
            
            logger.warning(f"LOGIN: Intento fallido para {nombre}")
            return {"status": "error", "message": "Asesora no autorizada."}
        except Exception as e:
            logger.error(f"LOGIN ERROR: {e}")
            return {"status": "error", "message": "Error de conexión con base de asesoras."}

    def obtener_asesoras_activas(self):
        """Retorna una lista simple de nombres para el dropdown del CRM."""
        try:
            data = self.sheets.obtener_datos_hoja("AsesorasActivas")
            # Extraer solo la columna 'Nombre' que no esté vacía
            return [str(a.get('nombre')) for a in data if a.get('nombre')]
        except Exception as e:
            logger.error(f"DROPDOWN ERROR: {e}")
            return []

    def obtener_auditores(self):
        try:
            data = self.sheets.obtener_datos_hoja("Auditores")
            return [str(a.get('Nombre')) for a in data if a.get('Nombre')]
        except: return []

    def login_asesora(self, nombre):
        try:
            asesoras = self.sheets.obtener_datos_hoja("AsesorasActivas")
            match = next((a for a in asesoras if str(a.get('nombre') or '').strip().lower() == str(nombre).strip().lower()), None)
            return {"status": "success", "nombre": match.get('nombre')} if match else {"status": "error", "message": "No autorizada."}
        except: return {"status": "error", "message": "Error de conexión."}

    def login_auditoria(self, nombre, password):
        try:
            auditores = self.sheets.obtener_datos_hoja("Auditores")
            match = next((a for a in auditores if str(a.get('Nombre') or '').strip().lower() == str(nombre).strip().lower()), None)
            if match and str(match.get('Contraseña') or '').strip() == str(password).strip():
                return {"status": "success", "nombre": match.get('Nombre'), "permisos": match.get('Permisos') or "Visualizador"}
            return {"status": "error", "message": "Invalido."}
        except: return {"status": "error"}

    def get_clients_for_agent(self, agent_name):
        try:
            res = self.supabase.table("prospectos").select("id, nombre, asesora, canal, fecha_registro, nivel_interes, fecha_proxima, estado_final, rendimiento").ilike("asesora", f"%{agent_name}%").order("updated_at", desc=True).execute()
            return [self._reconstruir_objeto_prospecto(item) for item in res.data]
        except: return []

    # --- INICIO MÓDULO POOL ---
    def get_pool_clients(self):
        try:
            # 1. Establecer hora actual en base a timezone local
            tz_mex = pytz.timezone('America/Mexico_City')
            now_mx = datetime.now(tz_mex)
            
            offset = 0
            limit = 50 # Lote más pequeño para optimizar consultas a Supabase
            pool = []
            
            # Se hace el query sin created_at para evitar el error column AGENDA_OBSOLETA.created_at does not exist
            while True:
                res = self.supabase.table("AGENDA_OBSOLETA").select("folio_i, telefono, status, hora, fecha, updated, updated_at").order("updated_at", desc=True).range(offset, offset + limit - 1).execute()
                if not res.data: break
                
                for item in res.data:
                    st = item.get('status') or ''
                    valid = False
                    
                    if st in ['', 'NSH', 'LIBRE'] or st is None:
                        # 2. EVALUACIÓN DE ANTIGÜEDAD (Regla 3 días usando la columna 'fecha')
                        fecha_str = item.get('fecha')
                        
                        if fecha_str:
                            try:
                                # Parsear la fecha soportando formatos convencionales YYYY-MM-DD o DD/MM/YYYY
                                fecha_str_clean = str(fecha_str).strip()[:10]
                                if '-' in fecha_str_clean:
                                    dt_fecha = datetime.strptime(fecha_str_clean, "%Y-%m-%d").date()
                                elif '/' in fecha_str_clean:
                                    dt_fecha = datetime.strptime(fecha_str_clean, "%d/%m/%Y").date()
                                else:
                                    dt_fecha = now_mx.date() # Fallback
                                    
                                # 3. Calcular la diferencia exacta de días contra el hoy en CDMX
                                diff_days = (now_mx.date() - dt_fecha).days
                                
                                # Si tiene 3 días o más de antigüedad, se libera al Pool público
                                if diff_days >= 3:
                                    valid = True
                            except Exception as e:
                                valid = False
                                
                    elif st.startswith('BLOQUEADO_'):
                        # Siempre debe ser True para que el registro viaje al frontend y aparezca en Mis Reclamados
                        valid = True
                        up = item.get('updated_at')
                        if up:
                            try:
                                dt = datetime.fromisoformat(str(up).replace('Z', '+00:00'))
                                if not dt.tzinfo:
                                    dt = tz_mex.localize(dt)
                                diff = (now_mx - dt).days
                                if diff > 5:
                                    # Si el apartado caducó, se modifica temporalmente el status a vacío 
                                    item['status'] = ''
                            except: pass
                            
                    if valid:
                        pool.append({
                            "folio_i": item.get('folio_i'),
                            "telefono": item.get('telefono'),
                            "status": item.get('status'),
                            "hora": item.get('hora'),
                            "fecha": item.get('fecha'),
                            "updated": item.get('updated'),
                            "updated_at": item.get('updated_at')
                        })
                        
                        # Detener el ciclo si ya logramos 10 prospectos válidos
                        if len(pool) == 10:
                            return pool
                            
                if len(res.data) < limit: break
                offset += limit
                
            return pool
        except Exception as e:
            logger.error(f"Error GET POOL: {str(e)}")
            return []

    def take_pool_client(self, lead_id, asesora_nombre):
        try:
            tz_mex = pytz.timezone('America/Mexico_City')
            now_mx = datetime.now(tz_mex)
            limit_check = self.supabase.table("AGENDA_OBSOLETA").select("folio_i").eq("status", f"BLOQUEADO_{asesora_nombre}").execute()
            if limit_check.data and len(limit_check.data) >= 10:
                return {"status": "error", "message": "Límite de 10 prospectos alcanzado.", "code": 403}
                
            lead_check = self.supabase.table("AGENDA_OBSOLETA").select("status, updated_at").eq("folio_i", lead_id).execute()
            if not lead_check.data:
                return {"status": "error", "message": "No encontrado", "code": 404}
            
            st = lead_check.data[0].get('status') or ''
            up = lead_check.data[0].get('updated_at')
            available = False
            if st in ['', 'NSH'] or st is None:
                available = True
            elif st.startswith('BLOQUEADO_'):
                if up:
                    try:
                        dt = datetime.fromisoformat(str(up).replace('Z', '+00:00'))
                        if not dt.tzinfo:
                            dt = tz_mex.localize(dt)
                        if (now_mx - dt).days > 5:
                            available = True
                    except: pass
            
            if not available:
                return {"status": "error", "message": "El prospecto ya fue tomado", "code": 409}
                
            self.supabase.table("AGENDA_OBSOLETA").update({
                "status": f"BLOQUEADO_{asesora_nombre}",
                "updated": True,
                "updated_at": now_mx.isoformat()
            }).eq("folio_i", lead_id).execute()
            
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
            return {"status": "success"}
        except Exception as e:
            return {"status": "error", "message": str(e), "code": 500}
    # --- FIN MÓDULO POOL ---

# Instancia global
handler = DataHandler()