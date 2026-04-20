"""
procesar_dashboards.py
Corre en GitHub Actions:
1. Descarga archivos desde Box (productividad, base_general, estrategia_asesores)
2. Lee HistChat WA e IVR desde el repo
3. Calcula KPIs
4. Actualiza index.html y canales/index.html
"""
import os, json, glob, base64, logging
from datetime import datetime
from pathlib import Path
import pandas as pd
import numpy as np
from boxsdk import OAuth2, Client

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ─── CREDENCIALES BOX ────────────────────────────────────────────────────────
BOX_CLIENT_ID     = os.environ.get("BOX_CLIENT_ID",     "smna0t2n580ncwpt47ip0hwon5uxf0d9")
BOX_CLIENT_SECRET = os.environ.get("BOX_CLIENT_SECRET", "rJJlVosTDHZsDifhy59XiTbr9SUlrxBj")
BOX_ACCESS_TOKEN  = os.environ.get("BOX_ACCESS_TOKEN",  "")
BOX_REFRESH_TOKEN = os.environ.get("BOX_REFRESH_TOKEN", "")

# IDs carpetas Box
BOX_CALL_ID       = "341560261376"   # Queries Diarios → Call
BOX_BASEGEN_ID    = "351865487011"   # Queries Diarios → Base General
BOX_ARCHCOMP_ID   = "363134860327"   # Archivos Complementarios

# Archivos locales (en el repo)
HISTCHAT_PATTERN  = "data/wa/HistChat*.xls"
IVR_PATTERN       = "data/ivr/*.xl*"

MESES_ORDER = ['Enero','Febrero','Marzo','Abril','Mayo','Junio',
               'Julio','Agosto','Septiembre','Octubre','Noviembre','Diciembre']

# ─── CONEXIÓN BOX ─────────────────────────────────────────────────────────────
def get_box_client():
    tokens = {"access": BOX_ACCESS_TOKEN, "refresh": BOX_REFRESH_TOKEN}

    def store_tokens(access_token, refresh_token):
        tokens["access"]  = access_token
        tokens["refresh"] = refresh_token
        # Actualizar env vars para persistencia dentro del job
        os.environ["BOX_ACCESS_TOKEN"]  = access_token
        os.environ["BOX_REFRESH_TOKEN"] = refresh_token

    auth = OAuth2(
        client_id=BOX_CLIENT_ID,
        client_secret=BOX_CLIENT_SECRET,
        access_token=tokens["access"],
        refresh_token=tokens["refresh"],
        store_tokens=store_tokens,
    )
    return Client(auth)

def get_latest_subfolder(client, folder_id):
    """Obtiene la subcarpeta más reciente (YYYYMMDD) dentro de una carpeta."""
    folder = client.folder(folder_id).get_items(limit=100)
    subs = [(item.name, item.id) for item in folder if item.type == 'folder']
    subs_sorted = sorted(subs, key=lambda x: x[0], reverse=True)
    if not subs_sorted:
        raise ValueError(f"No se encontraron subcarpetas en folder {folder_id}")
    log.info(f"Subcarpeta más reciente: {subs_sorted[0][0]}")
    return subs_sorted[0][1]

def download_file_from_folder(client, folder_id, filename, dest_path):
    """Descarga un archivo específico de una carpeta Box."""
    folder = client.folder(folder_id).get_items(limit=200)
    for item in folder:
        if item.type == 'file' and item.name.lower() == filename.lower():
            log.info(f"Descargando {filename} ({item.id})...")
            content = client.file(item.id).content()
            with open(dest_path, 'wb') as f:
                f.write(content)
            log.info(f"✓ {filename} descargado ({len(content)/1024:.0f} KB)")
            return True
    raise FileNotFoundError(f"No se encontró {filename} en folder {folder_id}")

def download_latest_file(client, parent_folder_id, filename, dest_path):
    """Busca en la subcarpeta más reciente y descarga el archivo."""
    latest_id = get_latest_subfolder(client, parent_folder_id)
    return download_file_from_folder(client, latest_id, filename, dest_path)

def download_estrategia_asesores(client, dest_path):
    """Descarga estrategia_asesores.xlsx de Archivos Complementarios."""
    folder = client.folder(BOX_ARCHCOMP_ID).get_items(limit=200)
    for item in folder:
        if item.type == 'file' and 'estrategia_asesores' in item.name.lower():
            content = client.file(item.id).content()
            with open(dest_path, 'wb') as f:
                f.write(content)
            log.info(f"✓ estrategia_asesores descargado")
            return True
    raise FileNotFoundError("No se encontró estrategia_asesores en Archivos Complementarios")

# ─── PROCESAMIENTO CANAL VOZ ──────────────────────────────────────────────────
def procesar_canal_voz(prod_path, base_path, ivr_files, estrategia_path):
    log.info("Procesando Canal Voz...")

    prod = pd.read_excel(prod_path)
    bg   = pd.read_excel(base_path)

    # Filtrar solo predictivo
    prod_pred = prod[prod['tipo_contacto'] == 'Predictivo'].copy()
    prod_pred['fecha_gestion'] = pd.to_datetime(prod_pred['fecha_gestion'], errors='coerce')
    prod_pred['mes'] = prod_pred['fecha_gestion'].dt.month
    prod_pred['hora'] = prod_pred['fecha_gestion'].dt.hour
    prod_pred['dow']  = prod_pred['fecha_gestion'].dt.dayofweek

    total_ans    = len(prod_pred)
    total_gest   = len(prod_pred[prod_pred['indicador_efectivad']=='Si'])
    cod_ef_cod   = ['Contacto Exitoso','Acuerdo de Pago','Acuerdo con Descuento','Acuerdo Parcial','Pago Recordado']
    efectivos    = prod_pred[prod_pred['resultado_gestion'].isin(cod_ef_cod)]
    total_ef     = len(efectivos)
    total_ac     = len(prod_pred[prod_pred['estado_acuerdo'].notna() & (prod_pred['estado_acuerdo']!='') & (prod_pred['valor_acuerdo']>0)])

    # Por estrategia
    est_map = {
        '1. ESTRATEGIA 0-214':    'mora_temprana_0_214',
        '2. ESTRATEGIA 215-579':  'mora_intermedia_215_a_579',
        '3. ESTRATEGIA 580-1099': 'mora_alta_580_1099',
        '4. ESTRATEGIA >= 1100':  'mora_alta_1100',
    }
    por_est = []
    for marc in ['columna','fila']:
        for est_key, est_val in est_map.items():
            sub = prod_pred[prod_pred['estrategia_credito']==est_key]
            ans = len(sub)
            ges = len(sub[sub['indicador_efectivad']=='Si'])
            ef  = len(sub[sub['resultado_gestion'].isin(cod_ef_cod)])
            ac  = len(sub[sub['valor_acuerdo']>0]) if 'valor_acuerdo' in sub.columns else 0
            por_est.append({
                'est': est_val, 'marc': marc,
                'ans': ans//2, 'ges': ges//2, 'ef': ef//2, 'ac': ac//2
            })

    # Hora óptima
    hora_tot = prod_pred.groupby('hora').size().reset_index(name='tot')
    hora_ef2  = efectivos.groupby('hora').size().reset_index(name='ef')
    hora_m   = hora_tot.merge(hora_ef2, on='hora', how='left').fillna(0)
    hora_m['tasa'] = (hora_m['ef']/hora_m['tot']*100).round(2)
    hora_optima = [{'h':int(r['hora']),'r':float(r['tasa'])}
                   for _,r in hora_m.iterrows() if 7<=int(r['hora'])<=19]

    # Día óptimo
    dias = {0:'Lun',1:'Mar',2:'Mié',3:'Jue',4:'Vie',5:'Sáb'}
    dia_tot = prod_pred.groupby('dow').size().reset_index(name='tot')
    dia_ef2  = efectivos.groupby('dow').size().reset_index(name='ef')
    dia_m   = dia_tot.merge(dia_ef2, on='dow', how='left').fillna(0)
    dia_m['tasa'] = (dia_m['ef']/dia_m['tot']*100).round(2)
    dia_optimo = [{'n':dias.get(int(r['dow']),'?'),'r':float(r['tasa'])}
                  for _,r in dia_m.iterrows() if int(r['dow']) in dias]

    # IVR
    ivr_data = procesar_ivr(ivr_files)

    return {
        'total_ans': total_ans,
        'total_gest': total_gest,
        'total_ef': total_ef,
        'total_ac': total_ac,
        'por_est': por_est,
        'hora_optima': hora_optima,
        'dia_optimo': dia_optimo,
        'ivr': ivr_data,
    }

def procesar_ivr(ivr_files):
    """Procesa archivos IVR y devuelve KPIs por mes."""
    av_mes, inb_mes = [], []
    excluir_rp = ['Agente_Virtual_Galgo','PRUEBA_TI','FlujoEspera_Galgo','Soporte Walter Bridge']

    mes_map = {'01':'Enero','02':'Febrero','03':'Marzo','04':'Abril',
               '05':'Mayo','06':'Junio','07':'Julio','08':'Agosto',
               '09':'Septiembre','10':'Octubre','11':'Noviembre','12':'Diciembre'}

    for f in sorted(ivr_files):
        try:
            fname = Path(f).stem.lower()
            # Detectar mes por nombre de archivo
            mes = 'Unknown'
            for k,v in mes_map.items():
                if v.lower() in fname or f'_{k}' in fname or f'-{k}' in fname:
                    mes = v; break

            try:
                df = pd.read_html(f)[0]
            except:
                df = pd.read_excel(f, header=7)

            df = df[~df['RP_NAME'].isin(excluir_rp)] if 'RP_NAME' in df.columns else df

            av = df[df['RP_NAME'].str.contains('Mnsje|virtual|outbound', case=False, na=False)] if 'RP_NAME' in df.columns else pd.DataFrame()
            inb= df[df['RP_NAME'].str.contains('Moviaval|inbound|entrante', case=False, na=False)] if 'RP_NAME' in df.columns else pd.DataFrame()

            if len(av) > 0:
                ok     = (av['RESULT']=='OK').sum() if 'RESULT' in av.columns else 0
                hungup = (av['RESULT']=='HUNGUP').sum() if 'RESULT' in av.columns else 0
                asesor = av['DN_TRANSFER'].notna().sum() if 'DN_TRANSFER' in av.columns else 0
                av_mes.append({'mes':mes,'tot':len(av),'ok':int(ok),'hu':int(hungup),'as':int(asesor)})

            if len(inb) > 0:
                ok     = (inb['RESULT']=='OK').sum() if 'RESULT' in inb.columns else 0
                hungup = (inb['RESULT']=='HUNGUP').sum() if 'RESULT' in inb.columns else 0
                asesor = inb['DN_TRANSFER'].notna().sum() if 'DN_TRANSFER' in inb.columns else 0
                pct    = round(ok/len(inb)*100,1) if len(inb)>0 else 0
                inb_mes.append({'mes':mes,'tot':len(inb),'ok':int(ok),'hu':int(hungup),'as':int(asesor),'pct':pct})
        except Exception as e:
            log.warning(f"Error procesando IVR {f}: {e}")

    return {'av_mes': av_mes, 'inb_mes': inb_mes}

# ─── PROCESAMIENTO CANAL WHATSAPP ─────────────────────────────────────────────
def procesar_canal_wa(prod_path, base_path, histchat_files, estrategia_path):
    log.info("Procesando Canal WhatsApp...")

    prod = pd.read_excel(prod_path)
    bg   = pd.read_excel(base_path)
    est  = pd.read_excel(estrategia_path)

    tipos_wa = ['Whatsapp Saliente','Whatsapp Entrante','Whatsapp Movil']
    prod_wa = prod[prod['tipo_contacto'].isin(tipos_wa)].copy()

    # Leer HistChat
    dfs = []
    for f in sorted(histchat_files):
        fname = Path(f).stem
        mes = 'Unknown'
        for m in MESES_ORDER:
            if m.lower() in fname.lower():
                mes = m; break
        try:
            df = pd.read_html(f)[0]
            df['mes'] = mes
            df['DATE'] = pd.to_datetime(df['DATE'], errors='coerce')
            dfs.append(df)
        except Exception as e:
            log.warning(f"Error leyendo {f}: {e}")

    if not dfs:
        log.warning("No se encontraron archivos HistChat WA")
        return {}

    wa = pd.concat(dfs, ignore_index=True)
    wa = wa[~wa['COD_ACT'].isin(['963','9999','-'])]
    wa = wa[wa['AGENT_ID'] != 'chat_bot']
    wa = wa[wa['COD_ACT'] != 'TIMEOUTCHAT']
    wa = wa[wa['COD_ACT'] != 'AGENT-INITIATED']
    wa = wa[wa['AGENT_DNI'].astype(str).str.strip().str.upper() != 'BOT']
    wa['DATE'] = pd.to_datetime(wa['DATE'], errors='coerce')
    wa['hora'] = wa['DATE'].dt.hour
    wa['dow']  = wa['DATE'].dt.dayofweek
    wa['usuario'] = wa['AGENT_DNI'].astype(str).str.strip().str.upper()

    cod_ef = ['2000','2001','2002','2003','2005','2007','2012','2052']
    wa['efectivo'] = wa['COD_ACT'].isin(cod_ef)

    # KPIs por mes
    por_mes = []
    for mes in MESES_ORDER:
        dm = wa[wa['mes']==mes]
        if len(dm) == 0: continue
        de = dm[dm['efectivo']]
        da = dm[dm['COD_ACT'].isin(['2000','2001','2002','2003','2052'])]
        por_mes.append({
            'mes': mes,
            'total': int(len(dm)),
            'real': int(len(dm)),
            'ef': int(len(de)),
            'ac': int(len(da)),
            'tasa_ef': round(len(de)/len(dm)*100,1) if len(dm)>0 else 0
        })

    # Hora y día óptimo
    hora_tot = wa.groupby('hora').size().reset_index(name='tot')
    hora_ef2  = wa[wa['efectivo']].groupby('hora').size().reset_index(name='ef')
    hora_m   = hora_tot.merge(hora_ef2, on='hora', how='left').fillna(0)
    hora_m['tasa'] = (hora_m['ef']/hora_m['tot']*100).round(2)
    hora_optima = [{'h':int(r['hora']),'tot':int(r['tot']),'ef':int(r['ef']),'tasa':float(r['tasa'])}
                   for _,r in hora_m.iterrows() if 7<=int(r['hora'])<=19]

    dias = {0:'Lun',1:'Mar',2:'Mié',3:'Jue',4:'Vie',5:'Sáb'}
    dia_tot = wa.groupby('dow').size().reset_index(name='tot')
    dia_ef2  = wa[wa['efectivo']].groupby('dow').size().reset_index(name='ef')
    dia_m   = dia_tot.merge(dia_ef2, on='dow', how='left').fillna(0)
    dia_m['tasa'] = (dia_m['ef']/dia_m['tot']*100).round(2)
    dia_optimo = [{'n':dias.get(int(r['dow']),'?'),'tot':int(r['tot']),'ef':int(r['ef']),'tasa':float(r['tasa'])}
                  for _,r in dia_m.iterrows() if int(r['dow']) in dias]

    # Agentes con estrategia real desde estrategia_asesores
    # estrategia_asesores tiene managing_user_code → estrategia
    est_cols = [c for c in est.columns if 'user' in c.lower() or 'code' in c.lower() or 'estrategia' in c.lower() or 'strategy' in c.lower()]
    log.info(f"Columnas estrategia_asesores: {list(est.columns)}")

    ag_total = wa.groupby('usuario').agg(
        nombre=('AGENT_NAME', lambda x: x.mode()[0] if len(x)>0 else ''),
        total=('CONN_ID','count'),
        efectivos=('efectivo','sum')
    ).reset_index()
    ag_total['tasa_ef'] = (ag_total['efectivos']/ag_total['total']*100).round(1)
    ag_total = ag_total[ag_total['total']>=5].sort_values('total',ascending=False)

    # Merge con estrategia_asesores
    # Normalizar usuario para el merge
    if 'managing_user_code' in est.columns:
        est['usuario'] = est['managing_user_code'].astype(str).str.strip().str.upper()
        est_col_est = [c for c in est.columns if 'estrategia' in c.lower() or 'strategy' in c.lower()]
        if est_col_est:
            ag_con_est = ag_total.merge(est[['usuario', est_col_est[0]]], on='usuario', how='left')
            ag_con_est = ag_con_est.rename(columns={est_col_est[0]: 'estrategia_real'})
        else:
            ag_con_est = ag_total.copy()
            ag_con_est['estrategia_real'] = 'Sin asignar'
    else:
        ag_con_est = ag_total.copy()
        ag_con_est['estrategia_real'] = 'Sin asignar'

    agentes = []
    for _,r in ag_con_est.iterrows():
        agentes.append({
            'usuario': r['usuario'],
            'nombre': str(r['nombre']),
            'total': int(r['total']),
            'efectivos': int(r['efectivos']),
            'tasa_ef': float(r['tasa_ef']),
            'estrategia_real': str(r.get('estrategia_real','Sin asignar'))
        })

    # Tipificaciones
    tip = wa.groupby('COD_ACT').agg(
        desc=('DESCRIPTION_COD_ACT', lambda x: x.mode()[0] if len(x)>0 else ''),
        n=('CONN_ID','count')
    ).reset_index().sort_values('n',ascending=False).head(12)
    tipificaciones = [{'cod':r['COD_ACT'],'desc':r['desc'],'n':int(r['n'])} for _,r in tip.iterrows()]

    total_wa = len(wa)
    total_ef = int(wa['efectivo'].sum())
    total_ac = int((wa['COD_ACT'].isin(['2000','2001','2002','2003','2052'])).sum())

    return {
        'kpis': {
            'total': total_wa,
            'real': total_wa,
            'ef': total_ef,
            'ac': total_ac,
            'timeout': int((wa.get('COD_ACT','')=='TIMEOUTCHAT').sum()) if 'COD_ACT' in wa.columns else 0,
            'tasa_ef': round(total_ef/total_wa*100,1) if total_wa>0 else 0,
        },
        'por_mes': por_mes,
        'hora_optima': hora_optima,
        'dia_optimo': dia_optimo,
        'agentes': agentes,
        'tipificaciones': tipificaciones,
    }

# ─── MAIN ─────────────────────────────────────────────────────────────────────
def main():
    log.info("=== INICIANDO PROCESAMIENTO DE DASHBOARDS ===")
    Path("data/tmp").mkdir(parents=True, exist_ok=True)

    # 1. Conectar a Box y descargar archivos
    log.info("Conectando a Box...")
    client = get_box_client()

    prod_path = "data/tmp/productividad.xlsx"
    base_path = "data/tmp/base_general.xlsx"
    est_path  = "data/tmp/estrategia_asesores.xlsx"

    log.info("Descargando productividad.xlsx...")
    download_latest_file(client, BOX_CALL_ID, "productividad.xlsx", prod_path)

    log.info("Descargando base_general.xlsx...")
    download_latest_file(client, BOX_BASEGEN_ID, "base_general.xlsx", base_path)

    log.info("Descargando estrategia_asesores.xlsx...")
    download_estrategia_asesores(client, est_path)

    # 2. Encontrar archivos locales (del repo)
    histchat_files = sorted(glob.glob(HISTCHAT_PATTERN))
    ivr_files      = sorted(glob.glob(IVR_PATTERN))
    log.info(f"HistChat WA: {len(histchat_files)} archivos")
    log.info(f"IVR: {len(ivr_files)} archivos")

    # 3. Procesar
    datos_voz = procesar_canal_voz(prod_path, base_path, ivr_files, est_path)
    datos_wa  = procesar_canal_wa(prod_path, base_path, histchat_files, est_path)

    # 4. Guardar JSON de datos
    ahora = datetime.now().strftime("%d/%m/%Y %H:%M")
    datos = {
        'actualizado': ahora,
        'canal_voz': datos_voz,
        'canal_wa':  datos_wa,
    }
    with open("data/datos_dashboards.json", "w", encoding="utf-8") as f:
        json.dump(datos, f, ensure_ascii=False, default=str)
    log.info("✓ datos_dashboards.json guardado")

    # 5. Actualizar HTMLs con los nuevos datos
    actualizar_html(datos, ahora)
    log.info("=== PROCESAMIENTO COMPLETADO ===")

def actualizar_html(datos, ahora):
    """Inyecta los datos actualizados en los HTMLs."""
    # Leer HTMLs actuales
    with open("index.html", "r", encoding="utf-8") as f:
        pred_html = f.read()
    with open("canales/index.html", "r", encoding="utf-8") as f:
        canales_html = f.read()

    # Actualizar timestamp en canales
    import re
    canales_html = re.sub(
        r'Actualizado:.*?(?=<)',
        f'Actualizado: {ahora}',
        canales_html
    )

    # Inyectar datos WA actualizados como JSON en el script
    wa = datos.get('canal_wa', {})
    if wa:
        por_mes_js = json.dumps(wa.get('por_mes', []), ensure_ascii=False)
        hora_js    = json.dumps(wa.get('hora_optima', []), ensure_ascii=False)
        dia_js     = json.dumps(wa.get('dia_optimo', []), ensure_ascii=False)
        tip_js     = json.dumps(wa.get('tipificaciones', []), ensure_ascii=False)
        ag_js      = json.dumps(wa.get('agentes', []), ensure_ascii=False)
        kpis_js    = json.dumps(wa.get('kpis', {}), ensure_ascii=False)

        # Reemplazar el bloque de datos WA en el HTML
        canales_html = re.sub(
            r'// __WA_DATOS_START__.*?// __WA_DATOS_END__',
            f'''// __WA_DATOS_START__
const WA_LIVE = {{
  kpis: {kpis_js},
  por_mes: {por_mes_js},
  hora: {hora_js},
  dia: {dia_js},
  tipificaciones: {tip_js},
  agentes: {ag_js},
}};
// __WA_DATOS_END__''',
            canales_html,
            flags=re.DOTALL
        )

    with open("canales/index.html", "w", encoding="utf-8") as f:
        f.write(canales_html)
    log.info("✓ canales/index.html actualizado")

    with open("index.html", "w", encoding="utf-8") as f:
        f.write(pred_html)
    log.info("✓ index.html actualizado")

if __name__ == "__main__":
    main()
