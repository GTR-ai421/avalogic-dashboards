"""
actualizar_canales.py
Lee archivos WA e IVR desde la carpeta WA-IVR, calcula KPIs,
actualiza canales/index.html y sube todo a GitHub Pages.
"""
import os, sys, json, glob, hashlib, base64, re
import urllib.request, urllib.error
from pathlib import Path
from datetime import datetime

# ─── CONFIGURACIÓN ───────────────────────────────────────────────────────────
CARPETA_WA_IVR = Path(__file__).parent / "WA-IVR"
HASH_FILE      = Path(__file__).parent / "wa_ivr_hash.json"
HTML_CANALES   = Path(__file__).parent / "canales" / "index.html"

GH_TOKEN = ""  # Se carga desde gh_config.json (no se sube al repo)
GH_USER  = "GTR-ai421"
GH_REPO  = "avalogic-dashboards"

def cargar_config():
    """Carga el token de GitHub desde archivo local (no versionado)."""
    global GH_TOKEN
    config_path = Path(__file__).parent / "gh_config.json"
    if config_path.exists():
        with open(config_path) as f:
            GH_TOKEN = json.load(f).get("token", "")
    if not GH_TOKEN:
        print("ERROR: No se encontró gh_config.json con el token de GitHub.")
        print("Crear el archivo gh_config.json en la carpeta con: {\"token\": \"TU_TOKEN\"}")
        sys.exit(1)

MESES_ORDER = ['Enero','Febrero','Marzo','Abril','Mayo','Junio',
               'Julio','Agosto','Septiembre','Octubre','Noviembre','Diciembre']
MES_NUM = {1:'Enero',2:'Febrero',3:'Marzo',4:'Abril',5:'Mayo',6:'Junio',
           7:'Julio',8:'Agosto',9:'Septiembre',10:'Octubre',11:'Noviembre',12:'Diciembre'}

# ─── HASH para detectar cambios ──────────────────────────────────────────────
def calcular_hash_carpeta():
    hashes = []
    for f in sorted(CARPETA_WA_IVR.glob("*")):
        if f.is_file():
            hashes.append(f"{f.name}:{f.stat().st_size}:{f.stat().st_mtime}")
    return hashlib.md5("\n".join(hashes).encode()).hexdigest()

def hay_cambios():
    hash_actual = calcular_hash_carpeta()
    if HASH_FILE.exists():
        with open(HASH_FILE) as f:
            hash_guardado = json.load(f).get("hash", "")
        if hash_actual == hash_guardado:
            return False, hash_actual
    return True, hash_actual

def guardar_hash(h):
    with open(HASH_FILE, "w") as f:
        json.dump({"hash": h, "actualizado": datetime.now().strftime("%d/%m/%Y %H:%M")}, f)

# ─── DETECTAR MES desde contenido del archivo ────────────────────────────────
def detectar_mes_wa(df):
    """Detecta el mes predominante en un DataFrame de HistChat."""
    try:
        import pandas as pd
        fechas = pd.to_datetime(df['DATE'], errors='coerce').dropna()
        if len(fechas) == 0:
            return 'Unknown'
        mes_num = fechas.dt.month.mode()[0]
        return MES_NUM.get(int(mes_num), 'Unknown')
    except:
        return 'Unknown'

def detectar_mes_ivr(nombre):
    """Detecta el mes por el nombre del archivo IVR."""
    nombre_lower = nombre.lower()
    for mes in MESES_ORDER:
        if mes.lower() in nombre_lower:
            return mes
    # Por número en el nombre
    for num, mes in MES_NUM.items():
        if f"_{num:02d}" in nombre or f"_{num}_" in nombre:
            return mes
    return 'Unknown'

# ─── LEER ARCHIVOS WA ────────────────────────────────────────────────────────
def leer_histchat_files():
    import pandas as pd
    archivos = list(CARPETA_WA_IVR.glob("HistChat*.xls")) + \
               list(CARPETA_WA_IVR.glob("HistChat*.xlsx"))
    dfs = []
    for f in sorted(archivos):
        try:
            try:
                df = pd.read_html(str(f))[0]
            except:
                df = pd.read_excel(str(f))
            mes = detectar_mes_wa(df)
            df['mes'] = mes
            df['DATE'] = pd.to_datetime(df['DATE'], errors='coerce')
            dfs.append(df)
            print(f"  ✓ {f.name} → {mes} ({len(df):,} registros)")
        except Exception as e:
            print(f"  ✗ {f.name}: {e}")
    return dfs

# ─── LEER ARCHIVOS IVR ───────────────────────────────────────────────────────
def leer_ivr_files():
    import pandas as pd
    archivos = list(CARPETA_WA_IVR.glob("ivr*.xls")) + \
               list(CARPETA_WA_IVR.glob("ivr*.xlsx"))
    resultados = []
    excluir = ['Agente_Virtual_Galgo','PRUEBA_TI','FlujoEspera_Galgo','Soporte Walter Bridge']

    for f in sorted(archivos):
        mes = detectar_mes_ivr(f.stem)
        try:
            try:
                df = pd.read_html(str(f))[0]
            except:
                df = pd.read_excel(str(f), header=7)

            if 'RP_NAME' in df.columns:
                df = df[~df['RP_NAME'].astype(str).isin(excluir)]

            resultados.append({'mes': mes, 'df': df, 'nombre': f.name})
            print(f"  ✓ {f.name} → {mes} ({len(df):,} registros)")
        except Exception as e:
            print(f"  ✗ {f.name}: {e}")
    return resultados

# ─── CALCULAR KPIs WA ────────────────────────────────────────────────────────
def calcular_kpis_wa(dfs):
    import pandas as pd
    if not dfs:
        return {}

    wa = pd.concat(dfs, ignore_index=True)
    wa = wa[~wa['COD_ACT'].astype(str).isin(['963','9999','-'])]
    wa = wa[wa['AGENT_ID'].astype(str) != 'chat_bot']
    wa = wa[wa['COD_ACT'].astype(str) != 'TIMEOUTCHAT']
    wa = wa[wa['COD_ACT'].astype(str) != 'AGENT-INITIATED']
    wa = wa[wa['AGENT_DNI'].astype(str).str.strip().str.upper() != 'BOT']
    wa['DATE'] = pd.to_datetime(wa['DATE'], errors='coerce')
    wa['hora'] = wa['DATE'].dt.hour
    wa['dow']  = wa['DATE'].dt.dayofweek
    wa['usuario'] = wa['AGENT_DNI'].astype(str).str.strip().str.upper()

    cod_ef = ['2000','2001','2002','2003','2005','2007','2012','2052']
    cod_ac = ['2000','2001','2002','2003','2052']
    wa['efectivo'] = wa['COD_ACT'].astype(str).isin(cod_ef)
    wa['acuerdo']  = wa['COD_ACT'].astype(str).isin(cod_ac)

    # Por mes
    por_mes = []
    for mes in MESES_ORDER:
        dm = wa[wa['mes']==mes]
        if len(dm) == 0: continue
        de = dm[dm['efectivo']]
        da = dm[dm['acuerdo']]
        por_mes.append({
            'mes': mes,
            'total': int(len(dm)),
            'real': int(len(dm)),
            'ef': int(len(de)),
            'ac': int(len(da)),
            'tasa_ef': round(len(de)/len(dm)*100,1) if len(dm)>0 else 0
        })

    # Hora óptima
    h_tot = wa.groupby('hora').size().reset_index(name='tot')
    h_ef  = wa[wa['efectivo']].groupby('hora').size().reset_index(name='ef')
    h_m   = h_tot.merge(h_ef, on='hora', how='left').fillna(0)
    h_m['tasa'] = (h_m['ef']/h_m['tot']*100).round(2)
    hora_optima = [{'h':int(r['hora']),'tot':int(r['tot']),'ef':int(r['ef']),'tasa':float(r['tasa'])}
                   for _,r in h_m.iterrows() if 7<=int(r['hora'])<=19]

    # Día óptimo
    dias = {0:'Lun',1:'Mar',2:'Mié',3:'Jue',4:'Vie',5:'Sáb'}
    d_tot = wa.groupby('dow').size().reset_index(name='tot')
    d_ef  = wa[wa['efectivo']].groupby('dow').size().reset_index(name='ef')
    d_m   = d_tot.merge(d_ef, on='dow', how='left').fillna(0)
    d_m['tasa'] = (d_m['ef']/d_m['tot']*100).round(2)
    dia_optimo = [{'n':dias.get(int(r['dow']),'?'),'tot':int(r['tot']),'ef':int(r['ef']),'tasa':float(r['tasa'])}
                  for _,r in d_m.iterrows() if int(r['dow']) in dias]

    # Tipificaciones
    tip = wa.groupby('COD_ACT').agg(
        desc=('DESCRIPTION_COD_ACT', lambda x: x.mode()[0] if len(x)>0 else ''),
        n=('CONN_ID','count')
    ).reset_index().sort_values('n',ascending=False).head(12)
    tipificaciones = [{'cod':str(r['COD_ACT']),'desc':str(r['desc']),'n':int(r['n'])}
                      for _,r in tip.iterrows()]

    # Agentes + merge con estrategia_asesores
    ag = wa.groupby('usuario').agg(
        nombre=('AGENT_NAME', lambda x: x.mode()[0] if len(x)>0 else ''),
        total=('CONN_ID','count'),
        efectivos=('efectivo','sum')
    ).reset_index()
    ag['tasa_ef'] = (ag['efectivos']/ag['total']*100).round(1)
    ag = ag[ag['total']>=5].sort_values('total',ascending=False)

    # Buscar estrategia_asesores.xlsx
    est_paths = [
        CARPETA_WA_IVR.parent / "estrategia_asesores.xlsx",
        CARPETA_WA_IVR.parent / "data" / "tmp" / "estrategia_asesores.xlsx",
    ]
    for est_path in est_paths:
        if est_path.exists():
            try:
                est = pd.read_excel(str(est_path))
                if 'managing_user_code' in est.columns and 'strategy' in est.columns:
                    est['usuario'] = est['managing_user_code'].astype(str).str.strip().str.upper()
                    est_merge = est[['usuario','strategy']].drop_duplicates('usuario')
                    ag = ag.merge(est_merge, on='usuario', how='left')
                    ag['estrategia_real'] = ag['strategy'].fillna('Sin asignar')
                    ag = ag.drop(columns=['strategy'], errors='ignore')
                    print(f"  ✓ Estrategia_asesores cargado: {est_path.name}")
                    break
            except Exception as e:
                print(f"  ✗ Error leyendo estrategia_asesores: {e}")
    else:
        ag['estrategia_real'] = 'Sin asignar'

    agentes = [{'usuario':r['usuario'],'nombre':str(r['nombre']),
                'total':int(r['total']),'efectivos':int(r['efectivos']),
                'tasa_ef':float(r['tasa_ef']),
                'estrategia_real':str(r.get('estrategia_real','Sin asignar'))}
               for _,r in ag.iterrows()]

    total_wa = int(len(wa))
    total_ef = int(wa['efectivo'].sum())
    total_ac = int(wa['acuerdo'].sum())
    total_to = int((wa['COD_ACT'].astype(str)=='TIMEOUTCHAT').sum())

    return {
        'kpis': {
            'total': total_wa, 'real': total_wa,
            'ef': total_ef, 'ac': total_ac,
            'timeout': total_to,
            'tasa_ef': round(total_ef/total_wa*100,1) if total_wa>0 else 0,
        },
        'por_mes': por_mes,
        'hora': hora_optima,
        'dia': dia_optimo,
        'tipificaciones': tipificaciones,
        'agentes': agentes,
    }

# ─── CALCULAR KPIs IVR ───────────────────────────────────────────────────────
def calcular_kpis_ivr(ivr_data):
    av_mes, inb_mes = [], []
    for item in ivr_data:
        df  = item['df']
        mes = item['mes']
        try:
            if 'RP_NAME' not in df.columns:
                continue
            av  = df[df['RP_NAME'].astype(str).str.contains('Mnsje|virtual|outbound|agente', case=False, na=False)]
            inb = df[df['RP_NAME'].astype(str).str.contains('Moviaval|inbound|entrante|IVR_M', case=False, na=False)]
            if 'RESULT' not in df.columns:
                continue
            if len(av) > 0:
                ok     = int((av['RESULT']=='OK').sum())
                hungup = int((av['RESULT']=='HUNGUP').sum())
                asesor = int(av['DN_TRANSFER'].astype(str).str.strip().ne('-').sum()) if 'DN_TRANSFER' in av.columns else 0
                av_mes.append({'mes':mes,'tot':len(av),'ok':ok,'hu':hungup,'as':asesor})
            if len(inb) > 0:
                ok     = int((inb['RESULT']=='OK').sum())
                hungup = int((inb['RESULT']=='HUNGUP').sum())
                asesor = int(inb['DN_TRANSFER'].astype(str).str.strip().ne('-').sum()) if 'DN_TRANSFER' in inb.columns else 0
                pct    = round(ok/len(inb)*100,1) if len(inb)>0 else 0
                inb_mes.append({'mes':mes,'tot':int(len(inb)),'ok':ok,'hu':hungup,'as':asesor,'pct':pct})
        except Exception as e:
            print(f"  ✗ IVR {mes}: {e}")
    return {'av_mes': av_mes, 'inb_mes': inb_mes}

# ─── ACTUALIZAR HTML ─────────────────────────────────────────────────────────
def actualizar_html(datos_wa, datos_ivr, ahora):
    if not HTML_CANALES.exists():
        print(f"  ✗ No se encontró {HTML_CANALES}")
        return False

    with open(HTML_CANALES, 'r', encoding='utf-8') as f:
        html = f.read()

    # Actualizar timestamp
    html = re.sub(r'Actualizado:.*?(?=<)', f'Actualizado: {ahora}', html)

    # Inyectar datos WA — reemplazar los arrays de datos en el JS
    if datos_wa:
        # por_mes
        html = re.sub(
            r'por_mes:\[.*?\],\s*hora:\[',
            f"por_mes:{json.dumps(datos_wa.get('por_mes',[]), ensure_ascii=False)},\n  hora:[",
            html, flags=re.DOTALL
        )
        # hora
        html = re.sub(
            r'hora:\[.*?\],\s*dia:\[',
            f"hora:{json.dumps(datos_wa.get('hora',[]), ensure_ascii=False)},\n  dia:[",
            html, flags=re.DOTALL
        )
        # dia
        html = re.sub(
            r'dia:\[.*?\],\s*tipificaciones:\[',
            f"dia:{json.dumps(datos_wa.get('dia',[]), ensure_ascii=False)},\n  tipificaciones:[",
            html, flags=re.DOTALL
        )

    # IVR - actualizar av_mes e inb_mes
    if datos_ivr:
        if datos_ivr.get('av_mes'):
            av_js = json.dumps(datos_ivr['av_mes'], ensure_ascii=False)
            html = re.sub(r'av_mes:\[.*?\]', f'av_mes:{av_js}', html, flags=re.DOTALL)
        if datos_ivr.get('inb_mes'):
            inb_js = json.dumps(datos_ivr['inb_mes'], ensure_ascii=False)
            html = re.sub(r'inb_mes:\[.*?\]', f'inb_mes:{inb_js}', html, flags=re.DOTALL)

    with open(HTML_CANALES, 'w', encoding='utf-8') as f:
        f.write(html)
    print(f"  ✓ HTML actualizado ({len(html)//1024} KB)")
    return True

# ─── SUBIR A GITHUB ──────────────────────────────────────────────────────────
def subir_github(ruta_local, ruta_repo, mensaje):
    api = f'https://api.github.com/repos/{GH_USER}/{GH_REPO}/contents/{ruta_repo}'
    headers = {
        'Authorization': f'token {GH_TOKEN}',
        'Content-Type': 'application/json',
        'Accept': 'application/vnd.github.v3+json',
    }
    with open(ruta_local, 'rb') as f:
        contenido_b64 = base64.b64encode(f.read()).decode()

    # Obtener SHA actual
    req_get = urllib.request.Request(api, headers=headers)
    try:
        with urllib.request.urlopen(req_get, timeout=30) as r:
            sha = json.loads(r.read())['sha']
    except:
        sha = None

    payload = {'message': mensaje, 'content': contenido_b64, 'branch': 'main'}
    if sha:
        payload['sha'] = sha

    data = json.dumps(payload).encode()
    req_put = urllib.request.Request(api, data=data, method='PUT', headers=headers)
    try:
        with urllib.request.urlopen(req_put, timeout=60) as r:
            json.loads(r.read())
        return True
    except urllib.error.HTTPError as e:
        err = e.read().decode()[:200]
        # Si el error es "too large" usar Git API
        if 'too large' in err.lower() or e.code == 422:
            print(f"    Archivo grande, omitiendo respaldo de {ruta_repo}")
            return False
        print(f"    Error GitHub {e.code}: {err}")
        return False

# ─── MAIN ─────────────────────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print("  ACTUALIZADOR CANAL VOZ / WHATSAPP")
    print("=" * 60)

    # Cargar configuración
    cargar_config()
    cambios, hash_actual = hay_cambios()
    if not cambios:
        print("\nSin cambios en WA-IVR — dashboard ya está actualizado.")
        return 'sin_cambios'

    print(f"\nArchivos nuevos detectados en WA-IVR. Procesando...\n")

    # Importar pandas aquí para dar mejor error si no está instalado
    try:
        import pandas as pd
    except ImportError:
        print("ERROR: pandas no está instalado.")
        print("Instalar con: pip install pandas openpyxl lxml")
        return 'error'

    # Leer archivos
    print("Leyendo archivos WA...")
    dfs_wa = leer_histchat_files()

    print("\nLeyendo archivos IVR...")
    ivr_data = leer_ivr_files()

    # Descargar estrategia_asesores.xlsx desde Box (siempre, para tener la versión más reciente)
    print("Descargando estrategia_asesores.xlsx desde Box...")
    est_local = CARPETA_WA_IVR.parent / "estrategia_asesores.xlsx"
    try:
        from boxsdk import OAuth2, Client as BoxClient

        box_config = CARPETA_WA_IVR.parent / "box_config.json"
        if box_config.exists():
            with open(box_config) as f:
                bc = json.load(f)
            BOX_CLIENT_ID     = bc.get("client_id",     "smna0t2n580ncwpt47ip0hwon5uxf0d9")
            BOX_CLIENT_SECRET = bc.get("client_secret", "rJJlVosTDHZsDifhy59XiTbr9SUlrxBj")
            BOX_ACCESS_TOKEN  = bc.get("access_token",  "")
            BOX_REFRESH_TOKEN = bc.get("refresh_token", "")
        else:
            # Leer tokens desde el token.json del VPS/local
            token_path = Path(r"C:\box_automation\token.json")
            if not token_path.exists():
                token_path = CARPETA_WA_IVR.parent / "box_token.json"
            with open(token_path) as f:
                t = json.load(f)
            BOX_CLIENT_ID     = "smna0t2n580ncwpt47ip0hwon5uxf0d9"
            BOX_CLIENT_SECRET = "rJJlVosTDHZsDifhy59XiTbr9SUlrxBj"
            BOX_ACCESS_TOKEN  = t.get("access_token", "")
            BOX_REFRESH_TOKEN = t.get("refresh_token", "")

        tokens_box = {"access": BOX_ACCESS_TOKEN, "refresh": BOX_REFRESH_TOKEN}
        token_path_save = Path(r"C:\box_automation\token.json")

        def store_tokens(at, rt):
            tokens_box["access"] = at
            tokens_box["refresh"] = rt
            try:
                with open(token_path_save, "w") as f:
                    json.dump({"access_token": at, "refresh_token": rt}, f)
            except: pass

        auth = OAuth2(client_id=BOX_CLIENT_ID, client_secret=BOX_CLIENT_SECRET,
                      access_token=tokens_box["access"], refresh_token=tokens_box["refresh"],
                      store_tokens=store_tokens)
        box = BoxClient(auth)

        # Buscar estrategia_asesores en Archivos Complementarios (ID 363134860327)
        items = box.folder("363134860327").get_items(limit=200)
        for item in items:
            if item.type == 'file' and 'estrategia_asesores' in item.name.lower():
                content = box.file(item.id).content()
                with open(est_local, 'wb') as f:
                    f.write(content)
                print(f"  ✓ estrategia_asesores.xlsx descargado de Box ({len(content)//1024} KB)")
                break
        else:
            print("  ✗ No se encontró estrategia_asesores en Box — usando versión local si existe")
    except Exception as e:
        print(f"  ✗ Error conectando a Box: {e}")
        print("    Usando versión local si existe...")

    # Calcular KPIs
    print("\nCalculando KPIs WA...")
    datos_wa = calcular_kpis_wa(dfs_wa) if dfs_wa else {}

    print("Calculando KPIs IVR...")
    datos_ivr = calcular_kpis_ivr(ivr_data) if ivr_data else {}

    # Actualizar HTML
    ahora = datetime.now().strftime("%d/%m/%Y %H:%M")
    print(f"\nActualizando dashboard...")
    ok = actualizar_html(datos_wa, datos_ivr, ahora)
    if not ok:
        print("ERROR: No se pudo actualizar el HTML")
        return 'error'

    # Subir HTML a GitHub
    print("Subiendo canales/index.html a GitHub Pages...")
    if subir_github(HTML_CANALES, 'canales/index.html',
                    f'Actualizar canal voz/WA: {ahora}'):
        print(f"  ✓ Dashboard publicado: https://gtr-ai421.github.io/avalogic-dashboards/canales/")
    else:
        print("  ✗ Error subiendo HTML")
        return 'error'

    # Subir archivos WA como respaldo (solo los nuevos)
    print("\nSubiendo archivos nuevos al repo como respaldo...")
    archivos_repo = []
    for f in sorted(CARPETA_WA_IVR.glob("HistChat*.xls*")):
        ruta_repo = f'data/wa/{f.name}'
        print(f"  Subiendo {f.name}...")
        subir_github(f, ruta_repo, f'Respaldo WA: {f.name}')

    for f in sorted(CARPETA_WA_IVR.glob("ivr*.xl*")):
        ruta_repo = f'data/ivr/{f.name}'
        print(f"  Subiendo {f.name}...")
        subir_github(f, ruta_repo, f'Respaldo IVR: {f.name}')

    # Guardar hash
    guardar_hash(hash_actual)

    print("\n" + "=" * 60)
    print(f"  ✓ LISTO — Dashboard actualizado: {ahora}")
    print(f"  URL: https://gtr-ai421.github.io/avalogic-dashboards/canales/")
    print("=" * 60)
    return 'ok'

if __name__ == '__main__':
    result = main()
    print(f"\nResultado: {result}")
