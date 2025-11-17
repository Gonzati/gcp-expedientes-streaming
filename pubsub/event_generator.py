import os
import json
import random
import datetime
from typing import Optional, Dict, Any, List

from google.cloud import bigquery
from google.cloud import pubsub_v1

# ==========================================================
# CONFIG
# ==========================================================
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/key.json"

PROJECT_ID = "Proyecto"
DATASET = "Expedientes_OLAP"
TABLE = "Expedientes"

PUBSUB_TOPIC_ID = "expedientes-updates"   # topic ya creado antes: gcloud pubsub topics create expedientes-updates

# ==========================================================
# CLIENTES
# ==========================================================
bq_client = bigquery.Client(project=PROJECT_ID)
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, PUBSUB_TOPIC_ID)

# ==========================================================
# CARGA DEL ESTADO ACTUAL DE LOS EXPEDIENTES
# ==========================================================
print("Cargando Ref, Procedimiento, último estado y última cuantía desde BigQuery...")

query = f"""
SELECT
  Ref,
  Procedimiento,
  (
    SELECT AS STRUCT e.estado, e.timestamp
    FROM UNNEST(Estado) e
    ORDER BY e.timestamp DESC
    LIMIT 1
  ) AS last_estado,
  (
    SELECT AS STRUCT c.importe, c.timestamp
    FROM UNNEST(Cuantia) c
    ORDER BY c.timestamp DESC
    LIMIT 1
  ) AS last_cuantia
FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
"""

expedientes: Dict[str, Dict[str, Any]] = {}

for row in bq_client.query(query):
    le = row.last_estado
    lc = row.last_cuantia

    # le y lc pueden ser None o un dict
    last_estado_val = le["estado"] if le is not None else None
    last_cuantia_val = float(lc["importe"]) if lc is not None else None

    expedientes[row.Ref] = {
        "procedimiento": row.Procedimiento,
        "last_estado": last_estado_val,
        "last_cuantia": last_cuantia_val
    }

refs = list(expedientes.keys())
print(f"Expedientes cargados: {len(refs)}")
print("Ejemplo:", list(expedientes.items())[:3])


# ==========================================================
# LÓGICA DE FLUJOS Y PROBABILIDADES
# ==========================================================

# Probabilidades según tu documento
PROB_VISTA_VERBAL = 0.40
PROB_JUICIO_ORDINARIO = 0.20
PROB_RECURSO = 0.20

def siguiente_estado(procedimiento: str, last_estado: Optional[str]) -> Optional[str]:
    """
    Devuelve el siguiente estado posible según:
      - Procedimiento (Verbal/Ordinario)
      - Último estado
      - Probabilidades
    Devuelve None si el expediente debe considerarse terminal (sin nuevo estado).
    """

    # ---------------- VERBAL ----------------
    if procedimiento == "Verbal":
        # Flujo:
        # Demanda -> Contestación -> (Vista 40%) -> Sentencia -> (Recurso 20%) -> Oposición -> Sentencia definitiva

        if last_estado is None:
            return "Demanda"

        if last_estado == "Demanda":
            return "Contestación"

        if last_estado == "Contestación":
            # 40% Vista, 60% Sentencia directa
            if random.random() < PROB_VISTA_VERBAL:
                return "Vista"
            else:
                return "Sentencia"

        if last_estado == "Vista":
            return "Sentencia"

        if last_estado == "Sentencia":
            # 20% Recurso, si no, terminal (no hay nuevo estado)
            if random.random() < PROB_RECURSO:
                return "Recurso"
            else:
                return None

        if last_estado == "Recurso":
            return "Oposición a recurso"

        if last_estado == "Oposición a recurso":
            return "Sentencia definitiva"

        if last_estado == "Sentencia definitiva":
            return None  # expediente totalmente cerrado

        # Cualquier estado raro: no generamos nada
        return None

    # ---------------- ORDINARIO ----------------
    else:
        # Flujo:
        # Demanda -> Contestación -> Audiencia Previa -> (Juicio 20%) -> Sentencia -> (Recurso 20%) -> Oposición -> Sentencia definitiva

        if last_estado is None:
            return "Demanda"

        if last_estado == "Demanda":
            return "Contestación"

        if last_estado == "Contestación":
            return "Audiencia Previa"

        if last_estado == "Audiencia Previa":
            # 20% Juicio, si no, Sentencia directa
            if random.random() < PROB_JUICIO_ORDINARIO:
                return "Juicio"
            else:
                return "Sentencia"

        if last_estado == "Juicio":
            return "Sentencia"

        if last_estado == "Sentencia":
            # 20% Recurso, si no, terminal
            if random.random() < PROB_RECURSO:
                return "Recurso"
            else:
                return None

        if last_estado == "Recurso":
            return "Oposición a recurso"

        if last_estado == "Oposición a recurso":
            return "Sentencia definitiva"

        if last_estado == "Sentencia definitiva":
            return None

        return None


def generar_evento_para_ref(ref: str) -> Optional[Dict[str, Any]]:
    """
    Genera un evento coherente para una Ref concreta:
      - Respeta el flujo de estados del procedimiento
      - Aplica probabilidades
      - Cuantía nunca sube
    Devuelve None si no hay nuevo estado (expediente ya terminal).
    """

    info = expedientes[ref]
    proc = info["procedimiento"]
    last_estado = info["last_estado"]
    last_cuantia = info["last_cuantia"]

    siguiente = siguiente_estado(proc, last_estado)
    if siguiente is None:
        # No hay nuevo estado coherente: expediente cerrado
        return None

    ahora = datetime.datetime.utcnow().isoformat() + "Z"

    # ----- Cuantía -----
    # 40% de probabilidad de que cambie la cuantía
    hay_cambio_cuantia = (last_cuantia is not None) and (random.random() < 0.4)

    cuantia_arr: List[Dict[str, Any]] = []
    if hay_cambio_cuantia:
        # La cuantía solo puede bajar, nunca subir
        if last_cuantia > 0:
            max_bajada = min(500, int(last_cuantia))
            bajada = random.randint(1, max_bajada)
            nueva_cuantia = max(0, last_cuantia - bajada)
        else:
            nueva_cuantia = 0.0

        cuantia_arr.append({
            "importe": float(nueva_cuantia),
            "timestamp": ahora
        })
        # Actualizamos el estado en memoria
        expedientes[ref]["last_cuantia"] = nueva_cuantia

    # ----- Estado -----
    estado_arr = [{
        "estado": siguiente,
        "timestamp": ahora
    }]
    expedientes[ref]["last_estado"] = siguiente

    return {
        "Ref": ref,
        "Cuantia": cuantia_arr,
        "Estado": estado_arr
    }


def generar_eventos(num_mensajes: int, publicar: bool = False) -> List[Dict[str, Any]]:
    """
    Genera num_mensajes eventos coherentes.
      - Si publicar=True, los envía a Pub/Sub.
      - Devuelve la lista de eventos generados.
    """
    eventos: List[Dict[str, Any]] = []
    intentos = 0
    max_intentos = num_mensajes * 10  # para evitar bucles infinitos si muchos están cerrados

    while len(eventos) < num_mensajes and intentos < max_intentos:
        intentos += 1
        ref = random.choice(refs)
        evento = generar_evento_para_ref(ref)
        if evento is None:
            continue  # expediente sin siguiente estado, probamos otro

        eventos.append(evento)

        if publicar:
            data = json.dumps(evento).encode("utf-8")
            future = publisher.publish(
                topic_path,
                data,
                content_type="application/json"
            )
            # Si quieres ver todos, descomenta:
            # print("Publicado:", ref, "msg_id:", future.result())

    print(f"Generados {len(eventos)} eventos coherentes (solicitados {num_mensajes}).")
    if not publicar:
        print("Ejemplo de primer evento:")
        if eventos:
            print(json.dumps(eventos[0], indent=2, ensure_ascii=False))

    return eventos

# ==========================================================
# EJEMPLOS DE USO
# ==========================================================

# Adaptar a lo que queramos generar
ejemplo = generar_eventos(50, publicar=True)


