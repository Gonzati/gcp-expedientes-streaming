import os
import random
import uuid
import datetime
from google.cloud import bigquery

# --------------------------------------------------------------
# CREDENCIALES
# --------------------------------------------------------------
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "key.json"

# --------------------------------------------------------------
# CONFIG BIGQUERY
# --------------------------------------------------------------
PROJECT_ID = "Proyecto"
DATASET = "Expedientes_OLAP"
TABLE = "Expedientes"

client = bigquery.Client(project=PROJECT_ID)

# Verificación de tabla
full_table_id = f"{PROJECT_ID}.{DATASET}.{TABLE}"
table = client.get_table(full_table_id)
print("OK, la tabla existe y tengo permisos de lectura sobre:")
print(table.full_table_id)

# --------------------------------------------------------------
# SECUENCIAS DE ESTADOS
# --------------------------------------------------------------
SECUENCIA_VERBAL = [
    ("Demanda", 1.0),
    ("Contestación", 1.0),
    ("Vista", 0.40),
    ("Sentencia", 1.0),
    ("Recurso", 0.20),
    ("Oposición a recurso", 1.0),
    ("Sentencia definitiva", 1.0)
]

SECUENCIA_ORDINARIO = [
    ("Demanda", 1.0),
    ("Contestación", 1.0),
    ("Audiencia Previa", 1.0),
    ("Juicio", 0.20),
    ("Sentencia", 1.0),
    ("Recurso", 0.20),
    ("Oposición a recurso", 1.0),
    ("Sentencia definitiva", 1.0)
]

# --------------------------------------------------------------
# GENERADORES DE NESTED FIELDS
# --------------------------------------------------------------
def generar_estados(secuencia):
    estados = []
    base_time = datetime.datetime(2024, 1, 1)

    for estado, prob in secuencia:
        if random.random() <= prob:
            base_time += datetime.timedelta(days=random.randint(5, 30))
            estados.append({
                "estado": estado,
                "timestamp": base_time.isoformat()
            })
        else:
            break
    return estados


def generar_cuantias():
    cuantias = []
    base_time = datetime.datetime(2024, 1, 1)

    importe = random.randint(1000, 10000)
    cuantias.append({
        "importe": float(importe),
        "timestamp": base_time.isoformat()
    })

    for _ in range(random.randint(0, 3)):
        base_time += datetime.timedelta(days=random.randint(5, 20))
        importe -= random.randint(50, 500)
        if importe < 0:
            importe = 0
        cuantias.append({
            "importe": float(importe),
            "timestamp": base_time.isoformat()
        })

    return cuantias

# --------------------------------------------------------------
# GENERACIÓN DE 5000 EXPEDIENTES
# --------------------------------------------------------------
rows_to_insert = []
juzgados = [f"Juzgado {i}" for i in range(1, 51)]

for _ in range(5000):
    procedimiento = random.choice(["Verbal", "Ordinario"])
    secuencia = SECUENCIA_VERBAL if procedimiento == "Verbal" else SECUENCIA_ORDINARIO

    rows_to_insert.append({
        "Ref": str(uuid.uuid4())[:8],
        "Procedimiento": procedimiento,
        "Juzgado": random.choice(juzgados),
        "Cuantia": generar_cuantias(),
        "Estado": generar_estados(secuencia)
    })

print(f"Generados {len(rows_to_insert)} expedientes. Insertando en BigQuery...")

# --------------------------------------------------------------
# INSERCIÓN
# --------------------------------------------------------------
errors = client.insert_rows_json(full_table_id, rows_to_insert)

if errors:
    print("Errores durante la inserción:")
    for e in errors:
        print(e)
else:
    print("Carga completada correctamente: 5000 expedientes insertados.")
