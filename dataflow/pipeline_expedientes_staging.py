import argparse
import json
import datetime

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions, StandardOptions


# ==========================================================
# CONFIGURACIÓN BÁSICA
# ==========================================================
PROJECT_ID = "PROYECTO"
DATASET = "Expedientes_OLAP"
TABLE = "Expedientes_Staging"

BQ_TABLE_SPEC = f"{PROJECT_ID}:{DATASET}.{TABLE}"

BQ_SCHEMA = {
    "fields": [
        {"name": "Ref", "type": "STRING", "mode": "REQUIRED"},
        {"name": "Cuantia", "type": "RECORD", "mode": "REPEATED", "fields": [
            {"name": "importe", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "timestamp", "type": "TIMESTAMP", "mode": "NULLABLE"}
        ]},
        {"name": "Estado", "type": "RECORD", "mode": "REPEATED", "fields": [
            {"name": "estado", "type": "STRING", "mode": "NULLABLE"},
            {"name": "timestamp", "type": "TIMESTAMP", "mode": "NULLABLE"}
        ]},
        {"name": "ingestion_timestamp", "type": "TIMESTAMP", "mode": "REQUIRED"}
    ]
}


# ==========================================================
# TRANSFORM: PARSEAR MENSAJES DE PUB/SUB → FILAS BQ
# ==========================================================
class ParsePubSubMessage(beam.DoFn):
    def process(self, element, publish_time=beam.DoFn.TimestampParam):
        """
        element: bytes (payload de Pub/Sub)
        publish_time: timestamp de Pub/Sub (no lo usamos, pero podríamos)
        """

        # Pub/Sub envía bytes -> pasamos a str
        payload_str = element.decode("utf-8")
        try:
            data = json.loads(payload_str)
        except json.JSONDecodeError:
            # si el mensaje está mal formado, lo ignoramos
            return

        ref = data.get("Ref")
        cuantia = data.get("Cuantia", [])
        estado = data.get("Estado", [])

        if not ref:
            # sin Ref no tiene sentido, descartamos
            return

        # ingestion_timestamp: tiempo de procesamiento
        ahora = datetime.datetime.utcnow().isoformat() + "Z"

        row = {
            "Ref": ref,
            "Cuantia": [],
            "Estado": [],
            "ingestion_timestamp": ahora
        }

        # Normalizamos cuantía (por si viniera vacío o raro)
        if isinstance(cuantia, list):
            for c in cuantia:
                importe = c.get("importe")
                ts = c.get("timestamp")
                if importe is not None and ts is not None:
                    row["Cuantia"].append({
                        "importe": float(importe),
                        "timestamp": ts
                    })

        # Normalizamos estados
        if isinstance(estado, list):
            for e in estado:
                est = e.get("estado")
                ts = e.get("timestamp")
                if est is not None and ts is not None:
                    row["Estado"].append({
                        "estado": est,
                        "timestamp": ts
                    })

        yield row


# ==========================================================
# PIPELINE
# ==========================================================
def run(argv=None):
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--project",
        default=PROJECT_ID,
        help="ID del proyecto de GCP"
    )
    parser.add_argument(
        "--region",
        default="europe-west1",
        help="Región de Dataflow"
    )
    parser.add_argument(
        "--input_subscription",
        required=True,
        help=(
            "Subscripción de Pub/Sub en formato "
            "'projects/PROYECTO/subscriptions/NOMBRE'"
        ),
    )
    parser.add_argument(
        "--temp_location",
        required=True,
        help="Ruta gs:// para ficheros temporales de Dataflow"
    )
    parser.add_argument(
        "--staging_location",
        required=True,
        help="Ruta gs:// para ficheros de staging de Dataflow"
    )

    known_args, pipeline_args = parser.parse_known_args(argv)

    # Opciones de pipeline
    pipeline_options = PipelineOptions(
        pipeline_args,
        save_main_session=True,
        streaming=True,  # MUY IMPORTANTE para Pub/Sub
        project=known_args.project,
        region=known_args.region,
        temp_location=known_args.temp_location,
        staging_location=known_args.staging_location,
    )
    pipeline_options.view_as(SetupOptions).save_main_session = True
    pipeline_options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | "Leer de PubSub" >> beam.io.ReadFromPubSub(subscription=known_args.input_subscription)
            | "Parsear JSON" >> beam.ParDo(ParsePubSubMessage())
            | "Filtrar nulos" >> beam.Filter(lambda row: row is not None)
            | "Escribir en BigQuery" >> beam.io.WriteToBigQuery(
                table=BQ_TABLE_SPEC,
                schema=BQ_SCHEMA,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
                method=beam.io.WriteToBigQuery.Method.STREAMING_INSERTS,
            )
        )


if __name__ == "__main__":
    run()
