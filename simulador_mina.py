import os
import time
import json
import random
import uuid # ¡Nueva librería para generar IDs únicos!
from datetime import datetime
from dotenv import load_dotenv
from azure.eventhub import EventHubProducerClient, EventData

# 1. Cargar las variables de entorno desde el archivo .env
load_dotenv()

# 2. Obtener las credenciales de forma segura
CADENA_CONEXION = os.getenv("EVENT_HUB_CONNECTION_STRING")
NOMBRE_EVENT_HUB = os.getenv("EVENT_HUB_NAME")

# Validación de seguridad para evitar que el script corra sin credenciales
if not CADENA_CONEXION or not NOMBRE_EVENT_HUB:
    raise ValueError("¡Alto ahí! Faltan variables de entorno. Revisa tu archivo .env")

def generar_telemetria():
    """Genera un JSON anidado y realista de un camión minero de alto tonelaje."""
    
    # Asignamos un estado lógico con pesos (es más probable que esté transportando a que esté en mantenimiento)
    estado_actual = random.choices(
        ["CARGANDO", "TRANSPORTANDO", "DESCARGANDO", "RALENTI", "MANTENIMIENTO"],
        weights=[0.15, 0.50, 0.15, 0.15, 0.05], k=1
    )[0]
    
    # Simulando alarmas (el 90% de las veces vacío, 10% con algún código de error)
    alarmas = []
    if random.random() > 0.9:
        alarmas.append(random.choice(["ERR-MOTOR-TEMP", "WARN-PRESION-LLANTA", "ERR-GPS-SEÑAL", "WARN-COMBUSTIBLE-BAJO"]))
        
    return {
        "metadata": {
            "event_id": str(uuid.uuid4()),
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "version_sensor": "v2.1"
        },
        "equipo": {
            "id_equipo": f"TRK-{random.randint(100, 150)}",
            "tipo": "CAMION_MINERO_797F",
            "id_operador": f"OP-{random.randint(8000, 8050)}"
        },
        "ubicacion": {
            # Coordenadas simuladas en zona minera de altura
            "latitud": round(random.uniform(-9.6000, -9.4000), 6),
            "longitud": round(random.uniform(-77.2000, -77.0000), 6),
            "elevacion_m": round(random.uniform(4100.0, 4300.0), 1)
        },
        "telemetria": {
            "motor": {
                "rpm": random.randint(800, 2200),
                "temperatura_c": round(random.uniform(75.0, 105.0), 1),
                "presion_aceite_kpa": round(random.uniform(300.0, 500.0), 1)
            },
            "desempeno": {
                "velocidad_kmh": round(random.uniform(15.0, 55.0), 1) if estado_actual == "TRANSPORTANDO" else 0.0,
                "carga_util_toneladas": round(random.uniform(300.0, 380.0), 1) if estado_actual in ["TRANSPORTANDO", "DESCARGANDO"] else 0.0,
                "nivel_combustible_pct": round(random.uniform(10.0, 100.0), 1)
            },
            "neumaticos_psi": {
                "frontal_izq": round(random.uniform(95.0, 105.0), 1),
                "frontal_der": round(random.uniform(95.0, 105.0), 1),
                "trasero_izq_ext": round(random.uniform(95.0, 105.0), 1),
                "trasero_izq_int": round(random.uniform(95.0, 105.0), 1),
                "trasero_der_ext": round(random.uniform(95.0, 105.0), 1),
                "trasero_der_int": round(random.uniform(95.0, 105.0), 1)
            }
        },
        "estado": {
            "operacion": estado_actual,
            "alarmas_activas": alarmas
        }
    }
    """Genera un diccionario con datos simulados de un camión minero."""
    return {
        "camion_id": f"TRK-{random.randint(100, 105)}",
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "temperatura_motor_c": round(random.uniform(70.0, 115.0), 2),
        "presion_llantas_psi": round(random.uniform(85.0, 105.0), 2),
        "velocidad_kmh": round(random.uniform(0.0, 60.0), 2),
        "estado": random.choice(["OPERATIVO", "OPERATIVO", "MANTENIMIENTO", "ALERTA"])
    }

def ejecutar_simulador():
    productor = EventHubProducerClient.from_connection_string(
        conn_str=CADENA_CONEXION,
        eventhub_name=NOMBRE_EVENT_HUB
    )

    print("Iniciando transmisión segura de telemetría a Azure Event Hubs...")
    print("Presiona Ctrl+C para detener el simulador.\n")

    try:
        with productor:
            while True:
                lote_eventos = productor.create_batch()
                
                datos_maquinaria = generar_telemetria()
                payload_json = json.dumps(datos_maquinaria)
                
                lote_eventos.add(EventData(payload_json))
                productor.send_batch(lote_eventos)
                
                print(f"[ENVIADO] {payload_json}")
                time.sleep(2)
                
    except KeyboardInterrupt:
        print("\nSimulación detenida manualmente. Cerrando conexión...")
    except Exception as e:
        print(f"\nError durante la transmisión: {e}")

if __name__ == "__main__":
    ejecutar_simulador()