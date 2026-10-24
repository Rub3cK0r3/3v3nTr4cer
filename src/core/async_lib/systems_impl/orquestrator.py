import event_engine
import time

def run_orchestrator():
    print("Orquestador Python iniciado...")
    
    # Python acumula y procesa la lógica de los eventos (ej. desde una cola de red)
    # Simulamos la preparación de un lote de eventos
    batch_size = 1000
    
    ids = [f"uuid_py_{i}" for i in range(batch_size)]
    severities = ["error" if i % 10 == 0 else "info" for i in range(batch_size)]
    timestamps = [int(time.time() * 1000) for _ in range(batch_size)]
    app_names = ["API-Gateway" for _ in range(batch_size)]
    tags_jsons = ['{"env": "production"}'] * batch_size

    # Enviamos el lote completo a C++. 
    # Gracias a `gil_scoped_release`, esta llamada retorna instantáneamente en Python.
    event_engine.ingest_batch(ids, severities, timestamps, app_names, tags_jsons)
    
    print("Python liberado. Continuando con la orquestación mientras C++ escribe en SQL...")
    time.sleep(1) # Simulación de tiempo de vida de la app

if __name__ == "__main__":
    run_orchestrator()

