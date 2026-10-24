#include <iostream>
#include <vector>
#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <string>
#include <functional>
#include <chrono>

// Mapeo exacto de tu tabla SQL 'events'
struct SqlEvent {
    std::string id;
    std::string severity;
    std::string stack;
    std::string type;
    int64_t timestamp;
    int64_t received_at;
    std::string resource;
    std::string referrer;
    std::string app_name;
    std::string app_version;
    std::string app_stage;
    std::string tags_json; // Almacenado temporalmente como string JSON
    std::string endpoint_id;
    std::string endpoint_language;
    std::string endpoint_platform;
    std::string endpoint_os;
    std::string endpoint_os_version;
    std::string endpoint_runtime;
    std::string endpoint_runtime_version;
    std::string endpoint_country;
    std::string endpoint_user_agent;
    std::string endpoint_device_type;
};

// Worker Pool especializado en persistencia SQL
class SqlEventWorkerPool {
public:
    SqlEventWorkerPool(size_t workerCount, size_t batchSize) 
        : stop(false), maxBatchSize(batchSize) 
    {
        for (size_t i = 0; i < workerCount; ++i) {
            workers.emplace_back([this, i] {
                // Cada hilo mantiene su propio buffer local para evitar colisiones entre núcleos
                std::vector<SqlEvent> localBatch;
                localBatch.reserve(maxBatchSize);

                while (true) {
                    {
                        std::unique_lock<std::mutex> lock(this->queueMutex);
                        // Espera si no hay eventos y no se ha ordenado parar, 
                        // O si hay eventos pero no son suficientes para un lote completo (y no estamos cerrando)
                        this->condition.wait(lock, [this, &localBatch] {
                            return this->stop || 
                                   !this->eventQueue.empty() || 
                                   (this->stop && !localBatch.empty());
                        });

                        // Transferir eventos de la cola global a la cola local del hilo
                        while (!this->eventQueue.empty() && localBatch.size() < maxBatchSize) {
                            localBatch.push_back(std::move(this->eventQueue.front()));
                            this->eventQueue.pop();
                        }

                        if (this->stop && localBatch.empty() && this->eventQueue.empty()) {
                            return; // Terminación segura del hilo
                        }
                    }

                    // Si el lote está listo o el sistema se está deteniendo, persistir en la DB
                    if (!localBatch.empty()) {
                        executeBatchInsert(localBatch, i);
                        localBatch.clear();
                    }
                }
            });
        }
    }

    // API pública para que la capa de red/API envíe eventos de forma no bloqueante
    void pushEvent(SqlEvent event) {
        {
            std::lock_guard<std::mutex> lock(queueMutex);
            if (stop) return;
            eventQueue.push(std::move(event));
        }
        condition.notify_one(); // Despierta a un worker disponible
    }

    ~SqlEventWorkerPool() {
        {
            std::lock_guard<std::mutex> lock(queueMutex);
            stop = true;
        }
        condition.notify_all();
        for (std::thread &worker : workers) {
            if (worker.joinable()) worker.join();
        }
    }

private:
    // Simulación de la inserción SQL masiva
    void executeBatchInsert(const std::vector<SqlEvent>& batch, size_t threadId) {
        // En producción, aquí inicializas o reúsas un "Prepared Statement"
        // Ejemplo conceptual del comando SQL generado dinámicamente:
        // "INSERT INTO events (...) VALUES (?,?,?...), (?,?,?...);"
        
        std::cout << "[Hilo Worker " << threadId << "] Insertando lote de " 
                  << batch.size() << " eventos en la base de datos.\n";
        
        // Simular latencia de red/disco de la base de datos (I/O Bound)
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }

    std::vector<std::thread> workers;
    std::queue<SqlEvent> eventQueue;
    std::mutex queueMutex;
    std::condition_variable condition;
    bool stop;
    size_t maxBatchSize;
};

// Ejemplo de uso
int main() {
    // Inicializa un pool de 4 hilos distribuidos, procesando en lotes de 5 en 5
    SqlEventWorkerPool dbPool(4, 5);

    std::cout << "Ingestando ráfaga de eventos de telemetría...\n";
    for (int i = 0; i < 12; ++i) {
        SqlEvent ev;
        ev.id = "evt_uuid_" + std::to_string(i);
        ev.severity = (i % 3 == 0) ? "error" : "info";
        ev.timestamp = 1713980000000 + i;
        ev.received_at = 1713980000500 + i;
        ev.app_name = "E-Commerce-Frontend";
        ev.tags_json = "{\"user_type\": \"premium\"}";
        
        dbPool.pushEvent(std::move(ev));
    }

    // Esperar a que los hilos procesen los lotes remanentes
    std::this_thread::sleep_for(std::chrono::seconds(1));
    return 0;
}
