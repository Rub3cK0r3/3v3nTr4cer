#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <vector>
#include <thread>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <iostream>

namespace py = pybind11;

// Estructura columnar compatible con la tabla SQL 'events'
struct ColumnarEventBatch {
    std::vector<std::string> ids;
    std::vector<std::string> severities;
    std::vector<int64_t> timestamps;
    std::vector<std::string> app_names;
    std::vector<std::string> tags_jsons;
};

class AsyncSqlDispatcher {
public:
    AsyncSqlDispatcher(size_t workers_count) : stop(false) {
        for (size_t i = 0; i < workers_count; ++i) {
            workers.emplace_back([this, i] {
                while (true) {
                    ColumnarEventBatch batch;
                    {
                        std::unique_lock<std::mutex> lock(queueMutex);
                        condition.wait(lock, [this] { return this->stop || !this->jobQueue.empty(); });
                        
                        if (this->stop && this->jobQueue.empty()) return;
                        
                        batch = std::move(this->jobQueue.front());
                        this->jobQueue.pop();
                    }
                    
                    // Inserción masiva optimizada (Simulación de I/O)
                    std::cout << "[C++ Worker " << i << "] Escribiendo en SQL lote de " 
                              << batch.ids.size() << " eventos.\n";
                }
            });
        }
    }

    void enqueueBatch(ColumnarEventBatch batch) {
        {
            std::lock_guard<std::mutex> lock(queueMutex);
            jobQueue.push(std::move(batch));
        }
        condition.notify_one();
    }

    ~AsyncSqlDispatcher() {
        { std::lock_guard<std::mutex> lock(queueMutex); stop = true; }
        condition.notify_all();
        for (auto &w : workers) { if (w.joinable()) w.join(); }
    }

private:
    std::vector<std::thread> workers;
    std::queue<ColumnarEventBatch> jobQueue;
    std::mutex queueMutex;
    std::condition_variable condition;
    bool stop;
};

// Instancia global del despachador C++
static AsyncSqlDispatcher dispatcher(4);

// Función expuesta a Python
void ingest_batch_from_python(
    std::vector<std::string> ids,
    std::vector<std::string> severities,
    std::vector<int64_t> timestamps,
    std::vector<std::string> app_names,
    std::vector<std::string> tags_jsons
) {
    ColumnarEventBatch batch{
        std::move(ids),
        std::move(severities),
        std::move(timestamps),
        std::move(app_names),
        std::move(tags_jsons)
    };

    // Liberamos el GIL aquí para que Python pueda seguir orquestando en paralelo
    py::gil_scoped_release release;
    dispatcher.enqueueBatch(std::move(batch));
}

// Macro de Pybind11 para generar el módulo de Python
PYBIND11_MODULE(event_engine, m) {
    m.def("ingest_batch", &ingest_batch_from_python, "Envía un lote columnar al pool asíncrono C++");
}

