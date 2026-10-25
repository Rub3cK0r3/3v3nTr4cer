#include <pybind11/pybind11.h>

#include <condition_variable>
#include <cstddef>
#include <mutex>
#include <queue>
#include <stdexcept>
#include <thread>
#include <utility>
#include <vector>

namespace py = pybind11;

class NativeAsyncManager {
public:
    NativeAsyncManager(size_t worker_count, size_t max_queue_size, py::function dispatch)
        : max_queue_size_(max_queue_size), dispatch_(std::move(dispatch)) {
        if (worker_count == 0) {
            throw std::invalid_argument("worker_count must be positive");
        }
        if (max_queue_size_ == 0) {
            throw std::invalid_argument("max_queue_size must be positive");
        }

        workers_.reserve(worker_count);
        try {
            for (size_t index = 0; index < worker_count; ++index) {
                workers_.emplace_back([this] { worker_loop(); });
            }
        } catch (...) {
            {
                std::lock_guard<std::mutex> lock(mutex_);
                stopping_ = true;
            }
            available_.notify_all();
            for (auto& worker : workers_) {
                if (worker.joinable()) {
                    worker.join();
                }
            }
            throw;
        }
    }

    ~NativeAsyncManager() {
        shutdown();
    }

    void enqueue(py::object item) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (stopping_) {
            throw std::runtime_error("AsyncManager is stopped");
        }
        if (queue_.size() >= max_queue_size_) {
            throw std::runtime_error("AsyncManager queue capacity invariant violated");
        }

        queue_.push(std::move(item));
        available_.notify_one();
    }

    bool empty() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return queue_.empty();
    }

    void shutdown() {
        std::lock_guard<std::mutex> shutdown_lock(shutdown_mutex_);
        {
            std::lock_guard<std::mutex> lock(mutex_);
            stopping_ = true;
        }
        available_.notify_all();

        py::gil_scoped_release release;
        for (auto& worker : workers_) {
            if (worker.joinable()) {
                worker.join();
            }
        }
    }

private:
    void worker_loop() {
        while (true) {
            py::object item;
            {
                std::unique_lock<std::mutex> lock(mutex_);
                available_.wait(lock, [this] { return stopping_ || !queue_.empty(); });
                if (stopping_ && queue_.empty()) {
                    return;
                }
                item = std::move(queue_.front());
                queue_.pop();
            }

            {
                py::gil_scoped_acquire acquire;
                try {
                    dispatch_(std::move(item));
                } catch (py::error_already_set& error) {
                    error.discard_as_unraisable("NativeAsyncManager worker");
                }
            }
        }
    }

    const size_t max_queue_size_;
    py::function dispatch_;
    mutable std::mutex mutex_;
    std::mutex shutdown_mutex_;
    std::condition_variable available_;
    std::queue<py::object> queue_;
    std::vector<std::thread> workers_;
    bool stopping_ = false;
};

PYBIND11_MODULE(_async_manager, module) {
    py::class_<NativeAsyncManager>(module, "NativeAsyncManager")
        .def(py::init<size_t, size_t, py::function>())
        .def("enqueue", &NativeAsyncManager::enqueue)
        .def("empty", &NativeAsyncManager::empty)
        .def("shutdown", &NativeAsyncManager::shutdown);
}
