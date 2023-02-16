#include "batch_worker.h"
#include <mutex>

int BatchWorker::DoWork(Worker * worker)
{
    std::unique_lock<std::mutex> helper(mu_);
    workers_.push_back(worker);

    while (workers_.front() != worker && !worker->Done())
    {
        cv_.wait(helper);
    }

    if (worker->Done())
    {
        return worker->done_status_;
    }

    // worker is first worker
    int worker_count = 0;
    auto iter = workers_.begin();
    Worker * merge_worker = *iter;
    iter++;
    worker_count++;

    while (iter != workers_.end() && merge_worker->CanMerge())
    {
        merge_worker->Merge(*iter);
        iter++;
        worker_count++;
    }

    helper.unlock();
    merge_worker->done_status_ = merge_worker->Do();

    helper.lock();
    while (worker_count > 0)
    {
        workers_.pop_front();
        worker_count--;
    }

    cv_.notify_all();
    return merge_worker->done_status_;
}
