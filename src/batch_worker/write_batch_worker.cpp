#include "batch_worker.h"
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <functional>
#include <iostream>
#include <mutex>
#include <thread>
#include <unistd.h>
#include <utility>
#include <vector>

/*
 * PrintWorker will print all elements of the nums_.
 * We use BatchWorker to merge print operation.
 */
class PrintWorker: public Worker
{
public:
    PrintWorker(std::vector<int> && nums): nums_(nums) {}
    virtual ~PrintWorker() override {};

    virtual bool CanMerge() override
    {
        if (nums_.size() > 200)
            return false;
        return true;
    }

    virtual int Do() override
    {
        for (auto x: nums_)
            std::cout << x;
        std::cout << std::endl;

        done_ = true;
        SetDone(1);
        for (auto worker: merge_workers_)
        {
            worker->SetDone(1);
            PrintWorker * pw = dynamic_cast<PrintWorker*>(worker);
            pw->done_ = true;
        }
        usleep(1000 * (std::rand() % 50));
        return GetDone();
    }

    virtual bool Done() override
    {
        return done_;
    }

    virtual void Merge(Worker * worker) override
    {
        PrintWorker * pw = dynamic_cast<PrintWorker*>(worker);
        this->merge_workers_.push_back(worker);
        for (auto x: pw->nums_)
        {
            nums_.push_back(x);
        }
    }

private:
    std::vector<int> nums_;
    std::vector<Worker*> merge_workers_;
    bool done_{false};
};

int main(int argc, char ** argv)
{

    BatchWorker batch_worker;
    std::mutex w_mu;
    std::vector<Worker*> workers;
    std::condition_variable cv;

    std::thread threads[10];

    std::srand(std::time(NULL));

    for (int i = 0; i < 10; i++)
    {
        threads[i] = std::thread([i, &batch_worker, &w_mu, &workers, &cv] () {
            for (int j = 0; j < 100; j++)
            {
                std::vector<int> tmp;
                int count = std::rand() % 50 + 10;
                for (int k = 0; k < count; k++)
                {
                    tmp.push_back(rand() % 100);
                }

                Worker * worker = nullptr;
                {
                    std::unique_lock<std::mutex> helper(w_mu);
                    worker = new PrintWorker(std::move(tmp));
                    workers.push_back(worker);
                }

                batch_worker.DoWork(worker);
            }

        });
    }

    for (auto && thread: threads)
    {
        thread.join();
    }

    for (auto worker: workers)
    {
        delete worker;
    }

    return 0;
}
