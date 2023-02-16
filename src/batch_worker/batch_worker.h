#pragma once
#include <condition_variable>
#include <deque>
#include <mutex>
#include <vector>

class nocopyable
{
public:
    nocopyable() = default;
private:
    nocopyable(const nocopyable &);
    nocopyable& operator=(const nocopyable &);
    nocopyable(nocopyable &&);
    nocopyable& operator=(nocopyable &&);
};

class BatchWorker;

class Worker
{
public:
    virtual bool CanMerge() = 0;
    virtual int Do() = 0;
    virtual bool Done() = 0;
    virtual void Merge(Worker * w) = 0;
    virtual ~Worker() {};
    void SetDone(int s) { done_status_ = s; }
    int GetDone() const { return done_status_; }
private:
    friend BatchWorker;
    int done_status_{0};
};

/*
 * A batch worker inspired by leveldb batch writer.
 * Batch worker can meger you small works to a bigger one, and finish it using less syscall.
 */
class BatchWorker: public nocopyable
{
public:
    BatchWorker() = default;
    ~BatchWorker() = default;

    int DoWork(Worker * worker);
private:
    std::deque<Worker*> workers_;
    std::mutex mu_;
    std::condition_variable cv_;
};
