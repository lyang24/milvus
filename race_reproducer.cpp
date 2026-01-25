/**
 * Minimal reproducer for the SegmentGrowingImpl race condition.
 *
 * This mimics the exact pattern in Milvus:
 * - sch_mutex_ protects schema updates (Reopen/fill_empty_field)
 * - But Search/Retrieve reads from insert_record_ WITHOUT acquiring sch_mutex_
 *
 * Compile with ThreadSanitizer:
 *   g++ -std=c++17 -g -fsanitize=thread -o race_reproducer race_reproducer.cpp -lpthread
 *
 * Run:
 *   ./race_reproducer
 *
 * Expected: TSan reports data race between writer and readers
 */

#include <atomic>
#include <chrono>
#include <iostream>
#include <mutex>
#include <shared_mutex>
#include <thread>
#include <vector>

// Simulates insert_record_ data structure
struct InsertRecord {
    std::vector<int64_t> data;
    std::atomic<size_t> size{0};

    void set_data(size_t count, int64_t value) {
        // This simulates fill_empty_field writing to insert_record_
        if (data.size() < count) {
            data.resize(count);
        }
        for (size_t i = 0; i < count; i++) {
            data[i] = value;  // WRITE - race with readers
        }
        size.store(count);
    }

    int64_t read_data(size_t idx) {
        // This simulates Search reading from insert_record_
        if (idx < size.load()) {
            return data[idx];  // READ - race with writer
        }
        return -1;
    }
};

// Simulates SegmentGrowingImpl
class Segment {
public:
    std::shared_mutex sch_mutex_;  // Only used by Insert and Reopen
    InsertRecord insert_record_;
    std::atomic<int> schema_version_{1};

    // Simulates Insert() - acquires shared lock
    void Insert(size_t count, int64_t value) {
        std::shared_lock lock(sch_mutex_);
        insert_record_.set_data(count, value);
    }

    // Simulates Reopen() -> fill_empty_field() - acquires unique lock
    void Reopen(int new_version) {
        std::unique_lock lock(sch_mutex_);
        if (new_version > schema_version_.load()) {
            // fill_empty_field - writes to insert_record_
            // This is protected by sch_mutex_
            insert_record_.set_data(10000, new_version * 100);
            schema_version_.store(new_version);
        }
    }

    // Simulates Search() - NO LOCK! This is the bug.
    int64_t Search() {
        // BUG: Search reads from insert_record_ without acquiring sch_mutex_
        // This races with Reopen's fill_empty_field
        int64_t sum = 0;
        size_t sz = insert_record_.size.load();
        for (size_t i = 0; i < sz && i < 1000; i++) {
            sum += insert_record_.read_data(i);  // RACE HERE
        }
        return sum;
    }
};

int main() {
    std::cout << "Race Condition Reproducer\n";
    std::cout << "=========================\n\n";
    std::cout << "This reproduces the SegmentGrowingImpl bug where:\n";
    std::cout << "- Reopen() holds sch_mutex_ while writing to insert_record_\n";
    std::cout << "- Search() reads from insert_record_ WITHOUT any lock\n\n";

    Segment segment;

    // Initial data
    segment.Insert(10000, 42);

    std::atomic<bool> start{false};
    std::atomic<bool> stop{false};
    std::atomic<int> search_count{0};
    std::atomic<int> reopen_count{0};

    // Search threads (readers) - NO LOCK
    std::vector<std::thread> search_threads;
    for (int i = 0; i < 4; i++) {
        search_threads.emplace_back([&]() {
            while (!start.load()) std::this_thread::yield();
            while (!stop.load()) {
                segment.Search();  // Reads without lock - RACES with Reopen
                search_count.fetch_add(1);
            }
        });
    }

    // Reopen thread (writer) - holds sch_mutex_
    std::thread reopen_thread([&]() {
        while (!start.load()) std::this_thread::yield();
        for (int v = 2; v <= 10; v++) {
            segment.Reopen(v);  // Writes with lock - but readers don't wait
            reopen_count.fetch_add(1);
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    });

    std::cout << "Starting threads...\n";
    start.store(true);

    // Let it run
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    stop.store(true);

    // Wait for threads
    for (auto& t : search_threads) t.join();
    reopen_thread.join();

    std::cout << "\nResults:\n";
    std::cout << "  Search operations: " << search_count.load() << "\n";
    std::cout << "  Reopen operations: " << reopen_count.load() << "\n";
    std::cout << "\nIf compiled with -fsanitize=thread, TSan should report:\n";
    std::cout << "  WARNING: ThreadSanitizer: data race\n";
    std::cout << "    Write at InsertRecord::set_data (Reopen path)\n";
    std::cout << "    Read at InsertRecord::read_data (Search path)\n";

    return 0;
}
