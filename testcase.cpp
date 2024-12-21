#include <iostream>
#include <cassert>
#include <thread>
#include <vector>
#include <nlohmann/json.hpp>
#include "kvdatastore.h" 

using json = nlohmann::json;
using namespace std;

void testCreateAndRead() {
    KVDataStore kvStore("test_datastore.json");

    auto result = kvStore.create("key1", {"name", "Alice"}, 10);
    if (result != "Key-value pair created successfully.") {
        throw runtime_error("testCreateAndRead failed: " + result);
    }

    
    result = kvStore.read("key1");
    if (result != "{\"name\":\"Alice\"}") {
        throw runtime_error("testCreateAndRead failed: " + result);
    }

   
    this_thread::sleep_for(chrono::seconds(11));
    result = kvStore.read("key1");
    if (result != "Error: Key has expired.") {
        throw runtime_error("testCreateAndRead failed: " + result);
    }

    cout << "testCreateAndRead passed!" << endl;
}

void testRemove() {
    KVDataStore kvStore("test_datastore.json");

    
    auto result = kvStore.create("key2", {"name", "Bob"});
    if (result != "Key-value pair created successfully.") {
        throw runtime_error("testRemove failed: " + result);
    }

    result = kvStore.remove("key2");
    if (result != "Key-value pair deleted successfully.") {
        throw runtime_error("testRemove failed: " + result);
    }

    
    result = kvStore.remove("key2");
    if (result != "Error: Key not found.") {
        throw runtime_error("testRemove failed: " + result);
    }

    cout << "testRemove passed!" << endl;
}

void testBatchCreate() {
    KVDataStore kvStore("test_datastore.json");

    vector<pair<string, json>> batch = {
        {"key3", {"name", "Charlie"}},
        {"key4", {"name", "Diana"}}
    };

    
    auto result = kvStore.batchCreate(batch);
    if (result != "Batch create operation successful.") {
        throw runtime_error("testBatchCreate failed: " + result);
    }

    result = kvStore.read("key3");
    if (result != "{\"name\":\"Charlie\"}") {
        throw runtime_error("testBatchCreate failed: " + result);
    }

    result = kvStore.read("key4");
    if (result != "{\"name\":\"Diana\"}") {
        throw runtime_error("testBatchCreate failed: " + result);
    }

   
    vector<pair<string, json>> batchWithDuplicate = {
        {"key3", {"name", "Charlie"}},
        {"key5", {"name", "Eve"}}
    };
    result = kvStore.batchCreate(batchWithDuplicate);
    if (result != "Error: Duplicate key found in batch.") {
        throw runtime_error("testBatchCreate failed: " + result);
    }

    cout << "testBatchCreate passed!" << endl;
}

void testPeriodicCleanup() {
    KVDataStore kvStore("test_datastore.json");

   
    auto result = kvStore.create("key6", {"name", "Frank"}, 5);
    if (result != "Key-value pair created successfully.") {
        throw runtime_error("testPeriodicCleanup failed: " + result);
    }

    result = kvStore.create("key7", {"name", "Grace"}, 15);
    if (result != "Key-value pair created successfully.") {
        throw runtime_error("testPeriodicCleanup failed: " + result);
    }

    
    this_thread::sleep_for(chrono::seconds(10));

    
    result = kvStore.read("key6");
    if (result != "Error: Key has expired.") {
        throw runtime_error("testPeriodicCleanup failed: " + result);
    }

    result = kvStore.read("key7");
    if (result != "{\"name\":\"Grace\"}") {
        throw runtime_error("testPeriodicCleanup failed: " + result);
    }

    cout << "testPeriodicCleanup passed!" << endl;
}

void testConcurrentAccess() {
    KVDataStore kvStore("test_datastore.json");

    auto writer = [&kvStore]() {
        for (int i = 0; i < 10; ++i) {
            auto result = kvStore.create("key" + to_string(i), {"value", i});
            if (result != "Key-value pair created successfully.") {
                throw runtime_error("testConcurrentAccess writer failed: " + result);
            }
        }
    };

    auto reader = [&kvStore]() {
        for (int i = 0; i < 10; ++i) {
            kvStore.read("key" + to_string(i));
        }
    };

    thread t1(writer);
    thread t2(reader);

    t1.join();
    t2.join();

    cout << "testConcurrentAccess passed!" << endl;
}

int main() {
    try {
        testCreateAndRead();
        testRemove();
        testBatchCreate();
        testPeriodicCleanup();
        testConcurrentAccess();

        cout << "All tests passed!" << endl;
    } catch (const exception& e) {
        cerr << "Test failed: " << e.what() << endl;
        return 1;
    }
    return 0;
}
