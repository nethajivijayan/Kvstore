#include <iostream>
#include <fstream>
#include <unordered_map>
#include <mutex>
#include <thread>
#include <chrono>
#include <nlohmann/json.hpp>

using json = nlohmann::json;
using namespace std;


struct ValueEntry {
    json value;
    time_t ttl; 
};

class KVDataStore {
private:
    unordered_map<string, ValueEntry> store;
    string filePath;
    mutable mutex mtx;
    const size_t MAX_KEY_LENGTH = 32;
    const size_t MAX_VALUE_SIZE = 16 * 1024; // 16KB
    const size_t MAX_FILE_SIZE = 1 * 1024 * 1024 * 1024; 

    
    void saveToFile() {
        lock_guard<mutex> lock(mtx);
        ofstream file(filePath, ios::trunc);
        if (file.is_open()) {
            file << json(store).dump();
            file.close();
        } else {
            throw runtime_error("Failed to open file for writing.");
        }
    }

    // Load the store from the file
    void loadFromFile() {
        lock_guard<mutex> lock(mtx);
        ifstream file(filePath);
        if (file.is_open()) {
            json data;
            file >> data;
            for (auto& [key, entry] : data.items()) {
                ValueEntry ve;
                ve.value = entry["value"];
                ve.ttl = entry["ttl"];
                store[key] = ve;
            }
            file.close();
        }
    }

    // Check if a key is expired
    bool isExpired(const string& key) const {
        if (store.count(key) == 0) return false;
        time_t now = time(nullptr);
        return store.at(key).ttl != 0 && now > store.at(key).ttl;
    }

    // Remove expired keys
    void cleanupExpiredKeys() {
        lock_guard<mutex> lock(mtx);
        for (auto it = store.begin(); it != store.end();) {
            if (isExpired(it->first)) {
                it = store.erase(it);
            } else {
                ++it;
            }
        }
    }

public:
    KVDataStore(const string& path = "datastore.json") : filePath(path) {
        if (ifstream(path)) {
            loadFromFile();
        }
    }

    ~KVDataStore() {
        saveToFile();
    }

    string create(const string& key, const json& value, time_t ttl = 0) {
        lock_guard<mutex> lock(mtx);
        cleanupExpiredKeys();

        if (key.length() > MAX_KEY_LENGTH) return "Error: Key length exceeds 32 characters.";
        if (value.dump().length() > MAX_VALUE_SIZE) return "Error: Value size exceeds 16KB.";
        if (store.count(key)) return "Error: Key already exists.";

        ValueEntry entry;
        entry.value = value;
        entry.ttl = ttl == 0 ? 0 : time(nullptr) + ttl;
        store[key] = entry;
        saveToFile();
        return "Key-value pair created successfully.";
    }

    string read(const string& key) {
        lock_guard<mutex> lock(mtx);
        cleanupExpiredKeys();

        if (!store.count(key)) return "Error: Key not found.";
        if (isExpired(key)) {
            store.erase(key);
            saveToFile();
            return "Error: Key has expired.";
        }

        return store[key].value.dump();
    }

    string remove(const string& key) {
        lock_guard<mutex> lock(mtx);
        cleanupExpiredKeys();

        if (!store.count(key)) return "Error: Key not found.";
        store.erase(key);
        saveToFile();
        return "Key-value pair deleted successfully.";
    }

    string batchCreate(const vector<pair<string, json>>& entries, time_t ttl = 0) {
        lock_guard<mutex> lock(mtx);
        cleanupExpiredKeys();

        const size_t BATCH_LIMIT = 100; // Limit batch size to avoid excessive memory use
        if (entries.size() > BATCH_LIMIT) {
            return "Error: Batch size exceeds limit of 100 entries.";
        }

        for (const auto& [key, value] : entries) {
            if (key.length() > MAX_KEY_LENGTH || value.dump().length() > MAX_VALUE_SIZE) {
                return "Error: One or more keys/values exceed size limits.";
            }
            if (store.count(key)) {
                return "Error: Duplicate key found in batch.";
            }
        }

        for (const auto& [key, value] : entries) {
            ValueEntry entry;
            entry.value = value;
            entry.ttl = ttl == 0 ? 0 : time(nullptr) + ttl;
            store[key] = entry;
        }

        saveToFile();
        return "Batch create operation successful.";
    }
};

int main() {
    KVDataStore kvStore;

    // Create operation
    cout << kvStore.create("key1", {"name", "Alice"}, 10) << endl;

    // Read operation
    cout << kvStore.read("key1") << endl;

    // Wait for TTL to expire
    this_thread::sleep_for(chrono::seconds(11));
    cout << kvStore.read("key1") << endl;

    // Delete operation
    cout << kvStore.remove("key1") << endl;

    // Batch create
    vector<pair<string, json>> batch = {
        {"key2", {"name", "Bob"}},
        {"key3", {"name", "Charlie"}}
    };
    cout << kvStore.batchCreate(batch) << endl;

    return 0;
}
