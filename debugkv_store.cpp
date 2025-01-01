#include <iostream>
#include <fstream>
#include <unordered_map>
#include <mutex>
#include <thread>
#include <chrono>
#include "json.hpp"

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
    const size_t MAX_VALUE_SIZE = 16 * 1024;

    void saveToFile() {

        cout << "Debug: Saving data to file: " << filePath << endl;

        // Open the file to write the data
        ofstream file(filePath, ios::trunc);
        if (!file.is_open()) {
            throw runtime_error("Failed to open file for writing.");
        }

        try {
            json j;
            // Populate the JSON object with key-value pairs
            for (const auto& [key, entry] : store) {
                j[key] = {{"value", entry.value}, {"ttl", entry.ttl}};
            }

            if (j.empty()) {
                cout << "Debug: Store is empty. Writing an empty JSON object to file." << endl;
                file << "{}";
            } else {
                file << j.dump();  // Write the JSON object to the file
                cout << "Debug: Data written to file: " << j.dump() << endl;
            }
        } catch (const exception& e) {
            cerr << "Error while saving to file: " << e.what() << endl;
            throw;
        }

        file.close();
        cout << "Debug: File saved successfully." << endl;
    }

    void loadFromFile() {
        lock_guard<mutex> lock(mtx);
        cout << "Debug: Loading data from file: " << filePath << endl;

        ifstream file(filePath);
        if (file.is_open()) {
            json data;
            try {
                file >> data;  // Attempt to read the data from the file
                if (data.is_null() || data.empty()) {
                    cout << "Debug: File is empty or contains null data." << endl;
                    return;
                }
                // Populate the store from the file data
                for (auto& [key, entry] : data.items()) {
                    ValueEntry ve;
                    ve.value = entry["value"];
                    ve.ttl = entry["ttl"];
                    store[key] = ve;
                }
                cout << "Debug: Data loaded: " << data.dump() << endl;
            } catch (const exception& e) {
                cerr << "Error while loading from file: " << e.what() << endl;
            }
            file.close();
        } else {
            cout << "Debug: File does not exist. Starting fresh." << endl;
        }
    }

    bool isExpired(const string& key) const {
        if (store.count(key) == 0) return false;
        time_t now = time(nullptr);
        bool expired = store.at(key).ttl != 0 && now > store.at(key).ttl;
        if (expired) {
            cout << "Debug: Key '" << key << "' has expired." << endl;
        }
        return expired;
    }

public:
    KVDataStore(const string& path = "datastore.json") : filePath(path) {
        loadFromFile();  // Load data when the object is created
    }

    ~KVDataStore() {
        saveToFile();  // Ensure data is saved when the object is destroyed
    }

    string create(const string& key, const json& value, time_t ttl = 0) {
        lock_guard<mutex> lock(mtx);
        cout << "Debug: Creating key: " << key << endl;

        if (key.length() > MAX_KEY_LENGTH) return "Error: Key length exceeds 32 characters.";
        if (value.dump().length() > MAX_VALUE_SIZE) return "Error: Value size exceeds 16KB.";
        if (store.count(key)) return "Error: Key already exists.";

        ValueEntry entry;
        entry.value = value;
        entry.ttl = ttl == 0 ? 0 : time(nullptr) + ttl;

        store[key] = entry;

        try {
            cout << "Debug: Calling saveToFile..." << endl;
            saveToFile();  // Save data immediately after creating the key
        } catch (const exception& e) {
            cerr << "Error in saveToFile: " << e.what() << endl;
            return "Error: Failed to save data.";
        }

        cout << "Debug: Key created successfully!" << endl;

        return "Key-value pair created successfully.";
    }

   string read(const string& key) {
    lock_guard<mutex> lock(mtx);
    cout << "Debug: Reading key: " << key << endl;

    if (!store.count(key)) return "Error: Key not found.";
    if (isExpired(key)) {
        store.erase(key);  // Remove expired key immediately
        saveToFile();
        return "Error: Key has expired.";  // Return expired message
    }

    return store[key].value.dump();  // Return value if not expired
}


    string remove(const string& key) {
        lock_guard<mutex> lock(mtx);
        cout << "Debug: Removing key: " << key << endl;

        if (!store.count(key)) return "Error: Key not found.";
        store.erase(key);
        saveToFile();
        return "Key-value pair deleted successfully.";
    }

    string batchCreate(const vector<pair<string, json>>& entries, time_t ttl = 0) {
        lock_guard<mutex> lock(mtx);
        cout << "Debug: Batch creating keys..." << endl;

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
    cout << "Program started..." << endl;
    KVDataStore kvStore;

    cout << "Creating key1..." << endl;
    string result = kvStore.create("key1", {{"name", "Alice"}}, 10);
    cout << "Create Result: " << result << endl;

    cout << "Reading key1..." << endl;
    result = kvStore.read("key1");
    cout << "Read Result: " << result << endl;

    cout << "Sleeping for 9 seconds to allow TTL expiration..." << endl;
    this_thread::sleep_for(chrono::seconds(9));

    cout << "Reading expired key1..." << endl;
    result = kvStore.read("key1");
    cout << "Read Result: " << result << endl;

    cout << "Removing key1..." << endl;
    result = kvStore.remove("key1");
    cout << "Remove Result: " << result << endl;

    cout << "Batch creating keys..." << endl;
    vector<pair<string, json>> batch = {
        {"key2", {{"name", "Bob"}}},
        {"key3", {{"name", "Charlie"}}}
    };
    result = kvStore.batchCreate(batch);
    cout << "Batch Create Result: " << result << endl;

    return 0;
}
