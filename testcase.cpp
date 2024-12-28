#include "doctest.h"
#include "kvstoe.hpp"
#include <thread>
#include <fstream>
#include <chrono>
#include <string>
#include <vector>
#include "json.hpp"


using json = nlohmann::json;

KVDataStore* kvStore;

TEST_SUITE("KVDataStore Tests") {

    TEST_CASE("Test Allow Only One Client Connection") {
        KVDataStore kvStore("datastore.json");
        KVDataStore anotherStore("datastore.json");
        CHECK_NOTHROW(anotherStore.create("key1", { {"name", "Client"} }));
    }

    TEST_CASE("Test TTL Checking") {
        KVDataStore kvStore("datastore.json");
        kvStore.create("key1", { {"name", "Alice"} }, 2); // TTL = 2 seconds
        std::this_thread::sleep_for(std::chrono::seconds(3));
        std::string result = kvStore.read("key1");
        CHECK(result == "Error: Key has expired.");
    }

    TEST_CASE("Test Batch Creation") {
        KVDataStore kvStore("datastore.json");
        std::vector<std::pair<std::string, json>> batch = {
            {"key1", { {"name", "Alice"} }},
            {"key2", { {"name", "Bob"} }}
        };
        std::string result = kvStore.batchCreate(batch);
        CHECK(result == "Batch create operation successful.");
        CHECK(kvStore.read("key1") == "{\"name\":\"Alice\"}");
        CHECK(kvStore.read("key2") == "{\"name\":\"Bob\"}");
    }

    TEST_CASE("Test Not Overwriting") {
        KVDataStore kvStore("datastore.json");
        kvStore.create("key1", { {"name", "Alice"} });
        std::string result = kvStore.create("key1", { {"name", "Bob"} });
        CHECK(result == "Error: Key already exists.");
        CHECK(kvStore.read("key1") == "{\"name\":\"Alice\"}");
    }

    TEST_CASE("Test Load Existing File") {
        KVDataStore kvStore("datastore.json");
        kvStore.create("key1", { {"name", "Alice"} });
        delete &kvStore;  // Triggers saveToFile()
        KVDataStore reloadedStore("datastore.json");
        CHECK(reloadedStore.read("key1") == "{\"name\":\"Alice\"}");
    }

    TEST_CASE("Test Concurrent Create and Read") {
        KVDataStore kvStore("datastore.json");
        std::thread writer([&]() {
            kvStore.create("key1", { {"name", "Alice"} });
        });

        std::thread reader([&]() {
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
            CHECK(kvStore.read("key1") == "{\"name\":\"Alice\"}");
        });

        writer.join();
        reader.join();
    }

    TEST_CASE("Test DB Close") {
        KVDataStore kvStore("datastore.json");
        kvStore.create("key1", { {"name", "Alice"} });
        delete &kvStore;  // Triggers saveToFile()
        KVDataStore reloadedStore("datastore.json");
        CHECK(reloadedStore.read("key1") == "{\"name\":\"Alice\"}");
    }

    TEST_CASE("Test Error Handling Duplicate Keys") {
        KVDataStore kvStore("datastore.json");
        kvStore.create("key1", { {"name", "Alice"} });
        std::string result = kvStore.create("key1", { {"name", "Duplicate"} });
        CHECK(result == "Error: Key already exists.");
    }
}


