#include "query5.hpp"
#include <iostream>
#include <fstream>
#include <sstream>
#include <thread>
#include <mutex>
#include <algorithm>
#include <iomanip>
#include <cstring>

// Helper to split a string by a delimiter (needed for parsing the | separated files)
std::vector<std::string> split(const std::string& s, char delimiter) {
    std::vector<std::string> tokens;
    std::string token;
    std::istringstream tokenStream(s);
    while (std::getline(tokenStream, token, delimiter)) {
        tokens.push_back(token);
    }
    return tokens;
}

// Function to parse command line arguments
bool parseArgs(int argc, char* argv[], std::string& r_name, std::string& start_date, std::string& end_date, int& num_threads, std::string& table_path, std::string& result_path) {
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (i + 1 < argc) { // Ensure there is a value after the flag
            if (arg == "--r_name") r_name = argv[++i];
            else if (arg == "--start_date") start_date = argv[++i];
            else if (arg == "--end_date") end_date = argv[++i];
            else if (arg == "--threads") num_threads = std::stoi(argv[++i]);
            else if (arg == "--table_path") table_path = argv[++i];
            else if (arg == "--result_path") result_path = argv[++i];
        }
    }
    
    // Basic validation to ensure we got the necessary args
    if (r_name.empty() || start_date.empty() || end_date.empty() || table_path.empty() || result_path.empty() || num_threads <= 0) {
        return false;
    }
    return true;
}

// Helper function to read a single .tbl file
bool loadTable(const std::string& filepath, std::vector<std::map<std::string, std::string>>& table_data, const std::vector<std::string>& columns) {
    std::ifstream file(filepath);
    if (!file.is_open()) {
        std::cerr << "Error opening file: " << filepath << std::endl;
        return false;
    }

    std::string line;
    while (std::getline(file, line)) {
        if (line.empty()) continue;
        std::vector<std::string> values = split(line, '|');
        if (values.size() < columns.size()) continue; 

        std::map<std::string, std::string> row;
        for (size_t i = 0; i < columns.size(); ++i) {
            row[columns[i]] = values[i];
        }
        table_data.push_back(row);
    }
    return true;
}

// Function to read TPCH data from the specified paths
bool readTPCHData(const std::string& table_path, 
                  std::vector<std::map<std::string, std::string>>& customer_data, 
                  std::vector<std::map<std::string, std::string>>& orders_data, 
                  std::vector<std::map<std::string, std::string>>& lineitem_data, 
                  std::vector<std::map<std::string, std::string>>& supplier_data, 
                  std::vector<std::map<std::string, std::string>>& nation_data, 
                  std::vector<std::map<std::string, std::string>>& region_data) {
    
    // Note: Ensure the column names match exactly what TPC-H provides and what logic expects
    // File names are usually lowercase like region.tbl or uppercase REGION.tbl. Adjust path joining if needed.
    // Using simple path concatenation. Ensure table_path ends with / or add it.
    std::string path_suffix = (table_path.back() == '/' ? "" : "/");

    if (!loadTable(table_path + path_suffix + "customer.tbl", customer_data, {"c_custkey", "c_name", "c_address", "c_nationkey", "c_phone", "c_acctbal", "c_mktsegment", "c_comment"})) return false;
    if (!loadTable(table_path + path_suffix + "orders.tbl", orders_data, {"o_orderkey", "o_custkey", "o_orderstatus", "o_totalprice", "o_orderdate", "o_orderpriority", "o_clerk", "o_shippriority", "o_comment"})) return false;
    if (!loadTable(table_path + path_suffix + "lineitem.tbl", lineitem_data, {"l_orderkey", "l_partkey", "l_suppkey", "l_linenumber", "l_quantity", "l_extendedprice", "l_discount", "l_tax", "l_returnflag", "l_linestatus", "l_shipdate", "l_commitdate", "l_receiptdate", "l_shipinstruct", "l_shipmode", "l_comment"})) return false;
    if (!loadTable(table_path + path_suffix + "supplier.tbl", supplier_data, {"s_suppkey", "s_name", "s_address", "s_nationkey", "s_phone", "s_acctbal", "s_comment"})) return false;
    if (!loadTable(table_path + path_suffix + "nation.tbl", nation_data, {"n_nationkey", "n_name", "n_regionkey", "n_comment"})) return false;
    if (!loadTable(table_path + path_suffix + "region.tbl", region_data, {"r_regionkey", "r_name", "r_comment"})) return false;

    return true;
}

// Function to execute TPCH Query 5 using multithreading
bool executeQuery5(const std::string& r_name, const std::string& start_date, const std::string& end_date, int num_threads, 
                   const std::vector<std::map<std::string, std::string>>& customer_data, 
                   const std::vector<std::map<std::string, std::string>>& orders_data, 
                   const std::vector<std::map<std::string, std::string>>& lineitem_data, 
                   const std::vector<std::map<std::string, std::string>>& supplier_data, 
                   const std::vector<std::map<std::string, std::string>>& nation_data, 
                   const std::vector<std::map<std::string, std::string>>& region_data, 
                   std::map<std::string, double>& results) {

    // 1. Filter Regions (Find Region Key for r_name, e.g., 'ASIA')
    std::vector<std::string> valid_region_keys;
    for (const auto& r : region_data) {
        if (r.at("r_name") == r_name) {
            valid_region_keys.push_back(r.at("r_regionkey"));
        }
    }

    // 2. Filter Nations (Find Nations in those Regions)
    std::map<std::string, std::string> nation_key_to_name; // Map Key -> Name
    for (const auto& n : nation_data) {
        for (const auto& r_key : valid_region_keys) {
            if (n.at("n_regionkey") == r_key) {
                nation_key_to_name[n.at("n_nationkey")] = n.at("n_name");
            }
        }
    }

    // 3. Filter Customers (Find Customers in those Nations)
    // Map CustKey -> NationKey (Only for valid nations)
    std::map<std::string, std::string> valid_customers; 
    for (const auto& c : customer_data) {
        if (nation_key_to_name.find(c.at("c_nationkey")) != nation_key_to_name.end()) {
            valid_customers[c.at("c_custkey")] = c.at("c_nationkey");
        }
    }
    
    // 4. Filter Suppliers (Find Suppliers in those Nations)
    // Map SuppKey -> NationKey
    std::map<std::string, std::string> valid_suppliers;
    for (const auto& s : supplier_data) {
        if (nation_key_to_name.find(s.at("s_nationkey")) != nation_key_to_name.end()) {
            valid_suppliers[s.at("s_suppkey")] = s.at("s_nationkey");
        }
    }

    // 5. Filter Orders (Match valid Customers and Date Range)
    // Map OrderKey -> CustomerKey
    std::map<std::string, std::string> valid_orders;
    for (const auto& o : orders_data) {
        std::string o_date = o.at("o_orderdate");
        // Check date range and if customer is valid
        if (o_date >= start_date && o_date < end_date) {
            std::string c_key = o.at("o_custkey");
            if (valid_customers.find(c_key) != valid_customers.end()) {
                valid_orders[o.at("o_orderkey")] = c_key;
            }
        }
    }

    // 6. Process Lineitems (The heavy lifting - Multithreaded)
    std::vector<std::thread> threads;
    std::vector<std::map<std::string, double>> thread_results(num_threads);
    std::mutex result_mutex;

    size_t total_items = lineitem_data.size();
    size_t chunk_size = total_items / num_threads;

    auto worker = [&](int thread_id, size_t start_idx, size_t end_idx) {
        for (size_t i = start_idx; i < end_idx; ++i) {
            const auto& item = lineitem_data[i];
            std::string o_key = item.at("l_orderkey");
            std::string s_key = item.at("l_suppkey");

            // Check if order is valid
            auto o_it = valid_orders.find(o_key);
            if (o_it != valid_orders.end()) {
                // Check if supplier is valid
                auto s_it = valid_suppliers.find(s_key);
                if (s_it != valid_suppliers.end()) {
                    
                    std::string c_key = o_it->second; // Customer Key from Order
                    std::string c_nation = valid_customers[c_key]; // Nation from Customer
                    std::string s_nation = s_it->second; // Nation from Supplier

                    // Condition: c_nationkey = s_nationkey
                    if (c_nation == s_nation) {
                        double price = std::stod(item.at("l_extendedprice"));
                        double discount = std::stod(item.at("l_discount"));
                        double revenue = price * (1.0 - discount);

                        std::string n_name = nation_key_to_name[s_nation];
                        thread_results[thread_id][n_name] += revenue;
                    }
                }
            }
        }
    };

    // Launch threads
    for (int i = 0; i < num_threads; ++i) {
        size_t start = i * chunk_size;
        size_t end = (i == num_threads - 1) ? total_items : (i + 1) * chunk_size;
        threads.emplace_back(worker, i, start, end);
    }

    // Join threads
    for (auto& t : threads) {
        t.join();
    }

    // Aggregate results
    for (const auto& partial_map : thread_results) {
        for (const auto& pair : partial_map) {
            results[pair.first] += pair.second;
        }
    }

    return true;
}

// Function to output results to the specified path
bool outputResults(const std::string& result_path, const std::map<std::string, double>& results) {
    std::ofstream outfile(result_path);
    if (!outfile.is_open()) return false;

    // Sort by revenue descending (Query requirement)
    // Copy map to vector of pairs for sorting
    std::vector<std::pair<std::string, double>> sorted_results(results.begin(), results.end());
    
    std::sort(sorted_results.begin(), sorted_results.end(), 
        [](const std::pair<std::string, double>& a, const std::pair<std::string, double>& b) {
            return a.second > b.second; // Descending order
        });

    // Write to file
    for (const auto& pair : sorted_results) {
        outfile << pair.first << "|" << std::fixed << std::setprecision(4) << pair.second << std::endl;
    }
    
    outfile.close();
    return true;
}