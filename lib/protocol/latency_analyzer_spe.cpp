#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <sstream>
#include <iomanip>

int main() {
    std::ifstream file("logs/latency_block_spe.log");
    if (!file.is_open()) {
        std::cerr << "无法打开文件 logs/latency_block_spe.log" << std::endl;
        return 1;
    }

    std::string line;
    // 跳过标题行
    std::getline(file, line);

    std::vector<int64_t> cross_chain_latencies;
    int total_tx_count = 0;
    int64_t total_latency = 0;

    // 读取前100笔交易
    while (std::getline(file, line) && total_tx_count < 100) {
        total_tx_count++;
        std::stringstream ss(line);
        std::string item;
        std::vector<std::string> items;

        // 解析CSV行
        while (std::getline(ss, item, ',')) {
            items.push_back(item);
        }

        // 检查是否是跨链交易
        if (items.size() >= 4 && items[3] != "延迟(微秒)" && items[4] == "跨链") {
            int64_t latency = std::stoll(items[3]);
            cross_chain_latencies.push_back(latency);
            total_latency += latency;
        }
    }

    if (!cross_chain_latencies.empty()) {
        std::cout << std::fixed << std::setprecision(0);  // 设置为不使用科学计数法，不显示小数点
        double avg_latency = static_cast<double>(total_latency) / cross_chain_latencies.size();
        std::cout << "在前" << total_tx_count << "笔交易中：" << std::endl;
        std::cout << "跨链交易数量: " << cross_chain_latencies.size() << std::endl;
        std::cout << "跨链交易平均时延: " << avg_latency << " 微秒" << std::endl;
        std::cout << "跨链交易比例: " << (cross_chain_latencies.size() * 100.0 / total_tx_count) << "%" << std::endl;
    } else {
        std::cout << "未找到跨链交易" << std::endl;
    }

    return 0;
} 