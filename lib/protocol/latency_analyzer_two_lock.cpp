#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <sstream>
#include <iomanip>
#include <map>
#include <algorithm>

int main() {
    std::ifstream file("logs/latency_block_two_lock.log");
    if (!file.is_open()) {
        std::cerr << "无法打开文件 logs/latency_block_two_lock.log" << std::endl;
        return 1;
    }

    std::string line;
    // 跳过标题行
    std::getline(file, line);

    std::vector<int64_t> cross_chain_latencies;
    std::map<int, int> latency_distribution;  // 延迟分布 (秒 -> 数量)
    int total_tx_count = 0;
    int64_t total_latency = 0;
    int64_t max_latency = 0;
    int64_t min_latency = INT64_MAX;

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
        if (items.size() >= 5 && items[4] == "跨链") {
            // 使用完成时间减去开始时间计算实际延迟
            int64_t start_time = std::stoll(items[1]);
            int64_t end_time = std::stoll(items[2]);
            int64_t latency = end_time - start_time;
            cross_chain_latencies.push_back(latency);
            total_latency += latency;
            
            // 更新最大最小延迟
            max_latency = std::max(max_latency, latency);
            min_latency = std::min(min_latency, latency);
            
            // 更新延迟分布（按秒统计）
            int latency_seconds = static_cast<int>(latency / 1000000);
            latency_distribution[latency_seconds]++;
        }
    }

    if (!cross_chain_latencies.empty()) {
        std::cout << std::fixed << std::setprecision(2);  // 显示2位小数
        double avg_latency = static_cast<double>(total_latency) / cross_chain_latencies.size();
        std::cout << "在前" << total_tx_count << "笔交易中：" << std::endl;
        std::cout << "跨链交易数量: " << cross_chain_latencies.size() << std::endl;
        std::cout << "跨链交易平均时延: " << avg_latency / 1000000.0 << " 秒" << std::endl;
        std::cout << "跨链交易最小时延: " << min_latency / 1000000.0 << " 秒" << std::endl;
        std::cout << "跨链交易最大时延: " << max_latency / 1000000.0 << " 秒" << std::endl;
        std::cout << "跨链交易比例: " << (cross_chain_latencies.size() * 100.0 / total_tx_count) << "%" << std::endl;
        
        std::cout << "\n延迟分布：" << std::endl;
        for (const auto& [seconds, count] : latency_distribution) {
            std::cout << seconds << "秒: " << count << "笔 (" 
                     << (count * 100.0 / cross_chain_latencies.size()) << "%)" << std::endl;
        }
    } else {
        std::cout << "未找到跨链交易" << std::endl;
    }

    return 0;
} 