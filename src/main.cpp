#include "Manager.h"

#include <getopt.h>
#include <iostream>
#include <algorithm>

using namespace std;

struct Options {
    string dataFile = "data/facebook.txt";
    Manager::Algorithm algorithm = Manager::Algorithm::RANDOM;
    size_t serverNum = 128;
    size_t virtualPrimaryNum = 3;
    int loadConstraint = 1;
    size_t nodeNum = 0;
};

Options parseOptions(int argc, char **argv) {
    const static char *optstring = "d:a:s:k:l:n:";
    const static option long_options[] = {
            {"data",      optional_argument, nullptr, 'd'},
            {"algorithm", optional_argument, nullptr, 'a'},
            {"server",    optional_argument, nullptr, 's'},
            {"replica",   optional_argument, nullptr, 'k'},
            {"load",      optional_argument, nullptr, 'l'},
            {"node",      optional_argument, nullptr, 'n'},
            {nullptr, 0,                     nullptr, 0}
    };
    int opt, option_index = 0;
    Options options;
    while ((opt = getopt_long(argc, argv, optstring, long_options, &option_index)) != -1) {
        switch (opt) {
            case 'd':
                options.dataFile = optarg;
                break;
            case 'a': {
                string algorithm = optarg;
                transform(algorithm.begin(), algorithm.end(), algorithm.begin(),
                          [](unsigned char c) { return std::tolower(c); });
                if (algorithm == "random") {
                    options.algorithm = Manager::Algorithm::RANDOM;
                } else if (algorithm == "spar") {
                    options.algorithm = Manager::Algorithm::SPAR;
                } else if (algorithm == "online") {
                    options.algorithm = Manager::Algorithm::ONLINE;
                } else if (algorithm == "offline") {
                    options.algorithm = Manager::Algorithm::OFFLINE;
                } else if (algorithm == "metis") {
                    options.algorithm = Manager::Algorithm::METIS;
                } else {
                    assert(0);
                }
                break;
            }
            case 's':
                options.serverNum = strtoul(optarg, nullptr, 10);
                break;
            case 'k':
                options.virtualPrimaryNum = strtoul(optarg, nullptr, 10);
                break;
            case 'l':
                options.loadConstraint = (int) strtol(optarg, nullptr, 10);
                break;
            case 'n':
                options.nodeNum = strtoul(optarg, nullptr, 10);
                break;
            default:
                std::cerr << "Unrecognized option" << std::endl;
                assert(0);
        }
    }
    return options;
}

int main(int argc, char *argv[]) {
    auto options = parseOptions(argc, argv);

    Manager manager(options.dataFile, options.algorithm, options.serverNum, options.virtualPrimaryNum,
                    options.loadConstraint, options.nodeNum);
    manager.run();

    return 0;
}
