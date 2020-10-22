//
// Created by liu on 22/10/2020.
//

#include <cstddef> /* NULL */
#include <metis.h>
#include <iostream>
#include <vector>
#include <Snap.h>
#include <cassert>
#include <unordered_map>

using namespace std;

// Install metis from:
// http://glaros.dtc.umn.edu/gkhome/fetch/sw/metis/metis-5.1.0.tar.gz

// Build with
// g++ metis.cc -lmetis

int main() {
    string dataFile = "data/facebook.txt";
    auto rawGraph = TSnap::LoadEdgeList<TPt<TUNGraph>>(dataFile.c_str(), 0, 1);

    idx_t nVertices = rawGraph->GetNodes();
    idx_t nEdges = rawGraph->GetEdges();
    idx_t nWeights = 1;
    idx_t nParts = 128;

    idx_t objval;
    vector<idx_t> part(nVertices);
//    idx_t part[nVertices];


    vector<idx_t> xadj;
    xadj.reserve(nVertices + 1);
    vector<idx_t> adjncy;
    adjncy.reserve(nEdges * 2);

    idx_t current = 0;
    unordered_map<int, int> nodeIdMap, nodeIdReverseMap;

    int nodeIdCount = 0;
    for (auto node = rawGraph->BegNI(); node != rawGraph->EndNI(); node++) {
        int nodeId = node.GetId();
        nodeIdReverseMap.emplace(nodeIdCount, nodeId);
        nodeIdMap.emplace(nodeId, nodeIdCount++);
    }

    for (auto node = rawGraph->BegNI(); node != rawGraph->EndNI(); node++) {
        int nodeId = nodeIdMap[node.GetId()];
        xadj.emplace_back(current);
        auto neighborNum = node.GetDeg();
        for (int i = 0; i < neighborNum; i++) {
            int neighborId = nodeIdMap[node.GetNbrNId(i)];
            if (nodeId == neighborId) continue;
            adjncy.emplace_back(neighborId);
            current++;
        }
    }
    xadj.emplace_back(current);

    assert(xadj.size() == nVertices + 1);
    assert(adjncy.size() == nEdges * 2);


    // Indexes of starting points in adjacent array
//    idx_t xadj[nVertices+1] = {0,2,5,7,9,12,14};

    // Adjacent vertices in consecutive index order
//    idx_t adjncy[2 * nEdges] = {1,3,0,4,2,1,5,0,4,3,1,5,4,2};

    // Weights of vertices
    // if all weights are equal then can be set to NULL
//    idx_t vwgt[nVertices * nWeights];


    // int ret = METIS_PartGraphRecursive(&nVertices,& nWeights, xadj, adjncy,
    // 				       NULL, NULL, NULL, &nParts, NULL,
    // 				       NULL, NULL, &objval, part);

    int ret = METIS_PartGraphKway(&nVertices, &nWeights, xadj.data(), adjncy.data(),
                                  NULL, NULL, NULL, &nParts, NULL,
                                  NULL, NULL, &objval, part.data());

    std::cout << ret << std::endl;

    for (unsigned part_i = 0; part_i < nVertices; part_i++) {
        std::cout << part_i << " " << part[part_i] << std::endl;
    }


    return 0;
}