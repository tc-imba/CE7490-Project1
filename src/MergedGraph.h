//
// Created by liu on 21/10/2020.
//

#ifndef SOCIAL_NETWORK_MERGEDGRAPH_H
#define SOCIAL_NETWORK_MERGEDGRAPH_H

#include <Snap.h>
#include <vector>
#include <set>
#include <random>
#include <algorithm>
#include <cassert>
#include <iostream>

using namespace std;

class MergedGraph {
public:
    struct Node {
        vector<int> nodeIds;
//        bool processed = false;
//        int primaryServerId = -1;
//        int groupId = -1;
        int internalNum = 0;
        int externalNum = 0;

        void Save(TSOut &SOut) const {}
    };

    typedef TNodeEDatNet<Node, TInt> Graph;
    typedef Graph::TNode GraphNode;

private:
    TPt<Graph> graph;
    vector<int> nodeIds;
    int primaryServerId;

public:
    MergedGraph(int primaryServerId) : primaryServerId(primaryServerId) {
        graph = Graph::New();
    }

    void addNode(int nodeId) {
        Node nodeData = {
                .nodeIds = {nodeId},
//                .primaryServerId = primaryServerId,
//                .groupId = nodeId,
        };
        graph->AddNode(nodeId, nodeData);
        nodeIds.emplace_back(nodeId);
    }

    GraphNode &getNode(int nodeId) {
        const Graph *g = graph();
        const auto &node = g->GetNode(nodeId);
        return const_cast<GraphNode &>(node);
    }

    bool isEdge(int nodeAId, int nodeBId) {
        return graph->IsEdge(nodeAId, nodeBId) || graph->IsEdge(nodeBId, nodeAId);
    }

    void addEdge(int nodeAId, int nodeBId, int weight = 1) {
        if (graph->IsNode(nodeAId) && graph->IsNode(nodeBId) && !isEdge(nodeAId, nodeBId)) {
            graph->AddEdge(nodeAId, nodeBId, weight);
            getNode(nodeAId).GetDat().externalNum += weight;
            getNode(nodeBId).GetDat().externalNum += weight;
        }
    }

    TInt &getEdge(int nodeAId, int nodeBId) {
        if (graph->IsEdge(nodeAId, nodeBId)) {
            return graph->GetEDat(nodeAId, nodeBId);
        } else {
            return graph->GetEDat(nodeBId, nodeAId);
        }
        assert(0);
    }

    void merge(mt19937 &generator) {
//        cout << "server " << primaryServerId << ": ";
//        for (auto nodeId : nodeIds) {
//            cout << nodeId << " ";
//        }
//        cout << endl;
        shuffle(nodeIds.begin(), nodeIds.end(), generator);
        vector<int> neighborIds;
        for (auto nodeId : nodeIds) {
            if (!graph->IsNode(nodeId)) continue;
            auto &node = getNode(nodeId);
            auto &nodeData = node.GetDat();

            // get all neighbors
            neighborIds.clear();
            auto neighborNum = node.GetDeg();
            for (int i = 0; i < neighborNum; i++) {
                auto neighborId = node.GetNbrNId(i);
                neighborIds.emplace_back(neighborId);
            }
            shuffle(neighborIds.begin(), neighborIds.end(), generator);

            for (auto neighborId : neighborIds) {
                auto &neighbor = getNode(neighborId);
                auto &neighborData = neighbor.GetDat();
                double beta = (nodeData.internalNum - nodeData.externalNum) / (double) nodeData.nodeIds.size();

                int sharedNum = getEdge(nodeId, neighborId);
                int newInternalNum = nodeData.internalNum + neighborData.internalNum + sharedNum;
                int newExternalNum = nodeData.externalNum + neighborData.externalNum - 2 * sharedNum;

                double newBeta = (newInternalNum - newExternalNum) /
                                 (double) (nodeData.nodeIds.size() + neighborData.nodeIds.size());
                if (newBeta > beta) {
                    nodeData.internalNum = newInternalNum;
                    nodeData.externalNum = newExternalNum;
                    vector<int> temp;
                    std::merge(nodeData.nodeIds.begin(), nodeData.nodeIds.end(),
                               neighborData.nodeIds.begin(), neighborData.nodeIds.end(),
                               back_inserter(temp));
                    nodeData.nodeIds.swap(temp);
                    int neighborNeighborNum = neighbor.GetDeg();
                    for (int i = 0; i < neighborNeighborNum; i++) {
                        auto neighborNeighborId = neighbor.GetNbrNId(i);
                        if (neighborNeighborId != nodeId) {
                            int weight = getEdge(neighborId, neighborNeighborId);
                            if (!isEdge(nodeId, neighborNeighborId)) {
                                graph->AddEdge(nodeId, neighborNeighborId, weight);
                            } else {
                                getEdge(nodeId, neighborNeighborId) += weight;
                            }
                        }
                    }
                    graph->DelNode(neighborId);
                }
            }
        }
    }

    void finalize(set<int> &singleNodes, vector<vector<int> > &groupedNodes) {
        for (auto node = graph->BegNI(); node != graph->EndNI(); node++) {
//            cout << "(";
            if (node.GetDat().nodeIds.size() == 1) {
                singleNodes.emplace(node.GetDat().nodeIds[0]);
            } else {
                groupedNodes.emplace_back(node.GetDat().nodeIds);
            }
//            for (auto nodeId : node.GetDat().nodeIds) {
//                cout << nodeId << " ";
//            }
//            cout << ") ";
        }
//        cout << endl;
    }

};


#endif //SOCIAL_NETWORK_MERGEDGRAPH_H
