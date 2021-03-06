//
// Created by liu on 18/10/2020.
//

#include "Server.h"
#include "Manager.h"
#include "MergedGraph.h"

#include <algorithm>

const char *Server::NodeTypeString[3] = {
        "PRIMARY",
        "VIRTUAL_PRIMARY",
        "NON_PRIMARY",
};

void Server::addNode(int nodeId, NodeType type) {
#ifndef NDEBUG
//        cout << "server " << id << ": add node " << nodeId << " (" << NodeTypeString[(int) type] << ")" << endl;
#endif
    if (type == NodeType::PRIMARY || type == NodeType::VIRTUAL_PRIMARY) {
        if (type == NodeType::PRIMARY) {
            primaryNodes.emplace(nodeId);
        } else {
            virtualPrimaryNodes.emplace(nodeId);
        }
        manager->removeServerFromSet(this);
        ++load;
        manager->addServerToSet(this);
    }
    graph->AddNode(nodeId, Node{type});
}

Server::Node &Server::getNode(int nodeId) {
    return graph->GetNDat(nodeId);
}

void Server::mergeNodes(mt19937 &generator) {
    MergedGraph mergedGraph(id);
    for (auto nodeId : primaryNodes) {
        mergedGraph.addNode(nodeId);
    }
    for (auto nodeId : primaryNodes) {
        auto &node = manager->getNode(nodeId);
        auto neighborNum = node.GetDeg();
        for (int i = 0; i < neighborNum; i++) {
            auto neighborId = node.GetNbrNId(i);
            mergedGraph.addEdge(nodeId, neighborId);
        }
    }
    mergedGraph.merge(generator);
    mergedGraph.finalize(singleNodes, groupedNodes);
}


void Server::removeNode(int nodeId) {
    auto &node = getNode(nodeId);
#ifndef NDEBUG
//        cout << "server " << id << ": remove node " << nodeId << " (" << NodeTypeString[(int) node.type] << ")" << endl;
#endif
    if (node.type == NodeType::PRIMARY || node.type == NodeType::VIRTUAL_PRIMARY) {
        if (node.type == NodeType::PRIMARY) {
            primaryNodes.erase(nodeId);
        } else {
            virtualPrimaryNodes.erase(nodeId);
        }
        manager->removeServerFromSet(this);
        --load;
        manager->addServerToSet(this);
    }
    graph->DelNode(nodeId);
}

bool Server::hasNode(int nodeId) const {
    return graph->IsNode(nodeId);
}

int Server::getLoad() const {
    return load;
}

int Server::getId() const {
    return id;
}

const set<int> &Server::getPrimaryNodes() const {
    return primaryNodes;
}

const set<int> &Server::getVirtualPrimaryNodes() const {
    return virtualPrimaryNodes;
}

int Server::computeInterServerCost() const {
    return graph->GetNodes() - (int) primaryNodes.size();
}

void Server::validate() {
    for (auto node = graph->BegNI(); node != graph->EndNI(); node++) {
        auto neighborNum = node.GetDeg();
        for (int i = 0; i < neighborNum; i++) {
            auto neighborId = node.GetNbrNId(i);
            const auto &neighbor = ((const TNodeNet<Node> *) graph())->GetNode(neighborId);
            bool flag = false;
            if (neighbor.GetDat().type == NodeType::NON_PRIMARY) {
                auto neighborNeighborNum = neighbor.GetDeg();
                for (int j = 0; j < neighborNeighborNum; j++) {
                    auto neighborNeighborId = neighbor.GetNbrNId(j);
                    if (graph->GetNDat(neighborNeighborId).type == NodeType::PRIMARY) {
                        flag = true;
                        break;
                    }
                }
            }
            if (!flag) {
                cout << "validation failed" << endl;
                exit(-1);
            }
        }
    }
}

set<int> &Server::getSingleNodes() {
    return singleNodes;
}

vector<vector<int> > &Server::getGroupedNodes() {
    return groupedNodes;
}
