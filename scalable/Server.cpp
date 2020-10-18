//
// Created by liu on 18/10/2020.
//

#include "Server.h"
#include "Manager.h"


void Server::addNode(int nodeId, NodeType type) {
    if (type == NodeType::PRIMARY) {
        primaryNodes.emplace(nodeId);
    }
    if (type == NodeType::PRIMARY || type == NodeType::VIRTUAL_PRIMARY) {
        manager->removeServerFromSet(this);
        ++load;
        manager->addServerToSet(this);
    }
    graph->AddNode(nodeId, Node{type});
}

Server::Node &Server::getNode(int nodeId) {
    return graph->GetNDat(nodeId);
}

void Server::removeNode(int nodeId) {
    auto &node = getNode(nodeId);
    if (node.type == NodeType::PRIMARY) {
        primaryNodes.erase(nodeId);
    }
    if (node.type == NodeType::PRIMARY || node.type == NodeType::VIRTUAL_PRIMARY) {
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
