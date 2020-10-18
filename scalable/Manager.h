//
// Created by liu on 18/10/2020.
//

#ifndef SOCIAL_NETWORK_MANAGER_H
#define SOCIAL_NETWORK_MANAGER_H

#include "Server.h"
#include <memory>
#include <vector>
#include <iostream>
#include <cassert>
#include <limits>

using namespace std;

class Manager {
public:
    struct Node {
        TInt primaryServerId = -1;
        std::vector<int> virtualPrimaryServerIds;

        void Save(TSOut &SOut) const {
        }
    };

    struct SNValue {
        int DSN = 0;
        int PDSN = 0;
    };

    typedef TNodeNet<Node> Graph;
    typedef Graph::TNode GraphNode;
private:
    vector<unique_ptr<Server> > servers;
    set<Server *, Server::Compare> serverSet;
    TPt<Graph> graph;
    size_t virtualPrimaryNum;
    int loadConstraint;
public:
    explicit Manager(size_t serverNum, size_t virtualPrimaryNum, int loadConstraint)
            : virtualPrimaryNum(virtualPrimaryNum), loadConstraint(loadConstraint) {
        assert(serverNum > virtualPrimaryNum);

        graph = TSnap::LoadEdgeList<TPt<TNodeNet<Node> > >("data/facebook.txt", 0, 1);

        for (std::size_t i = 0; i < serverNum; i++) {
            auto server = make_unique<Server>(i, this);
            serverSet.emplace(server.get());
            servers.emplace_back(move(server));
        }
    }

    void removeServerFromSet(Server *server) {
        serverSet.erase(server);
    }

    void addServerToSet(Server *server) {
        serverSet.emplace(server);
    }

    GraphNode &getNode(int nodeId) {
        const Graph *g = graph();
        const auto &node = g->GetNode(nodeId);
        return const_cast<GraphNode &>(node);
    }

    void ensureLocality(int nodeId, int neighborId) {
        auto &node = getNode(nodeId);
        auto &neighbor = getNode(neighborId);

        // skip nodes haven't been added
        if (node.GetDat().primaryServerId < 0 || neighbor.GetDat().primaryServerId < 0) {
            return;
        }

        auto nodeServer = servers[node.GetDat().primaryServerId].get();
        auto neighborServer = servers[neighbor.GetDat().primaryServerId].get();

        if (!nodeServer->hasNode(neighborId)) {
            nodeServer->addNode(neighborId, Server::NodeType::NON_PRIMARY);
        }
        if (!neighborServer->hasNode(nodeId)) {
            neighborServer->addNode(nodeId, Server::NodeType::NON_PRIMARY);
        }
    }

    void shrinkLocality(int nodeId, int neighborId) {
        auto &node = getNode(nodeId);
        auto &neighbor = getNode(neighborId);

        // skip nodes haven't been added
        if (node.GetDat().primaryServerId < 0 || neighbor.GetDat().primaryServerId < 0) {
            return;
        }

        auto nodeServer = servers[node.GetDat().primaryServerId].get();

        // we can only delete non primary node
        assert(nodeServer->hasNode(neighborId));
        if (nodeServer->getNode(neighborId).type != Server::NodeType::NON_PRIMARY) {
            return;
        }

        // if any of neighbor's neighbor except self has a primary copy in the server, we can not delete
        auto neighborNeighborNum = neighbor.GetDeg();
        for (int i = 0; i < neighborNeighborNum; i++) {
            auto neighborNeighborId = neighbor.GetNbrNId(i);
            if (neighborNeighborId == nodeId) continue;
            if (nodeServer->hasNode(neighborNeighborId) &&
                nodeServer->getNode(neighborNeighborId).type == Server::NodeType::PRIMARY) {
                return;
            }
        }

        nodeServer->removeNode(neighborId);
    }

    void addNode(int nodeId) {
        auto &node = getNode(nodeId);

        auto it = serverSet.begin();
        auto primaryServer = *it;
        node.GetDat().primaryServerId = primaryServer->getId();
        for (++it; node.GetDat().virtualPrimaryServerIds.size() < virtualPrimaryNum; ++it) {
            node.GetDat().virtualPrimaryServerIds.emplace_back((*it)->getId());
        }

        primaryServer->addNode(nodeId, Server::NodeType::PRIMARY);
        for (auto virtualPrimaryServerId : node.GetDat().virtualPrimaryServerIds) {
            auto virtualPrimaryServer = servers[virtualPrimaryServerId].get();
            virtualPrimaryServer->addNode(nodeId, Server::NodeType::VIRTUAL_PRIMARY);
        }

        auto neighborNum = node.GetDeg();
        for (int i = 0; i < neighborNum; i++) {
            auto neighborId = node.GetNbrNId(i);
            ensureLocality(nodeId, neighborId);
        }
    }

    void moveNode(int nodeId, int serverBId) {
        auto &node = getNode(nodeId);
        int serverAId = node.GetDat().primaryServerId;

        auto serverA = servers[serverAId].get();
        auto serverB = servers[serverBId].get();

        // clear unused locality on Server A
        auto neighborNum = node.GetDeg();
        for (int i = 0; i < neighborNum; i++) {
            auto neighborId = node.GetNbrNId(i);
            shrinkLocality(nodeId, neighborId);
        }

        if (serverB->hasNode(nodeId) && serverB->getNode(nodeId).type == Server::NodeType::VIRTUAL_PRIMARY) {
            // When a node moves from Server A to Server B, if the
            // Server B holds the virtual primary copy of the node, the primary
            // copy of the node from Server A will be swapped with
            // the virtual copy of the node on Server B in order to ensure
            // that the data availability requirement is maintained.
            serverA->removeNode(nodeId);
            serverB->removeNode(nodeId);
            serverA->addNode(nodeId, Server::NodeType::VIRTUAL_PRIMARY);
            serverB->addNode(nodeId, Server::NodeType::PRIMARY);
        } else {
            serverA->removeNode(nodeId);
            if (serverB->hasNode(nodeId)) {
                assert(serverB->getNode(nodeId).type == Server::NodeType::NON_PRIMARY);
                serverB->removeNode(nodeId);
            }
            serverA->addNode(nodeId, Server::NodeType::NON_PRIMARY);
            serverB->addNode(nodeId, Server::NodeType::PRIMARY);
        }

        // rebuild locality on Server B
        node.GetDat().primaryServerId = serverBId;
        for (int i = 0; i < neighborNum; i++) {
            auto neighborId = node.GetNbrNId(i);
            ensureLocality(nodeId, neighborId);
        }
    }

    // is vi Same Side Neighbor of vj
    bool isSSN(GraphNode &vj, GraphNode &vi) {
        return vj.GetDat().primaryServerId == vi.GetDat().primaryServerId &&
               graph->IsEdge(vj.GetId(), vi.GetId());
    }

    // is vi Pure Same Side Neighbor of vj
    bool isPSSN(GraphNode &vj, GraphNode &vi) {
        if (!isSSN(vj, vi)) return false;
        auto neighborNum = vi.GetDeg();
        for (int i = 0; i < neighborNum; i++) {
            auto neighborId = vi.GetNbrNId(i);
            if (neighborId == vj.GetId()) continue;
            auto &neighbor = getNode(neighborId);
            if (neighbor.GetDat().primaryServerId != vj.GetDat().primaryServerId) return false;
        }
        return true;
    }

    // is vi Different Side Neighbor of vj
    bool isDSN(GraphNode &vj, GraphNode &vi) {
        return vj.GetDat().primaryServerId != vi.GetDat().primaryServerId &&
               graph->IsEdge(vj.GetId(), vi.GetId());
    }

    // is vi Pure Different Side Neighbor of vj
    bool isPDSN(GraphNode &vj, GraphNode &vi) {
        if (!isDSN(vj, vi)) return false;
        auto neighborNum = vi.GetDeg();
        for (int i = 0; i < neighborNum; i++) {
            auto neighborId = vi.GetNbrNId(i);
            if (neighborId == vj.GetId()) continue;
            auto &neighbor = getNode(neighborId);
            if (neighbor.GetDat().primaryServerId == vj.GetDat().primaryServerId) return false;
        }
        return true;
    }

    pair<int, int> findMaxSCB(int nodeId, int targetServer = -1) {
        auto &node = getNode(nodeId);
        auto neighborNum = node.GetDeg();
        int serverAId = node.GetDat().primaryServerId;
        assert(targetServer != serverAId);

        vector<SNValue> values(servers.size());
        int SSN = 0;
        int PSSN = 0;
        SNValue total;

        for (int i = 0; i < neighborNum; i++) {
            auto neighborId = node.GetNbrNId(i);
            auto &neighbor = getNode(neighborId);
            int serverId = neighbor.GetDat().primaryServerId;
            if (serverId >= 0) {
                values[serverId].DSN += (int) isDSN(node, neighbor);
                values[serverId].PDSN += (int) isPDSN(node, neighbor);
                SSN += (int) isSSN(node, neighbor);
                PSSN += (int) isPSSN(node, neighbor);
                total.DSN += values[serverId].DSN;
                total.PDSN += values[serverId].PDSN;
            }
        }

        int maxSCBServerId = serverAId;
        int maxSCB = numeric_limits<int>::min();
        int serverBId = targetServer > 0 ? targetServer : 0;
        for (; serverBId < servers.size(); serverBId++) {
            if (serverBId == serverAId) continue;
            int SCB = 0;
            int PDSN_B = values[serverAId].PDSN;
            int PDSN_AB = total.PDSN - values[serverAId].PDSN - values[serverBId].PDSN;
            int DSN_AB = total.DSN - values[serverAId].DSN - values[serverBId].DSN;
            SCB += PDSN_B;
            SCB += PDSN_AB;
            SCB -= PSSN;
            SCB -= DSN_AB;
            if (values[serverBId].DSN > 0) SCB += 1;
            if (SSN > 0) SCB -= 1;
            if (SCB > maxSCB) {
                maxSCB = SCB;
                maxSCBServerId = serverBId;
            }
            if (targetServer > 0) break;
        }

        assert(maxSCBServerId != serverAId);
        return make_pair(maxSCB, maxSCBServerId);
    }

    void reallocateNode(int nodeId, int SCB, int serverBId) {
        auto &node = getNode(nodeId);
        int serverAId = node.GetDat().primaryServerId;
        assert(serverAId != serverBId);

        auto serverA = servers[serverAId].get();
        auto serverB = servers[serverBId].get();
        assert(serverA->getNode(nodeId).type == Server::NodeType::PRIMARY);

        auto serverALoad = serverA->getLoad();
        auto serverBLoad = serverB->getLoad();
        if (!serverB->hasNode(nodeId) || serverB->getNode(nodeId).type == Server::NodeType::NON_PRIMARY) {
            --serverALoad;
            ++serverBLoad;
        }

        if (abs(serverALoad - serverBLoad) <= loadConstraint) {
            // The node is moved to Server B if it would not violate the
            // load balance constraint.
            moveNode(nodeId, serverBId);
        } else {
            // Otherwise, the algorithm tries to swap the node vi with
            // another node on Server B.
            int maxSCBNodeId = -1;
            int maxSCB = numeric_limits<int>::min();;
            for (auto serverBNodeId : serverB->getPrimaryNodes()) {
                auto p = findMaxSCB(serverBNodeId);
                if (p.first > maxSCB) {
                    maxSCB = p.first;
                    maxSCBNodeId = serverBNodeId;
                }
            }
            // If the sum of the SCBs of the two
            // nodes, i.e., vi (to be moved from A to B) and vj (to be moved
            // from B to A) is positive, they are swapped.
            if (maxSCBNodeId >= 0 && SCB + maxSCB > 0) {
                assert(serverB->getNode(maxSCBNodeId).type == Server::NodeType::PRIMARY);
                moveNode(nodeId, serverBId);
                moveNode(maxSCBNodeId, serverAId);
            }
        }
    }

    int computeInterServerCost() {
        int cost = 0;
        for (auto &server : servers) {
            cost += server->computeInterServerCost();
        }
        return cost;
    }


    void run() {
        int i = 0;
        for (auto node = graph->BegNI(); node != graph->EndNI(); node++) {
            int nodeId = node.GetId();
            addNode(nodeId);
            auto p = findMaxSCB(nodeId);
            int maxSCB = p.first;
            int maxSCBServerId = p.second;
            if (maxSCB > 0 && maxSCBServerId >= 0) {
                reallocateNode(nodeId, maxSCB, maxSCBServerId);
            }
            if (++i % 256 == 0) {
                int cost = computeInterServerCost();
                cout << i << " " << cost << endl;
            }
        }
        int cost = computeInterServerCost();
        cout << i << " " << cost << endl;
    }

};

#endif //SOCIAL_NETWORK_MANAGER_H
