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

    struct SCBValue {
        int PDSN_B = 0;
        int PDSN_AB = 0;
        int PSSN = 0;
        int DSN_AB = 0;
        int bonus = 0;
        int penalty = 0;
        int value = 0;
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

    pair<int, int> ensureLocality(int nodeId, int neighborId) {
        auto &node = getNode(nodeId);
        auto &neighbor = getNode(neighborId);
        int deltaA = 0, deltaB = 0;

        // skip nodes haven't been added
        if (node.GetDat().primaryServerId < 0 || neighbor.GetDat().primaryServerId < 0) {
            return make_pair(deltaA, deltaB);
        }

        auto nodeServer = servers[node.GetDat().primaryServerId].get();
        auto neighborServer = servers[neighbor.GetDat().primaryServerId].get();

        if (!nodeServer->hasNode(neighborId)) {
            nodeServer->addNode(neighborId, Server::NodeType::NON_PRIMARY);
            ++deltaA;
        }
        if (!neighborServer->hasNode(nodeId)) {
            neighborServer->addNode(nodeId, Server::NodeType::NON_PRIMARY);
            ++deltaB;
        }
        return make_pair(deltaA, deltaB);
    }

    pair<int, int> shrinkLocality(int nodeId, int neighborId) {
        auto &node = getNode(nodeId);
        auto &neighbor = getNode(neighborId);

        // skip nodes haven't been added
        if (node.GetDat().primaryServerId < 0 || neighbor.GetDat().primaryServerId < 0) {
            return make_pair(0, 0);
        }

        auto nodeServer = servers[node.GetDat().primaryServerId].get();

        // we can only delete non primary node
        assert(nodeServer->hasNode(neighborId));
        if (nodeServer->getNode(neighborId).type != Server::NodeType::NON_PRIMARY) {
            return make_pair(0, 0);
        }

        // if any of neighbor's neighbor except self has a primary copy in the server, we can not delete
        auto neighborNeighborNum = neighbor.GetDeg();
        for (int i = 0; i < neighborNeighborNum; i++) {
            auto neighborNeighborId = neighbor.GetNbrNId(i);
            if (neighborNeighborId == nodeId) continue;
            if (nodeServer->hasNode(neighborNeighborId) &&
                nodeServer->getNode(neighborNeighborId).type == Server::NodeType::PRIMARY) {
                return make_pair(0, 0);
            }
        }

        nodeServer->removeNode(neighborId);
        return make_pair(-1, 0);
    }

    void addNode(int nodeId) {
        auto &node = getNode(nodeId);

        auto it = serverSet.begin();
        auto primaryServer = *it;
        node.GetDat().primaryServerId = primaryServer->getId();
        for (++it; node.GetDat().virtualPrimaryServerIds.size() < virtualPrimaryNum; ++it) {
            node.GetDat().virtualPrimaryServerIds.emplace_back((*it)->getId());
        }
#ifndef NDEBUG
        //        cout << "--- add node " << nodeId << " (" << primaryServer->getId() <<  ") ---" << endl;
#endif
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

    pair<int, int> moveNode(int nodeId, int serverBId) {
        auto &node = getNode(nodeId);
        int serverAId = node.GetDat().primaryServerId;
#ifndef NDEBUG
        //        cout << "--- move node " << nodeId << " (" << serverAId << " -> " << serverBId << ") ---" << endl;
#endif
        auto serverA = servers[serverAId].get();
        auto serverB = servers[serverBId].get();

        int deltaA = 0, deltaB = 0;

        // clear unused locality on Server A
        auto neighborNum = node.GetDeg();
        for (int i = 0; i < neighborNum; i++) {
            auto neighborId = node.GetNbrNId(i);
            auto p = shrinkLocality(nodeId, neighborId);
            deltaA += p.first;
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
            ++deltaA;
            --deltaB;
        } else {
            serverA->removeNode(nodeId);
            if (serverB->hasNode(nodeId)) {
                assert(serverB->getNode(nodeId).type == Server::NodeType::NON_PRIMARY);
                serverB->removeNode(nodeId);
                --deltaB;
            }
//            serverA->addNode(nodeId, Server::NodeType::NON_PRIMARY);
            serverB->addNode(nodeId, Server::NodeType::PRIMARY);
        }

        // rebuild locality on Server B
        node.GetDat().primaryServerId = serverBId;
        for (int i = 0; i < neighborNum; i++) {
            auto neighborId = node.GetNbrNId(i);
            auto p = ensureLocality(nodeId, neighborId);
            deltaA += p.second;
            deltaB += p.first;
        }
        return make_pair(deltaA, deltaB);
    }

    // is vi Same Side Neighbor of vj
    bool isSSN(GraphNode &vj, GraphNode &vi) {
        return vj.GetDat().primaryServerId == vi.GetDat().primaryServerId;
    }

    // is vi Pure Same Side Neighbor of vj (no neighbor on Server B)
    bool isPSSN(GraphNode &vj, GraphNode &vi, int serverBId) {
        if (!isSSN(vj, vi)) return false;
        auto neighborNum = vi.GetDeg();
        for (int i = 0; i < neighborNum; i++) {
            auto neighborId = vi.GetNbrNId(i);
            if (neighborId == vj.GetId()) continue;
            auto &neighbor = getNode(neighborId);
            if (neighbor.GetDat().primaryServerId < 0) continue;
            if (neighbor.GetDat().primaryServerId == serverBId) return false;
        }
        return true;
    }

    // is vi Different Side Neighbor of vj
    bool isDSN(GraphNode &vj, GraphNode &vi) {
        return vj.GetDat().primaryServerId != vi.GetDat().primaryServerId;
    }

    bool isDSN(GraphNode &vj, GraphNode &vi, int serverBId) {
        if (!isDSN(vj, vi)) return false;
        auto neighborNum = vi.GetDeg();
        for (int i = 0; i < neighborNum; i++) {
            auto neighborId = vi.GetNbrNId(i);
            if (neighborId == vj.GetId()) continue;
            auto &neighbor = getNode(neighborId);
            if (neighbor.GetDat().primaryServerId < 0) continue;
            if (neighbor.GetDat().primaryServerId == serverBId) return false;
        }
        return true;
    }

    // is vi Pure Different Side Neighbor of vj
    bool isPDSN(GraphNode &vj, GraphNode &vi) {
        if (!isDSN(vj, vi)) return false;
        auto neighborNum = vi.GetDeg();
        for (int i = 0; i < neighborNum; i++) {
            auto neighborId = vi.GetNbrNId(i);
            if (neighborId == vj.GetId()) continue;
            auto &neighbor = getNode(neighborId);
            if (neighbor.GetDat().primaryServerId < 0) continue;
            if (neighbor.GetDat().primaryServerId == vj.GetDat().primaryServerId) return false;
        }
        return true;
    }

    SCBValue calculateSCB(int nodeId, int serverBId, vector<int> &PDSNs, int totalPDSN) {
        auto &node = getNode(nodeId);
        auto neighborNum = node.GetDeg();
        int serverAId = node.GetDat().primaryServerId;
        assert(serverAId != serverBId);
        auto serverA = servers[serverAId].get();
        auto serverB = servers[serverBId].get();

        SCBValue SCB;

        SCB.PDSN_B += PDSNs[serverBId];
        SCB.PDSN_AB += totalPDSN - PDSNs[serverAId] - PDSNs[serverBId];

        // if serverB has virtual primary nodeA, they will be swapped
        // so serverA has one more virtual primary node (penalty = -1)
        // and serverB has one less virtual primary node (bonus = 1)
        if (serverB->hasNode(nodeId) && serverB->getNode(nodeId).type == Server::NodeType::VIRTUAL_PRIMARY) {
            SCB.penalty = -1;
            SCB.bonus = 1;
        }

        for (int i = 0; i < neighborNum; i++) {
            auto neighborId = node.GetNbrNId(i);
            assert(graph->IsEdge(nodeId, neighborId) || graph->IsEdge(neighborId, nodeId));
            auto &neighbor = getNode(neighborId);
            int neighborServerId = neighbor.GetDat().primaryServerId;

            if (neighborServerId >= 0) {
                /*if (!PDSNs && neighborServerId == serverBId &&
                    serverA->getNode(neighborId).type == Server::NodeType::NON_PRIMARY) {
                    SCB.PDSN_B += (int) isPDSN(node, neighbor);
                }
                if (!PDSNs && neighborServerId != serverAId && neighborServerId != serverBId &&
                    serverA->getNode(neighborId).type == Server::NodeType::NON_PRIMARY) {
                    SCB.PDSN_AB += (int) isPDSN(node, neighbor);
                }*/
                if (neighborServerId == serverAId) {
                    SCB.PSSN += (int) isPSSN(node, neighbor, serverBId);
                }
                if (neighborServerId != serverAId && neighborServerId != serverBId) {
                    SCB.DSN_AB += (int) isDSN(node, neighbor, serverBId);
                }
                if (SCB.bonus == 0 && neighborServerId == serverBId) {
                    SCB.bonus = 1;
                }
                if (SCB.penalty == 0 && neighborServerId == serverAId) {
                    SCB.penalty = -1;
                }
            }
        }

        SCB.value = SCB.PDSN_B + SCB.PDSN_AB - SCB.PSSN - SCB.DSN_AB + SCB.bonus + SCB.penalty;
        return SCB;
    }

    pair<SCBValue, int> findMaxSCB(int nodeId, int targetServer = -1) {
        auto &node = getNode(nodeId);
        auto neighborNum = node.GetDeg();
        int serverAId = node.GetDat().primaryServerId;
        assert(targetServer != serverAId);
        auto serverA = servers[serverAId].get();

        vector<int> PDSNs(servers.size());
        int totalPDSN = 0;
//        int SSN = 0;
//        int PSSN = 0;
//        SNValue total;

        for (int i = 0; i < neighborNum; i++) {
            auto neighborId = node.GetNbrNId(i);
//            assert(graph->IsEdge(nodeId, neighborId) || graph->IsEdge(neighborId, nodeId));
            auto &neighbor = getNode(neighborId);
            int serverBId = neighbor.GetDat().primaryServerId;
            if (serverBId >= 0) {
                if (serverA->getNode(neighborId).type == Server::NodeType::NON_PRIMARY && isPDSN(node, neighbor))  {
                    PDSNs[serverBId] += 1;
                    totalPDSN += 1;
                }
//                int DSN = (int) isDSN(node, neighbor);
//                int PDSN = (int) isPDSN(node, neighbor);
//                values[serverBId].DSN += DSN;
//                values[serverBId].PDSN += PDSN;
//                SSN += (int) isSSN(node, neighbor);
//                PSSN += (int) isPSSN(node, neighbor);
//                total.DSN += DSN;
//                total.PDSN += PDSN;
            }
        }

        int maxSCBServerId = serverAId;
        SCBValue maxSCB;
        maxSCB.value = numeric_limits<int>::min();
        int serverBId = targetServer >= 0 ? targetServer : 0;
        for (; serverBId < servers.size(); serverBId++) {
            if (serverBId == serverAId) continue;
            SCBValue SCB = calculateSCB(nodeId, serverBId, PDSNs, totalPDSN);
/*//            int SCB = 0;
            SCB.PDSN_B = values[serverBId].PDSN;
            SCB.PDSN_AB = total.PDSN - values[serverAId].PDSN - values[serverBId].PDSN;
            SCB.DSN_AB = total.DSN - values[serverAId].DSN - values[serverBId].DSN;
            SCB.PSSN = PSSN;
            SCB.bonus = (int) (values[serverBId].DSN > 0);
            SCB.penalty = -(int) (SSN > 0);
            SCB.value = SCB.PDSN_B + SCB.PDSN_AB - SCB.PSSN - SCB.DSN_AB + SCB.bonus + SCB.penalty;*/
            if (SCB.value > maxSCB.value) {
                maxSCB = SCB;
                maxSCBServerId = serverBId;
            }
            if (targetServer >= 0) break;
        }

        assert(maxSCBServerId != serverAId);
        return make_pair(maxSCB, maxSCBServerId);
    }

    void reallocateNode(int nodeId, SCBValue SCB, int serverBId) {
        auto &node = getNode(nodeId);
        int serverAId = node.GetDat().primaryServerId;
        assert(serverAId != serverBId);
#ifndef NDEBUG
        //        cout << "--- reallocate node " << nodeId << " (" << serverAId << " -> " << serverBId << ") ---" << endl;
#endif
        auto serverA = servers[serverAId].get();
        auto serverB = servers[serverBId].get();
        assert(serverA->getNode(nodeId).type == Server::NodeType::PRIMARY);

        auto serverALoad = serverA->getLoad();
        auto serverBLoad = serverB->getLoad();
        if (!serverB->hasNode(nodeId) || serverB->getNode(nodeId).type == Server::NodeType::NON_PRIMARY) {
            --serverALoad;
            ++serverBLoad;
        }

//        int cost1 = computeInterServerCost();

        if (abs(serverALoad - serverBLoad) <= loadConstraint) {
            // The node is moved to Server B if it would not violate the
            // load balance constraint.
            auto p1 = moveNode(nodeId, serverBId);

/*            int cost2 = computeInterServerCost();
            cout << "move " << cost1 - cost2 << " " << SCB.value << endl;
            cout << SCB.PDSN_AB << " " << SCB.PDSN_B << " " << -SCB.penalty << " " << p1.first << endl;
            cout << SCB.DSN_AB << " " << SCB.PSSN << " " << -SCB.bonus << " " << p1.second << endl;*/

        } else {
            // Otherwise, the algorithm tries to swap the node vi with
            // another node on Server B.
            auto p1 = moveNode(nodeId, serverBId);

            int maxSCBNodeId = -1;
            SCBValue maxSCB;
            maxSCB.value = numeric_limits<int>::min();;
            for (auto serverBNodeId : serverB->getPrimaryNodes()) {
                auto p = findMaxSCB(serverBNodeId, serverAId);
                SCBValue tempSCB = p.first;
                if (tempSCB.value > maxSCB.value) {
                    maxSCB = tempSCB;
                    maxSCBNodeId = serverBNodeId;
                }
            }
            // If the sum of the SCBs of the two
            // nodes, i.e., vi (to be moved from A to B) and vj (to be moved
            // from B to A) is positive, they are swapped.
            auto p2 = p1;
            if (maxSCBNodeId >= 0 && SCB.value + maxSCB.value > 0) {
                assert(serverB->getNode(maxSCBNodeId).type == Server::NodeType::PRIMARY);
                p2 = moveNode(maxSCBNodeId, serverAId);
            } else {
                p2 = moveNode(nodeId, serverAId);
            }
/*            int cost2 = computeInterServerCost();
            if (maxSCBNodeId >= 0 && SCB.value + maxSCB.value > 0) {
                cout << "swap " << cost1 << " " << cost2 << " " << SCB.value << " " << maxSCB.value << endl;
                cout << SCB.PDSN_AB << " " << SCB.PDSN_B << " " << SCB.penalty << " " << p1.first << endl;
                cout << -SCB.DSN_AB << " " << -SCB.PSSN << " " << SCB.bonus << " " << p1.second << endl;
                cout << maxSCB.PDSN_AB << " " << maxSCB.PDSN_B << " " << maxSCB.penalty << " " << p2.first << endl;
                cout << -maxSCB.DSN_AB << " " << -maxSCB.PSSN << " " << maxSCB.bonus << " " << p2.second << endl;
            } else {
                cout << "move back " << cost1 << " " << cost2 << " " << SCB.value << " " << maxSCB.value << endl;
            }
            move(0);*/
        }


    }

    void validate() {
        for (auto &server : servers) {
            server->validate();
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
//            validate();

            // offline algorithm
            auto p = findMaxSCB(nodeId);
            SCBValue maxSCB = p.first;
            int maxSCBServerId = p.second;
            if (maxSCB.value > 0 && maxSCBServerId >= 0) {
                reallocateNode(nodeId, maxSCB, maxSCBServerId);
            }
//            validate();


/*            if (++i % 256 == 0) {
                int cost = computeInterServerCost();
                cout << i << " " << cost << endl;
            }*/
        }
        int cost = computeInterServerCost();
        cout << i << " " << cost << endl;
    }

};

#endif //SOCIAL_NETWORK_MANAGER_H
