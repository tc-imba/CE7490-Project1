//
// Created by liu on 18/10/2020.
//

#ifndef SOCIAL_NETWORK_MANAGER_H
#define SOCIAL_NETWORK_MANAGER_H

#include "Server.h"
#include <metis.h>
#include <memory>
#include <vector>
#include <unordered_map>
#include <iostream>
#include <cassert>
#include <limits>
#include <random>
#include <algorithm>

using namespace std;

class Manager {
public:
    struct Node {
        int primaryServerId = -1;
        int virtualPrimaryNum = 0;

        void Save(TSOut &SOut) const {}
    };

    enum class Algorithm {
        RANDOM,
        SPAR,
        METIS,
        ONLINE,
        OFFLINE
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

    struct SPARValue {
        set<int> removeFromA;
        set<int> removeFromB;
        set<int> addToA;
        set<int> addToB;
        int serverADelta = -1;
        int serverBDelta = 1;
        int cost = 0;
    };

    typedef TNodeNet<Node> Graph;
    typedef Graph::TNode GraphNode;
    typedef vector<int> MergedNode;

    struct MergedNodeCompare {
        bool operator()(const MergedNode &a, const MergedNode &b) {
            if (a.size() != b.size()) return a.size() > b.size();
            return a[0] < b[0];
        }
    };

private:
    vector<unique_ptr<Server> > servers;
    set<Server *, Server::Compare> serverSet;
    TPt<TUNGraph> rawGraph;
    TPt<Graph> graph;
    vector<int> allNodes;
    size_t virtualPrimaryNum;
    int loadConstraint;

    set<MergedNode, MergedNodeCompare> mergedNodes;
    mt19937 randomGenerator;
    Algorithm algorithm;

public:
    explicit Manager(const string &dataFile, Algorithm algorithm, size_t serverNum, size_t virtualPrimaryNum,
                     int loadConstraint)
            : algorithm(algorithm), virtualPrimaryNum(virtualPrimaryNum), loadConstraint(loadConstraint) {
        assert(serverNum > virtualPrimaryNum);

        // initialize (maybe) directed graph as undirected graph
        graph = Graph::New();

        rawGraph = TSnap::LoadEdgeList<TPt<TUNGraph>>(dataFile.c_str(), 0, 1);
        for (auto node = rawGraph->BegNI(); node != rawGraph->EndNI(); node++) {
            int nodeId = node.GetId();
            graph->AddNode(nodeId);
            allNodes.emplace_back(nodeId);
        }
/*        for (auto node = rawGraph->BegNI(); node != rawGraph->EndNI(); node++) {
            auto neighborNum = node.GetDeg();
            for (int i = 0; i < neighborNum; i++) {
                int nodeAId = node.GetId();
                int nodeBId = node.GetNbrNId(i);
                if (nodeAId == nodeBId) continue;
                if (nodeAId > nodeBId) swap(nodeAId, nodeBId);
                if (!graph->IsEdge(nodeAId, nodeBId)) {
                    graph->AddEdge(nodeAId, nodeBId);
                }
            }
        }*/

//        graph = TSnap::LoadEdgeList<TPt<Graph>>(dataFile.c_str(), 0, 1);

        for (size_t i = 0; i < serverNum; i++) {
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

    bool isEdge(int nodeAId, int nodeBId) {
        return graph->IsEdge(nodeAId, nodeBId) || graph->IsEdge(nodeBId, nodeAId);
    }

    SPARValue calculateSPAR(int nodeAId, int nodeBId) {
        auto &nodeA = getNode(nodeAId);
        auto &nodeB = getNode(nodeBId);
        int serverAId = nodeA.GetDat().primaryServerId;
        int serverBId = nodeB.GetDat().primaryServerId;
        auto serverA = servers[serverAId].get();
        auto serverB = servers[serverBId].get();

        SPARValue SPAR;

        int neighborNum = nodeA.GetDeg();

        // first calculate addition of virtual primary nodes
        for (int i = 0; i < neighborNum; i++) {
            int neighborId = nodeA.GetNbrNId(i);
            if (neighborId == nodeBId) continue;
            assert(serverA->hasNode(neighborId));
            if (SPAR.addToA.empty() && serverA->getNode(neighborId).type == Server::NodeType::PRIMARY) {
                SPAR.addToA.emplace(nodeAId);
            }
            if (!serverB->hasNode(neighborId)) {
                SPAR.addToB.emplace(neighborId);
            }
        }

        // then calculate removal of virtual primary nodes
        for (int i = 0; i < neighborNum; i++) {
            int neighborId = nodeA.GetNbrNId(i);
            if (neighborId == nodeBId) continue;
            if (serverA->getNode(neighborId).type == Server::NodeType::VIRTUAL_PRIMARY) {
                auto &neighborNode = getNode(neighborId);
                int neighborNeighborNum = neighborNode.GetDeg();
                int virtualNumAfterRemove = neighborNode.GetDat().virtualPrimaryNum - 1 +
                                            ((int) (SPAR.addToB.find(neighborId) != SPAR.addToB.end()));
                if (virtualNumAfterRemove >= virtualPrimaryNum) {
                    bool flag = true;
                    for (int j = 0; j < neighborNeighborNum; j++) {
                        int neighborNeighborId = neighborNode.GetNbrNId(j);
                        if (neighborNeighborId == nodeAId) continue;
                        if (serverA->hasNode(neighborNeighborId) &&
                            serverA->getNode(neighborNeighborId).type == Server::NodeType::PRIMARY) {
                            flag = false;
                            break;
                        }
                    }
                    if (flag) {
                        SPAR.removeFromA.emplace(neighborId);
                    }
                }
            }
        }
        if (serverB->hasNode(nodeAId)) {
            assert(serverB->getNode(nodeAId).type == Server::NodeType::VIRTUAL_PRIMARY);
            int virtualNumAfterRemove = nodeA.GetDat().virtualPrimaryNum - 1 +
                                        ((int) (SPAR.addToA.find(nodeAId) != SPAR.addToA.end()));
            if (virtualNumAfterRemove >= virtualPrimaryNum) {
                SPAR.removeFromB.emplace(nodeAId);
            }
        }

        SPAR.serverADelta = ((int) SPAR.addToA.size()) - ((int) SPAR.removeFromA.size()) - 1;
        SPAR.serverBDelta = ((int) SPAR.addToB.size()) - ((int) SPAR.removeFromB.size()) + 1;

        int serverALoad = serverA->getLoad() + SPAR.serverADelta;
        int serverBLoad = serverB->getLoad() + SPAR.serverBDelta;

        if (abs(serverALoad - serverBLoad) <= loadConstraint) {
            SPAR.cost = SPAR.serverADelta + SPAR.serverBDelta;
        } else {
            SPAR.cost = numeric_limits<int>::max() / 2;
        }
        return SPAR;
    }

    void applySPAR(const SPARValue &SPAR, int nodeAId, int nodeBId) {
        auto &nodeA = getNode(nodeAId);
        auto &nodeB = getNode(nodeBId);
        int serverAId = nodeA.GetDat().primaryServerId;
        int serverBId = nodeB.GetDat().primaryServerId;
        auto serverA = servers[serverAId].get();
        auto serverB = servers[serverBId].get();

        serverA->removeNode(nodeAId);
        for (auto nodeId : SPAR.removeFromA) {
            serverA->removeNode(nodeId);
            getNode(nodeId).GetDat().virtualPrimaryNum--;
        }
        for (auto nodeId : SPAR.removeFromB) {
            serverB->removeNode(nodeId);
            getNode(nodeId).GetDat().virtualPrimaryNum--;
        }
        for (auto nodeId : SPAR.addToA) {
            serverA->addNode(nodeId, Server::NodeType::VIRTUAL_PRIMARY);
            getNode(nodeId).GetDat().virtualPrimaryNum++;
        }
        for (auto nodeId : SPAR.addToB) {
            serverB->addNode(nodeId, Server::NodeType::VIRTUAL_PRIMARY);
            getNode(nodeId).GetDat().virtualPrimaryNum++;
        }
        serverB->addNode(nodeAId, Server::NodeType::PRIMARY);
        nodeA.GetDat().primaryServerId = serverBId;
    }

    void addEdgeSPAR(int nodeId, int neighborId) {
//        cout << "edge: " << nodeId << " " << neighborId << endl;
        auto &node = getNode(nodeId);
        auto &neighbor = getNode(neighborId);

        // skip nodes haven't been added
        if (node.GetDat().primaryServerId < 0 || neighbor.GetDat().primaryServerId < 0) {
            return;
        }

        auto nodeServer = servers[node.GetDat().primaryServerId].get();
        auto neighborServer = servers[neighbor.GetDat().primaryServerId].get();

        int conf1 = ((int) (!nodeServer->hasNode(neighborId))) + ((int) (!neighborServer->hasNode(nodeId)));

        // checks whether both masters are already
        // co-located with each other or with a master’s slave.
        // If so, no further action is required.
        if (conf1 == 0) {
            return;
        }

        SPARValue conf2 = calculateSPAR(nodeId, neighborId);
        SPARValue conf3 = calculateSPAR(neighborId, nodeId);

        // choose conf 1 if do nothing is better
        if (conf1 <= conf2.cost && conf1 <= conf3.cost) {
            if (!nodeServer->hasNode(neighborId)) {
                nodeServer->addNode(neighborId, Server::NodeType::VIRTUAL_PRIMARY);
                neighbor.GetDat().virtualPrimaryNum++;
            }
            if (!neighborServer->hasNode(nodeId)) {
                neighborServer->addNode(nodeId, Server::NodeType::VIRTUAL_PRIMARY);
                node.GetDat().virtualPrimaryNum++;
            }
            return;
        }

        if (conf2.cost < conf3.cost) {
            applySPAR(conf2, nodeId, neighborId);
        } else {
            applySPAR(conf3, neighborId, nodeId);
        }
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
        vector<int> virtualPrimaryServerIds;
        for (++it; virtualPrimaryServerIds.size() < virtualPrimaryNum; ++it) {
            virtualPrimaryServerIds.emplace_back((*it)->getId());
        }
#ifndef NDEBUG
        //        cout << "--- add node " << nodeId << " (" << primaryServer->getId() <<  ") ---" << endl;
#endif
        primaryServer->addNode(nodeId, Server::NodeType::PRIMARY);
        for (auto virtualPrimaryServerId : virtualPrimaryServerIds) {
            auto virtualPrimaryServer = servers[virtualPrimaryServerId].get();
            virtualPrimaryServer->addNode(nodeId, Server::NodeType::VIRTUAL_PRIMARY);
        }
        node.GetDat().virtualPrimaryNum = virtualPrimaryNum;
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
                if (serverA->getNode(neighborId).type == Server::NodeType::NON_PRIMARY && isPDSN(node, neighbor)) {
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

    void _reallocateNode(int nodeId, SCBValue SCB, int serverBId) {
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

//        int cost2 = computeInterServerCost();
//        if (cost1 < cost2) {
//            cout << cost1 << " " << cost2 << endl;
//        }

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

    void reallocateNode(int nodeId) {
        auto p = findMaxSCB(nodeId);
        SCBValue maxSCB = p.first;
        int maxSCBServerId = p.second;
        if (maxSCB.value > 0 && maxSCBServerId >= 0) {
            _reallocateNode(nodeId, maxSCB, maxSCBServerId);
        }
    }

    void reallocateAndSwapNode() {
        vector<pair<int, int> > arr;
        arr.reserve(allNodes.size());
        for (auto nodeId : allNodes) {
            auto p = findMaxSCB(nodeId);
            if (p.first.value > 0) {
                arr.emplace_back(p.first.value, nodeId);
            }
        }
        sort(arr.begin(), arr.end(), greater<>());
        for (auto p : arr) {
            reallocateNode(p.second);
        }
    }

    bool tryReBalance(int serverAId, int serverBId, int originCost) {
        auto serverA = servers[serverAId].get();
        auto serverB = servers[serverBId].get();
        int loadDiff = serverA->getLoad() - serverB->getLoad();
        int newCost = serverA->computeInterServerCost() + serverB->computeInterServerCost();
        // return false if new cost is even larger
        if (newCost >= originCost) {
            return false;
        }
        // return true if already balanced
        if (-loadConstraint <= loadDiff && loadDiff <= loadConstraint) {
            return true;
        }
        // ensure we're moving from A to B
        if (loadDiff < 0) {
            swap(serverAId, serverBId);
            swap(serverA, serverB);
        }
        auto singleNodes = serverA->getSingleNodes();
        // if server B have virtual primary, moving it have no effect on load
        for (auto it = singleNodes.begin(); it != singleNodes.end();) {
            auto nodeId = *it;
            if (serverB->hasNode(nodeId) && serverB->getNode(nodeId).type == Server::NodeType::VIRTUAL_PRIMARY) {
                it = singleNodes.erase(it);
            } else {
                ++it;
            }
        }
        vector<int> movedNodes;
        while (!singleNodes.empty()) {
            int maxSCBNodeId = -1;
            SCBValue maxSCB;
            maxSCB.value = numeric_limits<int>::min();
            for (auto nodeId : singleNodes) {
                auto p = findMaxSCB(nodeId, serverBId);
                if (p.first.value > maxSCB.value) {
                    maxSCB = p.first;
                    maxSCBNodeId = nodeId;
                }
            }
            if (originCost - newCost + maxSCB.value > 0) {
                movedNodes.emplace_back(maxSCBNodeId);
                singleNodes.erase(maxSCBNodeId);
                moveNode(maxSCBNodeId, serverBId);
                newCost = serverA->computeInterServerCost() + serverB->computeInterServerCost();
            } else break;
        }
        // examine the result
        loadDiff = serverA->getLoad() - serverB->getLoad();
        newCost = serverA->computeInterServerCost() + serverB->computeInterServerCost();
        if (-loadConstraint <= loadDiff && loadDiff <= loadConstraint && newCost < originCost) {
            // update the single nodes
            for (auto nodeId : movedNodes) {
                serverA->getSingleNodes().erase(nodeId);
                serverB->getSingleNodes().emplace(nodeId);
            }
//            cout << "rebalance move " << movedNodes.size() << " nodes from server " << serverAId << " to " << serverBId;
//            cout << ", cost " << originCost << " -> " << newCost << endl;
            return true;
        }
        // if load balance failed, reverse the operations
        for (auto nodeId : movedNodes) {
            moveNode(nodeId, serverAId);
        }
//        cout << "rebalance failed" << endl;
        return false;
    }

    void mergeNodes() {
        for (auto &server : servers) {
            server->mergeNodes(randomGenerator);
            int serverId = server->getId();
            auto &groupedNodes = server->getGroupedNodes();
            for (auto &nodeIds : groupedNodes) {
                mergedNodes.emplace(nodeIds);
            }
        }
        int count = 0;
        for (auto itA = mergedNodes.begin(); itA != mergedNodes.end(); ++itA) {
            int serverAId = getNode(itA->front()).GetDat().primaryServerId;
            auto serverA = servers[serverAId].get();

            auto itB = itA;
            while (++itB != mergedNodes.end()) {
                if (itA->size() - itB->size() > loadConstraint) break;
                int serverBId = getNode(itB->front()).GetDat().primaryServerId;
                if (serverAId == serverBId) break;
                auto serverB = servers[serverBId].get();

                int originCost = serverA->computeInterServerCost() + serverB->computeInterServerCost();
                for (auto nodeId : *itA) {
                    moveNode(nodeId, serverBId);
                }
                for (auto nodeId : *itB) {
                    moveNode(nodeId, serverAId);
                }

                if (itA->size() == 1) {
                    serverA->getSingleNodes().erase((*itA)[0]);
                }
                if (itB->size() == 1) {
                    serverB->getSingleNodes().erase((*itB)[0]);
                }

                bool flag = tryReBalance(serverAId, serverBId, originCost);

                if (itA->size() == 1) {
                    serverA->getSingleNodes().emplace((*itA)[0]);
                }
                if (itB->size() == 1) {
                    serverB->getSingleNodes().emplace((*itB)[0]);
                }
//                if (newCost < originCost) {
//                    cout << "swap " << itA->size() << " nodes from server " << serverAId;
//                    cout << " and " << itB->size() << " nodes from server " << serverBId;
//                    cout << ", cost " << originCost << " -> " << newCost << endl;
//                }
                if (!flag) {
                    for (auto nodeId : *itA) {
                        moveNode(nodeId, serverAId);
                    }
                    for (auto nodeId : *itB) {
                        moveNode(nodeId, serverBId);
                    }
                } else {
                    ++count;
                }
            }
        }
    }

    void getSwappableVirtualPrimary(int serverAId, int serverBId, vector<int> &nodes) {
        auto serverA = servers[serverAId].get();
        auto serverB = servers[serverBId].get();
        for (auto nodeId : serverA->getVirtualPrimaryNodes()) {
            if (serverB->hasNode(nodeId) && serverB->getNode(nodeId).type == Server::NodeType::NON_PRIMARY) {
                auto &node = getNode(nodeId);
                auto neighborNum = node.GetDeg();
                bool flag = true;
                for (int i = 0; i < neighborNum; i++) {
                    int neighborId = node.GetNbrNId(i);
                    if (getNode(neighborId).GetDat().primaryServerId == serverAId) {
                        flag = false;
                        break;
                    }
                }
                if (flag) {
                    nodes.emplace_back(nodeId);
                }
            }
        }
    }

    void virtualPrimarySwapping() {
        for (int serverAId = 0; serverAId < servers.size(); serverAId++) {
            auto serverA = servers[serverAId].get();
            for (int serverBId = 0; serverBId < servers.size(); serverBId++) {
                if (serverAId == serverBId) continue;
                auto serverB = servers[serverBId].get();
                vector<int> nodesA, nodesB;
                getSwappableVirtualPrimary(serverAId, serverBId, nodesA);
                getSwappableVirtualPrimary(serverBId, serverAId, nodesB);
                auto removeNum = min(nodesA.size(), nodesB.size());
                for (int i = 0; i < removeNum; i++) {
                    int nodeAId = nodesA[i];
                    int nodeBId = nodesB[i];
                    serverA->removeNode(nodeAId);
                    serverA->removeNode(nodeBId);
                    serverB->removeNode(nodeAId);
                    serverB->removeNode(nodeBId);
                    serverA->addNode(nodeBId, Server::NodeType::VIRTUAL_PRIMARY);
                    serverB->addNode(nodeAId, Server::NodeType::VIRTUAL_PRIMARY);
//                    cout << "server " << serverAId << " " << nodeAId << " (V) " << nodeBId << " (N), ";
//                    cout << "server " << serverBId << " " << nodeBId << " (V) " << nodeAId << " (N)" << endl;
                }
            }
        }
    }

    void runSPAR() {
        for (auto node = rawGraph->BegNI(); node != rawGraph->EndNI(); node++) {
            int nodeId = node.GetId();
            addNode(nodeId);
        }

        // SPAR's edge addition
        for (auto edge = rawGraph->BegEI(); edge != rawGraph->EndEI(); edge++) {
            int nodeId = edge.GetSrcNId();
            int neighborId = edge.GetDstNId();
            if (nodeId == neighborId) continue;
            auto &neighborNode = getNode(neighborId);
            if (neighborNode.GetDat().primaryServerId >= 0 && !isEdge(nodeId, neighborId)) {
                graph->AddEdge(nodeId, neighborId);
                addEdgeSPAR(nodeId, neighborId);
            }
        }

        int cost = computeInterServerCost();
        cout << cost << endl;
    }

    void runMetis() {
        idx_t nVertices = rawGraph->GetNodes();
        idx_t nEdges = rawGraph->GetEdges();
        idx_t nWeights = 1;
        idx_t nParts = servers.size();

        idx_t objval;
        vector<idx_t> part(nVertices);

        vector<idx_t> xadj;
        xadj.reserve(nVertices + 1);
        vector<idx_t> adjncy;
        adjncy.reserve(nEdges * 2);

        idx_t current = 0;
        unordered_map<int, int> nodeIdMap;

        int nodeIdCount = 0;
        for (auto node = rawGraph->BegNI(); node != rawGraph->EndNI(); node++) {
            int nodeId = node.GetId();
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

        int ret = METIS_PartGraphKway(&nVertices, &nWeights, xadj.data(), adjncy.data(),
                                      NULL, NULL, NULL, &nParts, NULL,
                                      NULL, NULL, &objval, part.data());

        for (auto node = graph->BegNI(); node != graph->EndNI(); node++) {
            int nodeId = node.GetId();
            int metisNodeId = nodeIdMap[nodeId];
            int serverId = part[metisNodeId];
            auto server = servers[serverId].get();
            server->addNode(nodeId, Server::NodeType::PRIMARY);
            node.GetDat().primaryServerId = serverId;
        }

        int cost = computeInterServerCost();
        cout << cost << endl;

/*        for (auto &server : servers) {
            cout << server->getLoad() << endl;
        }*/

        for (auto node = graph->BegNI(); node != graph->EndNI(); node++) {
            int nodeId = node.GetId();
            auto it = serverSet.begin();
            vector<int> virtualPrimaryServerIds;
            for (; virtualPrimaryServerIds.size() < virtualPrimaryNum; ++it) {
                if (!(*it)->hasNode(nodeId)) {
                    virtualPrimaryServerIds.emplace_back((*it)->getId());
                }
            }
            for (auto virtualPrimaryServerId : virtualPrimaryServerIds) {
                auto virtualPrimaryServer = servers[virtualPrimaryServerId].get();
                virtualPrimaryServer->addNode(nodeId, Server::NodeType::VIRTUAL_PRIMARY);
            }
            node.GetDat().virtualPrimaryNum = virtualPrimaryNum;
        }

        cost = computeInterServerCost();
        cout << cost << endl;

/*        for (auto &server : servers) {
            cout << server->getLoad() << endl;
        }*/

        for (auto edge = rawGraph->BegEI(); edge != rawGraph->EndEI(); edge++) {
            int nodeId = edge.GetSrcNId();
            int neighborId = edge.GetDstNId();
            if (nodeId == neighborId) continue;
            auto &node = getNode(nodeId);
            auto &neighbor = getNode(neighborId);
            auto nodeServer = servers[node.GetDat().primaryServerId].get();
            auto neighborServer = servers[neighbor.GetDat().primaryServerId].get();
            if (neighbor.GetDat().primaryServerId >= 0 && !isEdge(nodeId, neighborId)) {
                graph->AddEdge(nodeId, neighborId);
//                addEdgeSPAR(nodeId, neighborId);
                if (!nodeServer->hasNode(neighborId)) {
                    nodeServer->addNode(neighborId, Server::NodeType::NON_PRIMARY);
//                    neighbor.GetDat().virtualPrimaryNum++;
                }
                if (!neighborServer->hasNode(nodeId)) {
                    neighborServer->addNode(nodeId, Server::NodeType::NON_PRIMARY);
//                    node.GetDat().virtualPrimaryNum++;
                }
            }
        }

        cost = computeInterServerCost();
        cout << cost << endl;

/*        for (auto &server : servers) {
            cout << server->getLoad() << endl;
        }*/

        /*for (auto node = graph->BegNI(); node != graph->EndNI(); node++) {
            int nodeId = node.GetId();
            int size = ((int) virtualPrimaryNum) - node.GetDat().virtualPrimaryNum;
            while (size > 0) {
                bool flag = false;
                for (auto server: serverSet) {
                    if (!server->hasNode(nodeId)) {
                        server->addNode(nodeId, Server::NodeType::VIRTUAL_PRIMARY);
                        node.GetDat().virtualPrimaryNum++;
                        --size;
                        flag = true;
                        break;
                    }
                }
                assert(flag);
            }
        }

        cost = computeInterServerCost();
        cout << cost << endl;

        for (auto &server : servers) {
            cout << server->getLoad() << endl;
        }*/
    }

    void runProposed(bool random = false, bool offline = true) {
        for (auto node = rawGraph->BegNI(); node != rawGraph->EndNI(); node++) {
            int nodeId = node.GetId();
            addNode(nodeId);

            // ensure locality
            auto neighborNum = node.GetDeg();
            for (int i = 0; i < neighborNum; i++) {
                int neighborId = node.GetNbrNId(i);
                if (nodeId == neighborId) continue;
                auto &neighborNode = getNode(neighborId);
                if (neighborNode.GetDat().primaryServerId >= 0 && !isEdge(nodeId, neighborId)) {
                    graph->AddEdge(nodeId, neighborId);
                    ensureLocality(nodeId, neighborId);
                }
            }

            // offline algorithm
            if (!random) {
                reallocateNode(nodeId);
            }
        }
        int cost = computeInterServerCost();
        cout << cost << endl;

        if (random || !offline) return;

        // node relocation and swapping
        for (int eta = 0; eta < 10; eta++) {
            reallocateAndSwapNode();
            int newCost = computeInterServerCost();
            cout << newCost << endl;
            if (cost - newCost < 50) {
                break;
            }
            cost = newCost;
        }

        // merge nodes
        mergeNodes();

        cost = computeInterServerCost();
        cout << cost << endl;

        // virtual primary swapping
        virtualPrimarySwapping();

        cost = computeInterServerCost();
        cout << cost << endl;
    }

    void run() {
        switch (algorithm) {
            case Algorithm::RANDOM:
                runProposed(true);
                break;
            case Algorithm::SPAR:
                runSPAR();
                break;
            case Algorithm::ONLINE:
                runProposed(false, false);
                break;
            case Algorithm::OFFLINE:
                runProposed(false, true);
                break;
            case Algorithm::METIS:
                runMetis();
                break;
            default:
                assert(0);
        }
    }


};

#endif //SOCIAL_NETWORK_MANAGER_H
