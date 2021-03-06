//
// Created by liu on 18/10/2020.
//

#ifndef SOCIAL_NETWORK_SERVER_H
#define SOCIAL_NETWORK_SERVER_H

#include <Snap.h>
#include <set>
#include <random>

using namespace std;

class Manager;

class Server {
public:
    enum class NodeType {
        PRIMARY,
        VIRTUAL_PRIMARY,
        NON_PRIMARY,
    };

    const static char *NodeTypeString[3];

    struct Node {
        NodeType type;

        void Save(TSOut& SOut) const {
            TInt temp((int) type);
            temp.Save(SOut);
        }
    };

private:
    TPt<TNodeNet<Node> > graph;
    set<int> primaryNodes, virtualPrimaryNodes;
    int id;
    int load = 0;
    Manager *manager;
    set<int> singleNodes;
    vector<vector<int> > groupedNodes;

public:
    struct Compare {
        bool operator()(const Server *a, const Server *b) const {
            if (a->load != b->load) return a->load < b->load;
            return a->id < b->id;
        }
    };

    Server(int id, Manager *manager) : id(id), manager(manager) {
        graph = TNodeNet<Node>::New();
    }

    void addNode(int nodeId, NodeType type);

    Node &getNode(int nodeId);

    void mergeNodes(mt19937 &generator);

    void removeNode(int nodeId);

    bool hasNode(int nodeId) const;

    int getLoad() const;
    
    int getId() const;

    const set<int> & getPrimaryNodes() const;

    const set<int> & getVirtualPrimaryNodes() const;

    int computeInterServerCost() const;

    void validate();

    set<int> &getSingleNodes();

    vector<vector<int> > &getGroupedNodes();
};


#endif //SOCIAL_NETWORK_SERVER_H
