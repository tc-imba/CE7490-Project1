#include "Manager.h"

#include <Snap.h>
#include <iostream>


using namespace std;




int main() {
//    graph->GetNodes();
//    cout << graph->GetNodes() << endl;

    string dataFile = "data/facebook.txt";
    Manager manager(dataFile, 128, 3, 1);
    manager.run();

    return 0;
}
