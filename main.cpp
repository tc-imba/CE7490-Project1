#include "scalable/Manager.h"

#include <Snap.h>
#include <iostream>


using namespace std;




int main() {
//    graph->GetNodes();
//    cout << graph->GetNodes() << endl;

    Manager manager(128, 0, 1);
    manager.run();

    return 0;
}
