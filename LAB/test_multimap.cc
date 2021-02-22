#include <iostream>
#include <vector>
#include <string>
#include <sstream>
#include <unordered_map>
#include <string.h>
#include <stdlib.h>

using namespace std;

int main()
{
   unordered_multimap<int, vector<int>> m;
   
   vector<int> v1;
   vector<int> v2;
   vector<int> v3;
   v1.push_back(1);
   v1.push_back(2);
   v1.push_back(3);
   v2.push_back(4);
   v2.push_back(5);
   v2.push_back(6);
   v3.push_back(7);
   v3.push_back(8);
   v3.push_back(9);-

   m.insert(make_pair(1, v1));
   m.insert(make_pair(1, v2));
   m.insert(make_pair(1, v3));

   
   auto its = m.equal_range(1);
   for(auto it = its.first; it != its.second; ++it) {
      cout << it->first << "  " << it->second[0] << endl;
   }
}