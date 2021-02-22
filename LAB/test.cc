#include<iostream>
#include<vector>
#include<list>
using namespace std;

vector<int> v1;

void func(){
	vector<int> v3;
	v3.push_back(1);
	v1 = v3;
}	

int main(){
	list<int> l1;
	l1.push_back(1);
	l1.push_back(2);
	l1.push_back(3);
	list<int>::iterator l1_it;
	
	for(l1_it = l1.begin() ; l1_it != l1.end() ; l1_it++){
		cout << *(l1_it) << endl;
	}
	l1_it = l1.begin();
	for(int i=0; i<2; i++) l1_it++;
	cout << *(l1_it) << endl;
}
