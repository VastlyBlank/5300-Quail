// test function -- returns true if all tests pass

#include <iostream>
#include "heap_storage.h"
#include <cstring>
#include <string>
using namespace std;

bool test_heap_storage() {
	ColumnNames column_names;
	column_names.push_back("a");
	column_names.push_back("b");
	ColumnAttributes column_attributes;
	ColumnAttribute ca(ColumnAttribute::INT);
	column_attributes.push_back(ca);
	ca.set_data_type(ColumnAttribute::TEXT);
	column_attributes.push_back(ca);
    HeapTable table1("_test_create_drop_cpp", column_names, column_attributes);
    table1.create();
    std::cout << "create ok" << std::endl;
    table1.drop();  // drop makes the object unusable because of BerkeleyDB restriction -- maybe want to fix this some day
    std::cout << "drop ok" << std::endl;

    HeapTable table("_test_data_cpp", column_names, column_attributes);
    table.create_if_not_exists();
    std::cout << "create_if_not_exsts ok" << std::endl;

    ValueDict row;
    row["a"] = Value(12);
    row["b"] = Value("Hello!");
    std::cout << "try insert" << std::endl;
    table.insert(&row);
    std::cout << "insert ok" << std::endl;
    Handles* handles = table.select();
    std::cout << "select ok " << handles->size() << std::endl;
    ValueDict *result = table.project((*handles)[0]);
    std::cout << "project ok" << std::endl;
    Value value = (*result)["a"];
    if (value.n != 12)
    	return false;
    value = (*result)["b"];
    if (value.s != "Hello!")
		return false;
    table.drop();
/*
	cout << "begin slotted page test" << endl;

	HeapFile* hFile = new HeapFile("newHFile");
	hFile->create();
	hFile->open();
	SlottedPage* page = hFile->get_new();//calls constructor
        string str1 = "hello";
	const char* str2 = "wow!";	
	cout << "before bytes" << endl;
	char*bytes = new char[DbBlock::BLOCK_SZ];
	uint size = str1.length();
	memcpy(bytes, str1.c_str(), size);
	char *right_size_bytes = new char[size];
	memcpy(right_size_bytes, bytes, size);
	//delete[] bytes;
	Dbt *data = new Dbt(right_size_bytes, size);
	
	cout << "before add" << endl;

	//char tempChar[sizeof(str1)];
//	std::memset(tempChar, 0, sizeof(tempChar));
	//Dbt temp(tempChar, sizeof(tempChar));

	//memcpy(tempChar, str1, sizeof(str1));
	//Dbt* temp = new Dbt(tempChar, sizeof(tempChar));

	//Dbt temp((void*)str1, sizeof(str1)); 
	Dbt temp2((void*)"wow!", sizeof("wow!"));
	RecordID id1 = page->add(data);
	cout << "id1: " << (int)id1 << endl;
	RecordID id2 = page->add(&temp2);
	const char*strComp= (char*)(page->get(id1)); 
	cout << "base string: " << str1 << endl;
	cout << "added: " << *strComp << endl; 
	cout << "added: " << (char*)page->get(id1)->get_data() << endl; 

	cout << "addition 1: " <<((strComp==str1)? "ok" : "failed") << endl;		
	cout << "addition 2: " <<(((char*)page->get(id2)->get_data()=="wow!")? "ok" : "failed") << endl;
	char* putTest = "goodbye";
	Dbt tempPut(putTest, sizeof(putTest));
	page->put(id1,tempPut);
	cout << "id 1 changed: " <<( (page->get(id1)==(Dbt*)"goodbye")? "ok" : "failed") << endl;		
	cout << "id 2 unchanged: " <<( (page->get(id2)==(Dbt*)"wow!")? "ok" : "failed") << endl;
	char* putTest2 = "again?";
	Dbt tempPut2(putTest2, sizeof(putTest2));
	page->put(id1, tempPut2);
	cout << "id 1 changed again: " <<( (page->get(id1)==(Dbt*)"again?")? "ok" : "failed") << endl;		
	cout << "id 2 unchanged: " <<( (page->get(id2)==(Dbt*)"wow!")? "ok" : "failed") << endl;

	RecordIDs* records = new RecordIDs();
	records->push_back(id1);
	records->push_back(id2);
	RecordIDs* recordsAdded = page->ids();
	cout << "id list size same: " << ( (records->size() == recordsAdded->size())? "ok" : "failed") << endl;	
	cout << "first id same: " << ( (records->front() == recordsAdded->front())? "ok" : "failed") << endl;	
	cout << "last id same: " << ( (records->back() == recordsAdded->back())? "ok" : "failed") << endl;

	cout << "deletion" << endl;
	cout << "size before deletion" << recordsAdded->size() << endl;
	page->del(id1);
	recordsAdded = page->ids();
	cout << "ids list size after deletion" << recordsAdded->size() << endl;
	cout << "id1 deleted: " << ( (page->get(id1)==NULL)?"ok":"failed") << endl;
	
	Dbt temp3((void*)"lastAdd", sizeof("lastAdd"));
	page->add(&temp3);
	recordsAdded = page->ids();
	cout << "ids list size after addition" << recordsAdded->size() << endl;

	bool idEqual = true;	
	if(page->get(recordsAdded->at(0)) != (Dbt*)"wow!")
		idEqual = false;
	if(page->get(recordsAdded->at(1)) != (Dbt*)"lastAdd")
		idEqual = false;
	cout << "after addition, new ids, but not overwritten: " << (idEqual?"ok":"failed") << endl;
*/

    return true;
}
