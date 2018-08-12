/**
 * @file unit_test.h - unit test for SlottedPage, HeapFile and HeapTable.
 *
 * @see "Seattle University, CPSC5300, Summer 2018"
 */
 
#pragma once

#include <cstring>
#include <memory>
#include "btree.h"

class test_fail_error : public std::runtime_error {
public:
	explicit test_fail_error(std::string s) : runtime_error(s) {}
};

void test_slotted_page() throw (test_fail_error);

void test_heap_file() throw (test_fail_error);

void test_heap_table() throw (test_fail_error);

bool btree_compare(BTreeIndex &idx, HeapTable &table, ValueDict *test);
bool btree_test();


bool unit_test();
