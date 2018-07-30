/**
 * @file unit_test.cpp - unit test definitions for SlottedPage, HeapFile and HeapTable.
 *
 * @see "Seattle University, CPSC5300, Summer 2018"
 */
 
#include "unit_test.h"
#include "db_cxx.h"
#include "heap_storage.h"

void test_slotted_page_when_empty()
{
	std::cout << "test_slotted_page_when_empty..." << std::endl;
	
	std::unique_ptr<char[]> block_space(new char[DbBlock::BLOCK_SZ]);
	Dbt block(block_space.get(), sizeof(block_space));
	SlottedPage slotted_page(block, 1, true);
	
	std::unique_ptr<RecordIDs> record_ids(slotted_page.ids());
	
	if (!record_ids->empty())
	{
		throw test_fail_error("Newly created block should not have any record");
	}
}

void test_slotted_page_add()
{
	std::cout << "test_slotted_page_add..." << std::endl;
	
	std::unique_ptr<char[]> block_space(new char[DbBlock::BLOCK_SZ]);
	Dbt block(block_space.get(), sizeof(block_space));
	SlottedPage slotted_page(block, 1, true);
	
	Dbt record((char*) "a", 2);
	
	// Filing out the space
	for (int i = 0; i < 100; i++)
	{
		slotted_page.add(&record);
	}
}

void test_slotted_page_get()
{
	std::cout << "test_slotted_page_get..." << std::endl;
	
	std::unique_ptr<char[]> block_space(new char[DbBlock::BLOCK_SZ]);
	Dbt block(block_space.get(), sizeof(block_space));
	SlottedPage slotted_page(block, 1, true);
	
	Dbt record((char*)"HelloWorld", 11);
	slotted_page.add(&record);
	
	std::unique_ptr<Dbt> dbt(slotted_page.get(1));
	std::string value((char *) dbt->get_data());
	
	if (value != "HelloWorld")
	{
		throw test_fail_error("slotted_page get() failed");
	}
}

void test_slotted_page_put()
{
	std::cout << "test_slotted_page_put..." << std::endl;
	
	std::unique_ptr<char[]> block_space(new char[DbBlock::BLOCK_SZ]);
	Dbt block(block_space.get(), sizeof(block_space));
	SlottedPage slotted_page(block, 1, true);
	
	Dbt record((char*)"HelloWorld", 11);
	
	slotted_page.add(&record);
	
	Dbt updated_record((char*)"HelloSeattleU", 14);
	slotted_page.put(1, updated_record);
	
	std::unique_ptr<Dbt> dbt(slotted_page.get(1));
	std::string value((char *) dbt->get_data());
	
	if (value != "HelloSeattleU")
	{
		throw test_fail_error("slotted_page put() failed");
	}
}

void test_slotted_page_del()
{
	std::cout << "test_slotted_page_del..." << std::endl;
	
	std::unique_ptr<char[]> block_space(new char[DbBlock::BLOCK_SZ]);
	Dbt block(block_space.get(), sizeof(block_space));
	SlottedPage slotted_page(block, 1, true);

	
	Dbt record((char*)"HelloWorld", 11);
	slotted_page.add(&record);
	slotted_page.del(1);
	
	std::unique_ptr<RecordIDs> record_ids(slotted_page.ids());
	
	if (!record_ids->empty())
	{
		throw test_fail_error("slotted_page del() failed");
	}
}

void test_slotted_page_get_block_id()
{
	std::cout << "test_slotted_page_get_block_id..." << std::endl;
	
	std::unique_ptr<char[]> block_space(new char[DbBlock::BLOCK_SZ]);
	Dbt block(block_space.get(), sizeof(block_space));
	SlottedPage slotted_page(block, 1, true);
	
	if (slotted_page.get_block_id() != 1)
	{
		throw test_fail_error("Use block_id=1 to create block, returned block_id != 1");
	}
}

void test_slotted_page_get_data()
{
	std::cout << "test_slotted_page_get_data..." << std::endl;
	
	std::unique_ptr<char[]> block_space(new char[DbBlock::BLOCK_SZ]);
	Dbt block(block_space.get(), sizeof(block_space));
	SlottedPage slotted_page(block, 1, true);
	
	if (slotted_page.get_data() != block_space.get())
	{
		throw test_fail_error("slotted_page get_data() failed");
	}
}

void test_slotted_page_get_block()
{
	std::cout << "test_slotted_page_get_block..." << std::endl;
	
	std::unique_ptr<char[]> block_space(new char[DbBlock::BLOCK_SZ]);
	Dbt block(block_space.get(), sizeof(block_space));
	SlottedPage slotted_page(block, 1, true);
	
	if (slotted_page.get_block()->get_data() != block_space.get())
	{
		throw test_fail_error("slotted_page get_block() failed");
	}
}

void test_slotted_page_with_old_block()
{
	std::cout << "test_slotted_page_with_old_block..." << std::endl;
	
	std::unique_ptr<char[]> block_space(new char[DbBlock::BLOCK_SZ]);
	Dbt block(block_space.get(), sizeof(block_space));
	SlottedPage slotted_page(block, 1, true);
	
	Dbt record((char*)"HelloWorld", 11);
	slotted_page.add(&record);
	
	SlottedPage slotted_page2(block, 2, false);
	std::unique_ptr<Dbt> dbt(slotted_page2.get(1));
	std::string value((char *) dbt->get_data());
	
	if (value != "HelloWorld")
	{
		throw test_fail_error("slotted_page get() failed");
	}
}

void test_slotted_page_ids()
{
	std::cout << "test_slotted_page_ids..." << std::endl;
	
	std::unique_ptr<char[]> block_space(new char[DbBlock::BLOCK_SZ]);
	Dbt block(block_space.get(), sizeof(block_space));
	SlottedPage slotted_page(block, 1, true);
	
	Dbt record((char*)"HelloWorld", 11);
	slotted_page.add(&record);
	slotted_page.add(&record);
	
	std::unique_ptr<RecordIDs> record_ids(slotted_page.ids());
	
	if (record_ids->at(0) != 1)
	{
		throw test_fail_error("slotted_page ids() failed");
	}
	
	if (record_ids->at(1) != 2)
	{
		throw test_fail_error("slotted_page ids() failed");
	}
}

void test_slotted_page() throw (test_fail_error)
{
	test_slotted_page_when_empty();
	test_slotted_page_add();
	test_slotted_page_get();
	test_slotted_page_put();
	test_slotted_page_del();
	test_slotted_page_with_old_block();
	test_slotted_page_ids();
	test_slotted_page_get_block();
	test_slotted_page_get_data();
}

void test_heap_file_create()
{
	std::cout << "test_heap_file_create..." << std::endl;
	
	HeapFile heap_file("heap_file_u");
	heap_file.create();
	
	HeapFile heap_file_duplicate("heap_file_u");
	
	try
	{
		heap_file_duplicate.create();
		throw test_fail_error("Should throw exception creating db when it already exists");
	}
	catch(DbException exception)
	{
		if (exception.get_errno() != EEXIST)
		{
			throw exception;
		}
	}
	
	heap_file.drop();
}

void test_heap_file_drop()
{
	std::cout << "test_heap_file_drop..." << std::endl;
	
	HeapFile heap_file("heap_file_u");
	heap_file.create();
	
	heap_file.drop();
}

void test_heap_file_open()
{
	std::cout << "test_heap_file_open..." << std::endl;
	
	HeapFile heap_file("heap_file_u");
	heap_file.create();
	
	heap_file.open();
	
	heap_file.drop();
}

void test_heap_file_close()
{
	std::cout << "test_heap_file_close..." << std::endl;
	
	HeapFile heap_file("heap_file_u");
	heap_file.create();
	heap_file.open();
	
	heap_file.close();
	
	heap_file.drop();
}

void test_heap_file_get_new()
{
	std::cout << "test_heap_file_get_new..." << std::endl;
	
	HeapFile heap_file("heap_file_u");
	heap_file.create();
	heap_file.open();
	
	std::unique_ptr<SlottedPage> slotted_page(heap_file.get_new());
	
	if (slotted_page->get_block_id() != 2)
	{
		throw test_fail_error("heap_file get_new() failed");
	}
	
	if (slotted_page->get_block()->get_size() != DbBlock::BLOCK_SZ)
	{
		throw test_fail_error("heap_file get_new() failed");
	}
	
	std::unique_ptr<SlottedPage> slotted_page_2(heap_file.get_new());
	
	if (slotted_page_2->get_block_id() != 3)
	{
		throw test_fail_error("heap_file get_new() failed");
	}
	
	heap_file.drop();
}

void test_heap_file_get_put()
{
	std::cout << "test_heap_file_get_put..." << std::endl;
	
	HeapFile heap_file("heap_file_u");
	heap_file.create();
	heap_file.open();
	
	std::unique_ptr<SlottedPage> slotted_page(heap_file.get_new());
	
	Dbt dbt((char*)"HelloWorld", 11);
	slotted_page->add(&dbt);
	heap_file.put(slotted_page.get());
	
	std::unique_ptr<SlottedPage> slotted_page_duplicate(heap_file.get(2));
	std::unique_ptr<Dbt> record(slotted_page_duplicate->get(1));
	std::string value((char*)record->get_data());
	
	if (value != "HelloWorld")
	{
		throw test_fail_error("heap_file get() or put() failed");
	}
	
	heap_file.drop();
}

void test_heap_file_block_ids()
{
	std::cout << "test_heap_file_block_ids..." << std::endl;
	
	HeapFile heap_file("heap_file_u");
	heap_file.create();
	heap_file.open();
	
	std::unique_ptr<SlottedPage> slotted_page(heap_file.get_new());
	std::unique_ptr<SlottedPage> slotted_page_2(heap_file.get_new());
	std::unique_ptr<BlockIDs> block_ids(heap_file.block_ids());
	
	if (block_ids->size() != 3 || block_ids->at(0) != 1 || block_ids->at(1) != 2)
	{
		throw test_fail_error("heap_file block_ids() failed");
	}
	
	heap_file.drop();
}

void test_heap_file() throw (test_fail_error)
{	
	test_heap_file_create();
	test_heap_file_drop();
	test_heap_file_open();
	test_heap_file_close();
	test_heap_file_get_new();
	test_heap_file_get_put();
	test_heap_file_block_ids();
}

void test_heap_table_create(ColumnNames &column_names, ColumnAttributes& column_attributes)
{
	std::cout << "test_heap_table_create..." << std::endl;
	
	HeapTable heap_table("heap_table_u", column_names, column_attributes);
	heap_table.create();
	
	try
	{
		HeapTable heap_table_duplicate("heap_table_u", column_names, column_attributes);
		heap_table_duplicate.create();
		throw test_fail_error("Should throw exception creating db when it already exists");
	}
	catch(DbException exception)
	{
		if (exception.get_errno() != EEXIST)
		{
			throw exception;
		}
	}
	
	heap_table.drop();
}

void test_heap_table_create_if_not_exists(ColumnNames &column_names, ColumnAttributes& column_attributes)
{
	std::cout << "test_heap_table_create_if_not_exists..." << std::endl;
	
	HeapTable heap_table("heap_table_u", column_names, column_attributes);
	heap_table.create();
	
	HeapTable heap_table_duplicate("heap_table_u", column_names, column_attributes);
	heap_table_duplicate.create_if_not_exists();
	
	heap_table.drop();
}

void test_heap_table_drop(ColumnNames &column_names, ColumnAttributes& column_attributes)
{
	std::cout << "test_heap_table_drop..." << std::endl;
	
	HeapTable heap_table("heap_table_u", column_names, column_attributes);
	heap_table.create();
	
	heap_table.drop();
}

void test_heap_table_open(ColumnNames &column_names, ColumnAttributes& column_attributes)
{
	std::cout << "test_heap_table_open..." << std::endl;
	
	HeapTable heap_table("heap_table_u", column_names, column_attributes);
	heap_table.create();
	
	heap_table.open();
	
	heap_table.drop();
}

void test_heap_table_close(ColumnNames &column_names, ColumnAttributes& column_attributes)
{
	std::cout << "test_heap_table_close..." << std::endl;
	
	HeapTable heap_table("heap_table_u", column_names, column_attributes);
	heap_table.create();
	
	heap_table.close();
	
	heap_table.drop();
}

void test_heap_table_insert(ColumnNames &column_names, ColumnAttributes& column_attributes)
{
	std::cout << "test_heap_table_insert..." << std::endl;
	
	HeapTable heap_table("heap_table_u", column_names, column_attributes);
	heap_table.create();
	
	ValueDict value_dict;
	value_dict["a"] = Value("a");
	value_dict["b"] = Value(1);
	
	Handle handle = heap_table.insert(&value_dict);
	std::unique_ptr<ValueDict> row_returned(heap_table.project(handle));
	
	if (row_returned->at("a").s != "a" || row_returned->at("b").n != 1)
	{
		heap_table.drop();

		throw test_fail_error("heap_table insert() failed");
	}
	
	heap_table.drop();
}

void test_heap_table_select(ColumnNames &column_names, ColumnAttributes& column_attributes)
{
	std::cout << "test_heap_table_select..." << std::endl;
	
	HeapTable heap_table("heap_table_u", column_names, column_attributes);
	heap_table.create();
	
	ValueDict value_dict;
	value_dict["a"] = Value("a");
	value_dict["b"] = Value(1);
	
	heap_table.insert(&value_dict);
	
	std::unique_ptr<Handles> handles(heap_table.select());
	
	if (handles->size() != 1)
	{
		heap_table.drop();
		throw test_fail_error("heap_table del() failed");
	}
	
	heap_table.insert(&value_dict);
	
	std::unique_ptr<Handles> handles_copy(heap_table.select());
	
	if (handles_copy->size() != 2)
	{
		heap_table.drop();
		throw test_fail_error("heap_table del() failed");
	}
	
	heap_table.drop();
}

void test_heap_table_project(ColumnNames &column_names, ColumnAttributes& column_attributes)
{
	std::cout << "test_heap_table_project..." << std::endl;
	
	HeapTable heap_table("heap_table_u", column_names, column_attributes);
	heap_table.create();
	
	ValueDict value_dict;
	value_dict["a"] = Value("HelloWorld");
	value_dict["b"] = Value(1);
	
	Handle handle = heap_table.insert(&value_dict);
	
	std::unique_ptr<ValueDict> returned_row(heap_table.project(handle));
	
	if (returned_row->at("a").s != "HelloWorld" || returned_row->at("b").n != 1)
	{
		heap_table.drop();
		throw test_fail_error("heap_table project() failed");
	}
	
	ColumnNames projected_column_names;
	projected_column_names.push_back("a");
	
	std::unique_ptr<ValueDict> returned_row_projected(heap_table.project(handle, &projected_column_names));
	
	if (returned_row_projected->at("a").s != "HelloWorld" || returned_row_projected->find("b") != returned_row_projected->end())
	{
		heap_table.drop();
		throw test_fail_error("heap_table project() failed");
	}
	
	heap_table.drop();
}

void test_heap_table() throw (test_fail_error)
{
	ColumnNames column_names;
	ColumnAttributes column_attributes;

	column_names.push_back("a");
	column_names.push_back("b");
	column_attributes.push_back(ColumnAttribute::DataType::TEXT);
	column_attributes.push_back(ColumnAttribute::DataType::INT);
	
	test_heap_table_create(column_names, column_attributes);
	test_heap_table_create_if_not_exists(column_names, column_attributes);
	test_heap_table_drop(column_names, column_attributes);
	test_heap_table_open(column_names, column_attributes);
	test_heap_table_close(column_names, column_attributes);
	test_heap_table_insert(column_names, column_attributes);
	test_heap_table_select(column_names, column_attributes);
	test_heap_table_project(column_names, column_attributes);
}

void unit_test()
{
	test_slotted_page();
	test_heap_file();
	test_heap_table();
}