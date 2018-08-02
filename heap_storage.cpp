/**
 * @file heap_storage.cpp - concrete implementation of the heap_storage.h prototypes
 * SlottedPage: DbBlock
 * HeapFile: DbFile
 * HeapTable: DbRelation
 *
 * @authors Brian Doersch, Jacob Mouser, Kevin Lundeen
 */

#include "heap_storage.h"
#include <cstring>
#include <memory>

using namespace std;

typedef uint16_t u16;

/**
 * @class SlottedPage - heap file implementation of DbBlock
 *
 */

//constructor , author K.Lundeen
SlottedPage::SlottedPage(Dbt &block, BlockID block_id, bool is_new):DbBlock(block, block_id, is_new) {
	if (is_new) {
		this->num_records=0;
		this->end_free=DbBlock::BLOCK_SZ-1;
		put_header();
	}else{
		get_header(this->num_records, this->end_free);
	}
}
//add a new record to the block and return the id, author K.Lundeen
RecordID SlottedPage::add(const Dbt* data) throw(DbBlockNoRoomError){
	if (!has_room(data->get_size()))
		throw DbBlockNoRoomError("not enough room for new record");
	u16 id = ++this->num_records;
	u16 size = (u16)data->get_size();
	this->end_free -= size;
	u16 loc = this -> end_free + 1;
	put_header();
	put_header(id, size, loc);
	memcpy(this->address(loc), data->get_data(), size);
	
	return id;
}

//gets a database block from the database file
Dbt* SlottedPage::get(RecordID record_id) const {
	u16 size, loc;
	get_header(size, loc, record_id);
	if (loc ==0)
		return NULL;
	Dbt* tempBlock = new Dbt(this->address(loc),size);//create a new Dbt structure from data and size	
	return tempBlock;
}

//replaces a record at record_id with the data from the passed in Dbt structure.
void SlottedPage::put(RecordID record_id, const Dbt &data) throw(DbBlockNoRoomError){
	u16 old_size, old_offset;
	get_header(old_size, old_offset, record_id);
	u16 new_size = (u16) data.get_size();
	
	if (new_size> old_size && !has_room(new_size-old_size))
	{
		throw DbBlockNoRoomError("not enough room for updating record");
	}

	if (new_size > old_size)
	{
		u16 shift_offset = new_size - old_size;
		u16 new_offset = old_offset - shift_offset;

		shift_records(record_id + 1, shift_offset);
		memcpy(this->address(new_offset), data.get_data(), new_size);
		put_header(record_id, new_size, new_offset);	

		if (record_id == this->num_records)
		{
			this->end_free -= shift_offset;
		}
	}
	else
	{
		u16 shift_offset = old_size - new_size;
		u16 new_offset = old_offset + shift_offset;

		shift_records(record_id + 1, shift_offset, false);
		memcpy(this->address(new_offset), data.get_data(), new_size);
		put_header(record_id, new_size, new_offset);	

		if (record_id == this->num_records)
		{
			this->end_free += shift_offset;
		}
	}

	put_header();	
}
/**
 * removes a record by setting its size and loc to 0
 * @param record_id which record to delete 
 */
void SlottedPage::del(RecordID record_id){
	u16 size, offset;
	get_header(size, offset, record_id);

	shift_records(record_id + 1, size, false);
	put_header(record_id, 0U, 0U);

	if (record_id == this->num_records)
	{
		this->end_free += size;
	}

	put_header();	
}

/**
 * Check if the block has a specific record 
 * @param record_id which record to check
 */
bool SlottedPage::have_record(RecordID record_id)
{
	if (record_id == 0 || record_id > this->num_records)
	{
		return false;
	}

	u16 size, offset;
	get_header(size, offset, record_id);

	if (offset == 0)
	{
		return false;
	}

	return true;
}

/**
 * Shift corresponding records in the block
 * @param begin_record_id the first record on the left of the deleted or updated record
 * @shift_offset          the size to shift 
 * @left 		  the direction to shift 
 */
void SlottedPage::shift_records(RecordID begin_record_id, u_int16_t shift_offset, bool left)
{
	while(begin_record_id <= this->num_records && !have_record(begin_record_id))
	{
		begin_record_id++;
	}

	if (begin_record_id > this->num_records)
	{
		return;
	}

	u16 begin_offset, begin_size;
	get_header(begin_size, begin_offset, begin_record_id);

	u16 shift_block_size = begin_offset + begin_size - 1 - this->end_free;

	char temperary[shift_block_size];
	memcpy(temperary, this->address(this->end_free + 1), shift_block_size);
	
	if (left)
	{
		memcpy(this->address(this->end_free + 1 - shift_offset), temperary, shift_block_size);
	}
	else
	{
		memcpy(this->address(this->end_free + 1 + shift_offset), temperary, shift_block_size);
	}

	for(RecordID i = begin_record_id; i <= this->num_records; i++)
	{
		if (have_record(i))
		{
			u16 temp_offset, temp_size;
			get_header(temp_size, temp_offset, i);

			if (left)
			{
				put_header(i, temp_size, temp_offset-shift_offset);
			}
			else
			{
				put_header(i, temp_size, temp_offset+shift_offset);
			}
		}
	}

	this->end_free = left ? this->end_free - shift_offset : this->end_free + shift_offset;

	put_header();
}

//sequence of all non-deleted record ids
RecordIDs* SlottedPage::ids(void) const {
	RecordIDs* records = new RecordIDs();//vector

	for (int i = 1; i <= this->num_records; i++){
		u16 size,loc;
		get_header(size, loc, i);//loads up size and loc from id i
		if (loc != 0)//0 is a deleted record, do not include.
		{
			records->push_back(i);
		}
	}

	return records;
}


//get the size and offset for the given id, with id of zero being the block header.
void SlottedPage::get_header(u16 &size, u16 &loc, RecordID id) const {
	size = get_n(4*id);//num_records
	
	if (id == 0 && size == 0)
	{
		loc = DbBlock::BLOCK_SZ - 1;
	}
	else
	{
		loc = get_n(4*id+2);
	}
}

//store the size and offset for given id. For id of zero, store the block header, author K.Lundeen
void SlottedPage::put_header(RecordID id, u16 size, u16 loc){
	if (id == 0) {//called the put_header() version, default params
		size = this->num_records;
		loc = this->end_free;
	}
	put_n(4*id,size);
	put_n(4*id +2, loc);
}

//returns true if there is enough room in the SlottedPage for the new record of size
bool SlottedPage::has_room(u16 size) const {
	u16 free = (this->end_free - ((this->num_records + 1) *4));//subtract the new header room from free space as well
	return (size <= free);
}

//get 2-byte int at given offset in block, author K.Lundeen
u16 SlottedPage::get_n(u16 offset) const {
	return *(u16*)this->address(offset);
}

//put a 2-byte int at given offset in block, author K.Lundeen

void SlottedPage::put_n(u16 offset, u16 n){
	*(u16*)this->address(offset)=n;
}

//make void pointer for given offset in the data block, author K.Lundeen
void* SlottedPage::address(u16 offset) const {
	return (void*)((char*)this->block.get_data()+offset);
}

/**
 * @class HeapFile - heap file implementation of DbFile
 */

HeapFile::HeapFile(string name) : DbFile(name), dbfilename(""), last(0), closed(true), db(_DB_ENV, 0) {
	this->dbfilename = this->name + ".db";
}

//creates the file using the Berkeley DB based on this architecture
void HeapFile::create(void){
	this->db_open (DB_CREATE | DB_EXCL);
	SlottedPage* page = this->get_new();//get new returns a SlottedPage
	delete page;
}

//deletes the file
void HeapFile::drop(void){
	close();
	//stdio
	//remove(this->dbfilename.c_str());
	Db db_handle(_DB_ENV, 0);
	db_handle.remove(this->dbfilename.c_str(), nullptr, 0);
}

//opens the file using db_open
void  HeapFile::open(void){
	this->db_open();
}

//closes the database
void HeapFile::close(void){
	if (!this->closed)
	{
		this->db.close(0);
		this->closed = true;
	}
}

//gets a new page to use with Berkeley db, by passing a blank to db for mem managing
SlottedPage* HeapFile::get_new(void){
	char block[SlottedPage::BLOCK_SZ];//changed from DB_BLOCK_SZ
	std::memset(block, 0, sizeof(block));
	Dbt data(block, sizeof(block));

	int block_id = ++this->last;
	Dbt key(&block_id, sizeof(block_id));
	
	//write out an empty block and read it back in so Berkeley DB is managing the memory.
	this->db.put(nullptr, &key, &data, 0); //write it out with initialization applied
	this->db.get(nullptr, &key, &data, 0);
	
	return new SlottedPage(data, this->last, true);
}

//gets the page at the block_id given
SlottedPage* HeapFile::get(BlockID block_id){
	char block[SlottedPage::BLOCK_SZ];
	std::memset(block, 0, sizeof(block));
	Dbt data(block, sizeof(block));
	Dbt key(&block_id, sizeof(block_id));
	this->db.get(nullptr, &key, &data,0);
	return new SlottedPage(data, block_id, false);
}

//puts a block in the database
void HeapFile::put(DbBlock* block){
	BlockID id = block->get_block_id();
	Dbt key(&id, sizeof(id));
	this->db.put(nullptr, &key, block->get_block(), 0);
}

//returns a list of all used blockIDs, similar to RecordIDs
BlockIDs* HeapFile::block_ids() const {
	BlockIDs* temp = new BlockIDs();//vector
	for(u_int32_t i = 1; i <=this->last; i++)
		temp->push_back(i);
	return temp;
	
}

uint32_t HeapFile::get_block_count() {
	DB_BTREE_STAT* stat;
	this->db.stat(nullptr, &stat, DB_FAST_STAT);
	return stat->bt_ndata;
}

//uses Berkeley db to initialize a database based on the file. 0 is the common flag.
void HeapFile::db_open(uint flags){
	if (!this->closed){
		return;
	}
	this->db.set_re_len(SlottedPage::BLOCK_SZ);
	this->db.open(NULL, (this->dbfilename).c_str(), NULL, DB_RECNO, flags,0);
	this->last = flags ? 0 : get_block_count();
	this->closed = false;
}

/**
 * @class HeapTable - Heap storage engine (implementation of DbRelation)
 */

HeapTable::HeapTable(Identifier table_name, ColumnNames column_names, ColumnAttributes column_attributes ) :
		DbRelation(table_name, column_names, column_attributes), file(table_name) {
}

/*
 * creates the heapFile and allocates the first slottedpage block
 */
void HeapTable::create(){ //
	this->file.create();
}

/*
 * removes the heapfile from existance
 */
void HeapTable::drop(){
   this->file.drop();
}

/*
 * opens the heapfile for performing reads/writes
 */
void HeapTable::open(){
	this->file.open();
}

/*
 * closes heapfile when done
 */
void HeapTable::close(){
	this->file.close();
}

/*
 * checks to see if relation exists, if it doesn't, catches 
 * DbException and creates the table
 */
void HeapTable::create_if_not_exists() {
	try {
		this->open();
	} catch (DbException& exc) {
		this->create();
	}
}

/*
 * inserts a row into relation validates row before inserting
 * @param ValuDict* to insert into realation
 * @return Handle with BlockID and RecordID to Row
 */
Handle HeapTable::insert(const ValueDict* row){
	this->open();
	
	ValueDict* full_row = validate(row);
	Handle handle = append(full_row);
	
	delete full_row;

	return handle;
}

/*
 * changes existing row. unimplemented stub
 * @param Handle contining blockID and RecordID to update, and ValuDict 
 * contining the new data
 */
void HeapTable::update(const Handle handle, const ValueDict* new_values){
	throw DbRelationError("Not implemented");
}

/*
 * deletes a row from relation. unimplented stub
 * @param Handle with BlockID and RecordID of row to delete
 */
void HeapTable::del(const Handle handle){
	this->open();
	std::unique_ptr<SlottedPage> slotted_page(this->file.get(handle.first));
	slotted_page->del(handle.second);
	this->file.put(slotted_page.get());
}

/*
 * returns an empty filter on rows (ie returns all rows)
 * @return a Handles List, of Handles pointing to each row returned
 */
Handles* HeapTable::select() {
	this->open();
	
	Handles* handles = new Handles();
	BlockIDs* blockIDs = file.block_ids();
	for (auto const& blockID: *blockIDs){
		SlottedPage *block = file.get(blockID);
		RecordIDs* record_ids = block->ids();
		for (auto const& record_id: *record_ids)
		{
			handles->push_back(Handle(blockID, record_id));
		}
		delete record_ids;
		delete block;
	}
	
	delete blockIDs;
	
	return handles;
}
/*
 * eventually will implement where here filter here
 * @param the where clause on which to filter rows
 * @return the Handles which point to the desired rows
 */
Handles* HeapTable::select(const ValueDict* where){
	this->open();
	
	Handles* handles = new Handles();
	BlockIDs* blockIDs = file.block_ids();
	
	for (auto const& blockID: *blockIDs){
		SlottedPage *block = file.get(blockID);
		RecordIDs* record_ids = block->ids();
		
		for (auto const& record_id: *record_ids)
		{
			Handle handle(blockID, record_id);
			
			if (selected(handle, where))
			{
				handles->push_back(handle);
			}
		}
		
		delete record_ids;
		delete block;
	}
	
	delete blockIDs;
	
	return handles;
}

// Refine another selection
Handles* HeapTable::select(Handles *current_selection, const ValueDict* where) {
    Handles* handles = new Handles();
    for (auto const& handle: *current_selection)
        if (selected(handle , where))
            handles->push_back(handle);
    return handles;
}

// See if the row at the given handle satisfies the given where clause
bool HeapTable::selected(Handle handle, const ValueDict* where) {
	this->open();
	
	if (where == nullptr)
		return true;
	
	unique_ptr<ValueDict> row(this->DbRelation::project(handle, where));
	
	for(auto const& pair : *where)
	{
		if (row->find(pair.first) == row->end() || row->at(pair.first) != pair.second)
		{
			return false;
		}
	}
	
	return true;
}

/*
 * for selecting all columns from row (ie SQL SELECT *)
 * @param the handle of the desired row to project
 * @return a ValueDict containing the entire row of data
 */
ValueDict* HeapTable::project(Handle handle){
   return project(handle, &this->column_names);
}	

/*
 * for selecting a subset of columns from relation (ie SQL SELECT ID, name)
 * @param  handle  Handle to the desired row
 * @param  ColumnNames a list of columns to project
 */
ValueDict* HeapTable::project(Handle handle, const ColumnNames* column_names){
	this->open();
	
	BlockID block_id = handle.first;
	RecordID record_id = handle.second;
	
	SlottedPage* block = this->file.get(block_id);
	Dbt* data = block->get(record_id);
	ValueDict* row = this->unmarshal(data);
	
	delete data;
	delete block;
	
	if (column_names == nullptr || column_names->empty()) {
		return row;
	}

	ValueDict* toReturn = new ValueDict();
	
	for (auto const& column_name: *column_names)
	{
		if (row->find(column_name) == row->end())
		{
			throw DbRelationError("table does not have column named '" + column_name + "'");
		}
		
		(*toReturn)[column_name] = (*row)[column_name];
	}
	
	delete row;
	
	return toReturn;
}

/*
 * checks if row to insert is a valid row before insertion
 * @param ValuDict row, to be checked
 * @return a fully fleshed out row
 */
ValueDict* HeapTable::validate(const ValueDict* row) const {
	ValueDict* full_row = new ValueDict();
	for (auto const& column_name: this->column_names) {
		ValueDict::const_iterator column = row->find(column_name);
		if (column == row->end()) {
			throw DbRelationError("Dont know how to handle NULLs, defaults, etc. yet");
		} else {
			full_row->insert(pair<Identifier,Value>(column->first, column->second));
		}
	}
	return full_row;
}

/*
 * adds a row to relation (helper function for insert)
 * @param ValuDict ro to add 
 * @return Handle pointing to record which was inserted
 */
Handle HeapTable::append(const ValueDict* row){
	this->open();
	
	Dbt* data = this->marshal(row);
	SlottedPage* block = this->file.get(this->file.get_last_block_id());
	RecordID record_id;
	
   	try {
		record_id = block->add(data);
	} catch (DbBlockNoRoomError) {
		block = this->file.get_new();
		record_id = block->add(data);
	}
	this->file.put(block);
	
	delete block;
	delete[] (char*)data->get_data();
	delete data;
	
	return Handle(this->file.get_last_block_id(), record_id);
}

/*
 * return the bits to go in the file
 *caller responsible for freeing the returned Dbt and its enclosed ret->get_data(), author: K. Lundeen
 * @param row to convert
 * @return Dbt* containing raw bitstring of row data
*/
Dbt* HeapTable::marshal(const ValueDict* row) const{
	char *bytes = new char[DbBlock::BLOCK_SZ]; // more than we need (we insist that one row fits into DbBlock::BLOCK_SZ)
    uint offset = 0;
    uint col_num = 0;
    for (auto const& column_name: this->column_names) {
    	ColumnAttribute ca = this->column_attributes[col_num++];
    	ValueDict::const_iterator column = row->find(column_name);
		Value value = column->second;

		if (ca.get_data_type() == ColumnAttribute::DataType::INT) {
			if (offset + 4 > DbBlock::BLOCK_SZ - 4)
				throw DbRelationError("row too big to marshal");
			*(int32_t*) (bytes + offset) = value.n;
			offset += sizeof(int32_t);
		} else if (ca.get_data_type() == ColumnAttribute::DataType::TEXT) {
			u_long size = value.s.length();
			if (size > UINT16_MAX)
				throw DbRelationError("text field too long to marshal");
			if (offset + 2 + size > DbBlock::BLOCK_SZ)
				throw DbRelationError("row too big to marshal");
			*(u16*) (bytes + offset) = size;
			offset += sizeof(u16);
			memcpy(bytes+offset, value.s.c_str(), size); // assume ascii for now
			offset += size;
        } else if (ca.get_data_type() == ColumnAttribute::DataType::BOOLEAN) {
            if (offset + 1 > DbBlock::BLOCK_SZ - 1)
                throw DbRelationError("row too big to marshal");
            *(uint8_t*) (bytes + offset) = (uint8_t)value.n;
            offset += sizeof(uint8_t);
		} else {
			throw DbRelationError("Only know how to marshal INT, TEXT, and BOOLEAN");
		}
	}
	char *right_size_bytes = new char[offset];
	memcpy(right_size_bytes, bytes, offset);
	delete[] bytes;
	Dbt *data = new Dbt(right_size_bytes, offset);
	return data;
}

/*
 * used by project to convert bits in Dbt structure into 
 * typed, ValueDict row
 * @param Dbt* data to be converted back to ValueDict Format
 * @return Valudict row created from data in Dbt* data
 */
ValueDict* HeapTable::unmarshal(Dbt* data) const {
    ValueDict *row = new ValueDict();
    Value value;
    char *bytes = (char*)data->get_data();
    uint offset = 0;
    uint col_num = 0;
    for (auto const& column_name: this->column_names) {
    	ColumnAttribute ca = this->column_attributes[col_num++];
		value.data_type = ca.get_data_type();
    	if (ca.get_data_type() == ColumnAttribute::DataType::INT) {
    		value.n = *(int32_t*)(bytes + offset);
    		offset += sizeof(int32_t);
    	} else if (ca.get_data_type() == ColumnAttribute::DataType::TEXT) {
    		u16 size = *(u16*)(bytes + offset);
    		offset += sizeof(u16);
    		char buffer[DbBlock::BLOCK_SZ];
    		memcpy(buffer, bytes+offset, size);
    		buffer[size] = '\0';
    		value.s = string(buffer);  // assume ascii for now
            offset += size;
        } else if (ca.get_data_type() == ColumnAttribute::DataType::BOOLEAN) {
            value.n = *(uint8_t*)(bytes + offset);
            offset += sizeof(uint8_t);
    	} else {
            throw DbRelationError("Only know how to unmarshal INT, TEXT, and BOOLEAN");
    	}
		(*row)[column_name] = value;
    }
    return row;
}

void test_set_row(ValueDict &row, int a, string b) {
	row["a"] = Value(a);
	row["b"] = Value(b);
	row["c"] = Value(a%2 == 0);  // true for even, false for odd
}

bool test_compare(DbRelation &table, Handle handle, int a, string b) {
	ValueDict *result = table.project(handle);
	Value value = (*result)["a"];
	if (value.n != a) {
		delete result;
		return false;
	}
	value = (*result)["b"];
	delete result;
    if (value.s != b)
        return false;
    value = (*result)["c"];
    if (value.n != (a%2 == 0))
        return false;
    return true;
}

// test function -- returns true if all tests pass
bool test_heap_storage() {
	ColumnNames column_names;
	column_names.push_back("a");
	column_names.push_back("b");
	column_names.push_back("c");
	ColumnAttributes column_attributes;
	ColumnAttribute ca(ColumnAttribute::INT);
	column_attributes.push_back(ca);
	ca.set_data_type(ColumnAttribute::TEXT);
	column_attributes.push_back(ca);
    ca.set_data_type(ColumnAttribute::BOOLEAN);
    column_attributes.push_back(ca);
    HeapTable table1("_test_create_drop_cpp", column_names, column_attributes);
	cout << "test_heap_storage: " << endl;
    table1.create();
    cout << "create ok" << endl;
    table1.drop();  // drop makes the object unusable because of BerkeleyDB restriction -- maybe want to fix this some day
    cout << "drop ok" << endl;
    
	HeapTable table("_test_data_cpp", column_names, column_attributes);
    table.create_if_not_exists();
    cout << "create_if_not_exsts ok" << endl;

    ValueDict row;
	string b = "alkjsl;kj; as;lkj;alskjf;laks df;alsdkjfa;lsdkfj ;alsdfkjads;lfkj a;sldfkj a;sdlfjk a";
	test_set_row(row, -1, b);
    table.insert(&row);
    cout << "insert ok" << endl;

    Handles* handles = table.select();
    if (!test_compare(table, (*handles)[0], -1, b))
        return false;
    cout << "select/project ok " << handles->size() << endl;
	delete handles;

    Handle last_handle;
    for (int i = 0; i < 1000; i++) {
        test_set_row(row, i, b);
        last_handle = table.insert(&row);
    }
    handles = table.select();
    if (handles->size() != 1001)
        return false;
    int i = -1;
    for (auto const& handle: *handles)
        if (!test_compare(table, handle, i++, b))
            return false;
    cout << "many inserts/select/projects ok" << endl;
	delete handles;

    table.del(last_handle);
    handles = table.select();
    if (handles->size() != 1000)
        return false;
    i = -1;
    for (auto const& handle: *handles)
        if (!test_compare(table, handle, i++, b))
            return false;
    cout << "del ok" << endl;
 
    table.drop();
	delete handles;
    return true;
}

