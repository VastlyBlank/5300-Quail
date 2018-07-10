/**
 * @file heap_storage.cpp - concrete implementation of the heap_storage.h prototypes
 * SlottedPage: DbBlock
 * HeapFile: DbFile
 * HeapTable: DbRelation
 *
 * @authors Brian Doersch, Jacob Mouser, Kevin Lundeen
 */

#include "heap_storage.h"
#include<cstring>

using namespace std;
/**
 * @class SlottedPage - heap file implementation of DbBlock
 *
 */
typedef u_int16_t u16;
//constructor , author K.Lundeen
SlottedPage::SlottedPage(Dbt &block, BlockID block_id, bool is_new=false):DbBlock(block, block_id, is_new) {
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
Dbt* SlottedPage::get(RecordID record_id){
	u16 size, loc;
	get_header(size, loc, record_id);
	if (loc ==0)
		return NULL;
	Dbt* tempBlock = new Dbt(this->address(loc),size);//create a new Dbt structure from data and size	
	return tempBlock;
}

void SlottedPage::put(RecordID record_id, const Dbt &data) throw(DbBlockNoRoomError){
	u16 size, loc;
	get_header(size, loc, record_id);
	u16 new_size = data->get_size();
	if (new_size > size){
		u16 needed = new_size-size;
		if (!has_room(new_size))
			throw DbBlockNoRoomError("not enough room for resized record");
		slide(loc+new_size, loc+size);//slide left for more room
		memcpy(this->address(loc-needed), data->get_data(), new_size);//loc is right-justified
	}else{
		memcpy(this->address(loc), data->get_data(),new_size);
		slide(loc+new_size, loc+size);//slide right to collapse to create more end_free in page
	}
	get_header(size,loc, record_id);//update the header size/offset
	put_header(record_id, size, loc);	
	
}

//removes a record by setting its size and loc to 0
void SlottedPage::del(RecordID record_id){
	u16 size, loc;
	get_header(size, loc, record_id);
	put_header(record_id,0,0);
	slide(loc, loc+size);	
}

//sequence of all non-deleted record ids
RecordIDs* SlottedPage::ids(void){
	RecordIDs* records = new RecordIDs();//vector
	for (int i = 1; i <= this->num_records, i++){
		u16 size,loc;
		get_header(size, loc, i);//loads up size and loc from id i
		if (loc != 0)//0 is a deleted record, do not include.
			records.push_back(i);
	}
	return records;
}


//get the size and offset for the given id, with id of zero being the block header.
void SlottedPage::get_header(u16 &size, u16 &loc, RecordID id=0){
	size = get_n(4*id);//num_records
	loc = get_n(4*id+2);//end of free space
}

//store the size and offset for given id. For id of zero, store the block header, author K.Lundeen
void SlottedPage::put_header(RecordID id=0, u16 size=0, u16 loc=0){
	if (id == 0) {//called the put_header() version, default params
		size = this->num_records;
		loc = this->end_free;
	}
	put_n(4*id,size);
	put_n(4*id +2, loc);
}

//returns true if there is enough room in the SlottedPage for the new record of size
bool SlottedPage::has_room(u16 size){
	u16 free = this->end_free - ((this->num_records + 1) *4);//subtract the new header room from free space as well
	return (size <= free);
}

//compacts or makes room for data so additions can always be done in the same place
//start < end implies removal of data, start > end implies extra data needs to be added in
//start < end will shift data to right, start > end will be a left shift.
void SlottedPage::slide(u16 start, u16 end){
	u16 shift = end - start;
	if (shift == 0)
		return;
	//sliding
	memcpy(this->address(end_free+1), this->address(end_free+1+shift),shift);

	//fix headers
	u16 size, loc;
	for (int RecordID : ids()){
		get_header(size, loc, RecordID);
		if (loc <= start) {
			loc+=shift;//new location
			put_header(RecordID, size, loc);
		}
	}
	this->end_free+=shift;
	put_header();//updates with new end_free	
}

//get 2-byte int at given offset in block, author K.Lundeen
u16 SlottedPage::get_n(u16 offset){
	return *(u16*)this->address(offset);
}

//put a 2-byte int at given offset in block, author K.Lundeen

void SlottedPage::put_n(u16 offset, u16 n){
	*(u16*)this->address(offset)=n;
}

//make void pointer for given offset in the data block, author K.Lundeen
void* SlottedPage::address(u16 offset){
	return (void*)((char*)this->block.get_data()+offset);
}

/**
 * @class HeapFile - heap file implementation of DbFile
 */

void HeapFile::create(void){
	this->db_open(bdb.DB_CREATE | bdb.DB.EXCL);
	DbBlock block = this->get_new();//get new returns a SlottedPage
	this->put(block);
}

void HeapFile::drop(void){
	close();
	//stdio
	remove(this->dbfilename);
}

void  HeapFile::open(void){
	this->db_open();
	//block size
}

void HeapFile::close(void){
	this->db.close();
	closed = true;
}

SlottedPage* HeapFile::get_new(void){
	char block[BLOCK_SZ];//changed from DB_BLOCK_SZ
	std::memset(block, 0, sizeof(block));
	Dbt data(block, sizeof(block));

	int block_id = ++this->last;
	Dbt key(&block_id, sizeof(block_id));
	
	//write out an empty block and read it back in so Berkeley DB is managing the memory.
	SlottedPage* page = new SlottedPage(data, this->last, true);
	this->db.put(nullptr, &key, &data, 0); //write it out with initialization applied
	this->db.get(nullptr, &key, &data, 0);
	return page;
}

SlottedPage* HeapFile::get(BlockID block_id){
	char block[BLOCK_SZ];
	std::memset(block, 0, sizeof(block));
	Dbt data(block, sizeof(block));
	Dbt key(&block_id, sizeof(block_id));
	this->db.get(nullptr, &key, &data,0);
	return new SlottedPage(data, block_id, false);
}

void HeapFile::put(DbBlock* block){
	BlockID id = block->get_block_id();
	Dbt key(&id, sizeof(id);
	this->db.put(nullptr, &key, block->get_block(), 0);
}

BlockIDs* HeapFile::block_ids(){
	BlockIDs temp = new BlockIDs();//vector
	for(int i = 1; i <=this->last; i++)
		temp->push_back(i);
	return temp;
	
}
void HeapFile::db_open(uint flags=0){
	if (!this->closed){
		return;
	}
	this->db = bdb.DB();
	this->db.set_re_len(this->block_size);
	this->dbfilename=_DB_ENV + this->name + ".db";
	this->db.open(dbfilename, None, DB_RECNO, flags);
	//stat line
	this->last=db.stat(bdb.DB_FAST_STAT)['ndata'];
	this->closed = false;
}

/**
 * @class HeapTable - Heap storage engine (implementation of DbRelation)
 */

HeapTable::HeapTable(Identifier table_name, ColumnNames column_names, ColumnAttributes column_attributes){
this->file = new HeapFile(table_name);
}

HeapTable::~HeapTable() {

}

void HeapTable::create(){
	this->file->create();
}

void HeapTable::drop(){
	this->file->drop();
}

void HeapTable::open(){
	this->file->open();
}

void HeapTable::close(){
	this->file->close();
}

void HeapTable::create_if_not_exists() {
	try {
		this->open();
	} catch (DbException& exc) {
		this->create();
	}
}

Handle HeapTable::insert(const ValueDict* row){
this->open();
	return this->append(this->validate(row));
}

void HeapTable::update(const Handle handle, const ValueDict* new_values){
	cerr << "FIXME" << endl;
}

void HeapTable::del(const Handle handle){
	cerr << "FIXME" << endl;
}

//Handles* HeapTable::select(){

//}

//
Handles* HeapTable::select(const ValueDict* where){
	Handles* handles = new Handles();
	BlockIDs* blockIDs = file->block_ids();
	for (auto const& blockID: *blockIDs){
		SlottedPage *block = file->get(blockID);
		RecordIDs* record_ids = block->ids();
		for (auto const& record_id: *record_ids)
			handles->push_back(Handle(blockID, record_id));
		delete record_ids;
		delete block;
	}
	delete blockIDs;
	return handles;
}

//ValueDict* HeapTable::project(Handle handle){
//   cerr << "FIXME" << endl;
//}	

ValueDict* HeapTable::project(Handle handle, const ColumnNames* column_names){
	BlockID block_id = handle.first;
	RecordID record_id = handle.second;
	SlottedPage* block = this->file->get(block_id);
   Dbt* data = block->get(record_id);
	ValueDict* row = this->unmarshal(data);
   ValueDict* toReturn;
	if (column_names == NULL) {
		return row;
	} else {
		toReturn = new ValueDict();
		for (auto const& column_name: *column_names) {
			ValueDict::const_iterator column = row->find(column_name);
         toReturn->insert(pair<Identifier,Value>(column->first, column->second));
		}
	}
	return toReturn;
}


ValueDict* HeapTable::validate(const ValueDict* row) {
	ValueDict* full_row = new ValueDict;
	for (auto const& column_name: this->column_names) {
		ValueDict::const_iterator column = row->find(column_name);
		if (column == row->end()) {
			cerr << "Dont know how to handle NULLs, defaults, etc. yet" << endl;
		} else {
			full_row->insert(pair<Identifier,Value>(column->first, column->second));
		}
	}
	return full_row;
}

Handle HeapTable::append(const ValueDict* row){
	Dbt* data = this->marshal(row);
	SlottedPage* block = this->file->get(this->file->get_last_block_id());
	RecordID record_id;
   	try {
		record_id = block->add(data);
	} catch (DbBlockNoRoomError) {
		block = this->file->get_new();
		record_id = block->add(data);
	}
	this->file->put(block);
	return Handle(this->file->get_last_block_id(), record_id);
}

//return the bits to go in the file
//caller responsible for freeing the returned Dbt and its enclosed ret->get_data(), author: K. Lundeen
Dbt* HeapTable::marshal(const ValueDict* row){
	char *bytes = new char[DbBlock::BLOCK_SZ]; //more than needed
	uint offset = 0;
	uint col_num = 0;
	for (auto const& column_name: this->column_names) {
		ColumnAttribute ca = this->column_attributes[col_num++];
		ValueDict::const_iterator column = row->find(column_name);
		Value value = column->second;
		if (ca.get_data_type()==ColumnAttribute::DataType::INT){
			*(int32_t*)(bytes+offset) = value.n;
			offset += sizeof(int32_t);
		}else if (ca.get_data_type()==ColumnAttribute::DataType::TEXT){
			uint size = value.s.length();
			*(u16*) (bytes+offset)=size;
			offset += sizeof(u16);
			memcpy(bytes+offset, value.s.c_str(), size); //assume ASCII
			offset += size;
		}else {
			throw DbRelationError("Only know how to marshal INT/TEXT");
		}	
	}
	char *right_size_bytes = new char[offset];
	memcpy(right_size_bytes, bytes, offset);
	delete[] bytes;
	Dbt *data = new Dbt(right_size_bytes, offset);
	return data;
}

ValueDict* HeapTable::unmarshal(Dbt* data){
	ValueDict *row = new ValueDict();
	const char* bytes = (const char*)data->get_data();
	uint offset = 0;
	uint col_num = 0;
	for (auto const& column_name: this->column_names) {
      Value value;
		ColumnAttribute ca = this->column_attributes[col_num++];
		if (ca.get_data_type()==ColumnAttribute::DataType::INT){
			value.n = *(u16*)(bytes+offset);
         row->insert(pair<Identifier,Value>(column_name, value));
			offset += sizeof(int32_t);
		}else if (ca.get_data_type()==ColumnAttribute::DataType::TEXT){ 
			uint size = *(u16*) (bytes+offset);
			offset += sizeof(u16);
			memcpy((void*)value.s.c_str(), (bytes+offset), size); //assume ASCII
         row->insert(pair<Identifier,Value>(column_name, value));
			offset += size;
		}else {
			throw DbRelationError("Only know how to unmarshal INT/TEXT");
		}	
	}
   delete[] bytes;
	return row;
}



