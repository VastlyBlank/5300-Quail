#include "btree.h"
#include <iostream>
using namespace std;


BTreeIndex::BTreeIndex(DbRelation& relation, Identifier name, ColumnNames key_columns, bool unique)
	: DbIndex(relation, name, key_columns, unique),
	closed(true),
	stat(nullptr),
	root(nullptr),
	file(relation.get_table_name() + "-" + name),
	key_profile() {

	if (!unique)
		throw DbRelationError("BTree index must have unique key");
	// FIXME - what else?!
	build_key_profile();
}

void BTreeIndex::build_key_profile()
{
	ColumnAttributes *cas = new ColumnAttributes;
	cas = relation.get_column_attributes(key_columns);

	for (auto ca : *cas)
		key_profile.push_back(ca.get_data_type());

	delete cas;
}

BTreeIndex::~BTreeIndex() {
	if (stat)
	{
		delete stat;
	}
	if (root)
	{
		delete root;
	}
}

// Create the index.
void BTreeIndex::create() {

	file.create();
	stat = new BTreeStat(this->file, this->STAT, this->STAT + 1, this->key_profile);
	root = new BTreeLeaf(this->file, this->stat->get_root_id(), this->key_profile, true);
	closed = false;

	Handles* handles = relation.select();
	Handles* new_handles = new Handles();

	try {

		for (auto const handle : *handles)
		{
			insert(handle);
			new_handles->push_back(handle);
		}
	}
	catch (DbRelationError)
	{
		file.drop();
		throw;
	}

	delete handles;
	delete new_handles;
	// FIXME
}


// Drop the index.
void BTreeIndex::drop() {
	file.drop();
}

// Open existing index. Enables: lookup, range, insert, delete, update.
void BTreeIndex::open() {

	if (closed)
	{
		file.open();
		stat = new BTreeStat(file, STAT, key_profile);

		if (stat->get_height() == 1)
			root = new BTreeLeaf(file, stat->get_root_id(), key_profile, false);

		else
			root = new BTreeInterior(file, stat->get_root_id(), key_profile, false);

		closed = false;
	}
	// FIXME
}

// Closes the index. Disables: lookup, range, insert, delete, update.
void BTreeIndex::close() {
	file.close();
	delete stat;
	stat = nullptr;
	delete root;
	root = nullptr;
	closed = true;
}

Handles* BTreeIndex::lookup(ValueDict* key_dict) const {
	return _lookup(root, stat->get_height(), tkey(key_dict));
}


void BTreeIndex::del(Handle handle) {
    throw DbRelationError("Don't know how to delete from a BTree index yet");
	// FIXME: Not in scope of M6
}

Handles* BTreeIndex::_lookup(BTreeNode* node, uint height, const KeyValue* key) const {

	Handles* handles = new Handles;

	if (height == 1) //Base Case
	{
		try
		{
			BTreeLeaf* leaf_node = (BTreeLeaf*)node;
			handles->push_back(leaf_node->find_eq(key));
		}
		catch (std::out_of_range) {}

		return handles;
	}
	else    //recursive call
	{
		BTreeInterior* interior_node = (BTreeInterior*)node;
		return _lookup(interior_node->find(key, this->stat->get_height()), this->stat->get_height() - 1, key);
	}
}


Handles* BTreeIndex::range(ValueDict* min_key, ValueDict* max_key) const {
	throw DbRelationError("Don't know how to do a range query on Btree index yet");
	// FIXME
}

// Insert a row with the given handle. Row must exist in relation already.
void BTreeIndex::insert(Handle handle) {

	KeyValue* kv = tkey(relation.project(handle, &key_columns));

	Insertion split_root = _insert(this->root, this->stat->get_height(), kv, handle);

	if (!BTreeNode::insertion_is_none(split_root))
	{
		BlockID rroot = split_root.first;
		KeyValue boundary = split_root.second;

		BTreeInterior *root1 = new BTreeInterior(file, 0, key_profile, true);

		root1->set_first(root->get_id());
		root1->insert(&boundary, rroot);
		root1->save();

		stat->set_root_id(root1->get_id());
		stat->set_height(stat->get_height() + 1);

		stat->save();
		root = root1;
	}
	// FIXME
}

Insertion BTreeIndex::_insert(BTreeNode* node, uint height, const KeyValue* key, Handle handle)
{
	Insertion insertion;

	if (height == 1) //Base Case
	{

		BTreeLeaf* leafNode = (BTreeLeaf*)node;
		insertion = leafNode->insert(key, handle);
		leafNode->save();
		return insertion;
	}

	BTreeInterior* interior = (BTreeInterior*)node;
	insertion = _insert(interior->find(key, height), height - 1, key, handle); //Recursive Call

	if (!BTreeNode::insertion_is_none(insertion))
	{
		insertion = interior->insert(&insertion.second, insertion.first);
		interior->save();
	}
	return insertion;
}

KeyValue *BTreeIndex::tkey(const ValueDict *key) const {
	KeyValue* kv = new KeyValue();

	for (Identifier k : this->key_columns)
	{
		kv->push_back(key->at(k));
	}
	return kv;
	//return nullptr;
	// FIXME
}




