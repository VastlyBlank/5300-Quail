/**
 * @file SQLExec.cpp - implementation of SQLExec class 
 * @author Kevin Lundeen
 * @see "Seattle University, CPSC5300, Summer 2018"
 */
#include <unordered_set>
#include "SQLExec.h"
using namespace std;
using namespace hsql;

// define static data
Tables* SQLExec::tables = nullptr;

// make query result be printable
ostream &operator<<(ostream &out, const QueryResult &qres) {
    if (qres.column_names != nullptr) {
        for (auto const &column_name: *qres.column_names)
            out << column_name << " ";
        out << endl << "+";
        for (unsigned int i = 0; i < qres.column_names->size(); i++)
            out << "----------+";
        out << endl;
        for (auto const &row: *qres.rows) {
            for (auto const &column_name: *qres.column_names) {
                Value value = row->at(column_name);
                switch (value.data_type) {
                    case ColumnAttribute::INT:
                        out << value.n;
                        break;
                    case ColumnAttribute::TEXT:
                        out << "\"" << value.s << "\"";
                        break;
                    default:
                        out << "???";
                }
                out << " ";
            }
            out << endl;
        }
    }
    out << qres.message;
    return out;
}

/*
 * destructor
 */
QueryResult::~QueryResult() {
	
	if (column_names != nullptr)
	{
		delete column_names;
	}
	
	if (column_attributes != nullptr)
	{
		delete column_attributes;
	}
	
	if (rows != nullptr)
	{
		for (auto row: *rows)
		{
			delete row;
		}
		
		delete rows;
	}
}

/**
 * Execute the given SQL statement.
 * @param statement   the Hyrise AST of the SQL statement to execute
 * @returns           the query result (freed by caller)
 */
QueryResult *SQLExec::execute(const SQLStatement *statement) throw(SQLExecError) {
    // FIXME: initialize _tables table, if not yet present
	if (SQLExec::tables == nullptr)
        SQLExec::tables = new Tables();

    try {
        switch (statement->type()) {
            case kStmtCreate:
                return create((const CreateStatement *) statement);
            case kStmtDrop:
                return drop((const DropStatement *) statement);
            case kStmtShow:
                return show((const ShowStatement *) statement);
            default:
                return new QueryResult("not implemented");
        }
    } catch (DbRelationError& e) {
        throw SQLExecError(string("DbRelationError: ") + e.what());
    }
}
/*
 * Pull out column name and attributes from AST's column definition clause
 * @param col                AST column definition
 * @param column_name        returned by reference
 * @param column_attributes  returned by reference
 */
void SQLExec::column_definition(const ColumnDefinition *col, Identifier& column_name,
                                ColumnAttribute& column_attribute) {
	column_name = col->name;

	switch(col->type){
		case ColumnDefinition::TEXT:
			column_attribute.set_data_type(ColumnAttribute::TEXT);
			break;
		case ColumnDefinition::INT:
			column_attribute.set_data_type(ColumnAttribute::INT);
			break;
		default:
			throw SQLExecError("Unrecognized type");
		
	}
}
/**
 * Pull out column names and attributes from AST's create clause
 * @param statement          AST create statement
 * @param column_name        returned by reference
 * @param column_attributes  returned by reference
 */
void SQLExec::column_definitions(const CreateStatement *statement, ColumnNames& column_names, ColumnAttributes& column_attributes)
{
	unordered_set<Identifier> column_names_hash;
	
	for(ColumnDefinition* col : *(statement->columns))
	{
		Identifier column_name;
		ColumnAttribute column_attribute;
		
		column_definition(col, column_name, column_attribute);
		
		if (column_names_hash.find(column_name) != column_names_hash.end())
		{
			throw SQLExecError("duplicate column " + string(statement->tableName) + "." + column_name);
		}
		else
		{
			column_names_hash.insert(column_name);
		}
		
		column_names.push_back(column_name);
		column_attributes.push_back(column_attribute);
	}
}

/*
 * create table
 * @param statement the query statement 
 * @returns the query result
 */
QueryResult *SQLExec::create(const CreateStatement *statement) {

	if (statement->type != CreateStatement::kTable)
	{
		throw SQLExecError("Unrecognized CreateStatement type");
	}
	
	Identifier table_name = statement->tableName;
	ColumnNames column_names;
	ColumnAttributes column_attributes;
	
	column_definitions(statement, column_names, column_attributes);
	
	ValueDict row;
	row["table_name"] = table_name;
	Handle table_handle = SQLExec::tables->insert(&row);
	
	try
	{
		Handles column_handles;
		DbRelation& columns_table = SQLExec::tables->get_table(Columns::TABLE_NAME);
		try
		{
			for (uint i = 0; i < column_names.size(); i++)
			{
				row["column_name"] = column_names[i];
				row["data_type"] = Value(column_attributes[i].get_data_type_string());
				column_handles.push_back(columns_table.insert(&row));
			}
			
			DbRelation& table = SQLExec::tables->get_table(table_name);
			
			if (statement->ifNotExists)
			{
				table.create_if_not_exists();
			}
			else
			{
				table.create();
			}
		}
		catch(...)
		{
			for (const auto &handle : column_handles)
			{
				columns_table.del(handle);
			}
			
			throw;
		}
	}
	catch(...)
	{
		SQLExec::tables->del(table_handle);
		
		throw;
	}
	
	return new QueryResult("created " + table_name);
}

/*
 * drop table
 * @param statement the query statement
 * @returns the query result
 */
QueryResult *SQLExec::drop(const DropStatement *statement) {
	
	if (statement->type != DropStatement::kTable)
	{
		throw SQLExecError("unrecognized DROP type");
	}
	
	Identifier table_name = statement->name;
	if (table_name == Tables::TABLE_NAME || table_name == Columns::TABLE_NAME)
	{
		throw SQLExecError("cannot drop a schema table");
	}
	
	ValueDict where;
	where["table_name"] = Value(table_name);
	
	DbRelation& table = SQLExec::tables->get_table(table_name);
	DbRelation& columns_table = SQLExec::tables->get_table(Columns::TABLE_NAME);
	
	Handles* column_handles = columns_table.select(&where);
	Handles* table_handles = SQLExec::tables->select(&where);
	for (const auto& column_handle: *column_handles)
	{
		columns_table.del(column_handle);
	}
	
	delete column_handles;
	
	table.drop();
	
	SQLExec::tables->del(*(table_handles->begin()));
	
	delete table_handles;
	
	return new QueryResult("dropped " + table_name);
}
/*
 * dealing with the show statement
 * @param statement the query statement
 * @returns the query result
 */
QueryResult *SQLExec::show(const ShowStatement *statement) {
	switch (statement->type)
	{
		case ShowStatement::kTables:
			return show_tables();
		case ShowStatement::kColumns:
			return show_columns(statement);
		case ShowStatement::kIndex:
		default:
			throw SQLExecError("unrecognized SHOW type");
	}
}
/*
 * show tables
 * @returns the query result
 */
QueryResult *SQLExec::show_tables() {
	ColumnNames* column_names = new ColumnNames();
	column_names->push_back("table_name");
	
	ColumnAttributes* column_attrbutes = new ColumnAttributes();
	column_attrbutes->push_back(ColumnAttribute(ColumnAttribute::TEXT));
	
	Handles* handles = SQLExec::tables->select();
	u_long n = handles->size() - 2;
	
	ValueDicts* rows = new ValueDicts();
	
	for (const auto& handle: *handles)
	{
		ValueDict* row = SQLExec::tables->project(handle, column_names);
		Identifier table_name = row->at("table_name").s;

		if (table_name != Tables::TABLE_NAME && table_name != Columns::TABLE_NAME)
		{
			rows->push_back(row);
		}
	}
	
	delete handles;
	
	return new QueryResult(column_names, column_attrbutes, rows, "successfully return " + to_string(n) + " rows");
}
/*
 * show columns 
 * @param statement the query statement
 * @returns the query result
 */
QueryResult *SQLExec::show_columns(const ShowStatement *statement) {
	DbRelation& columns_table = SQLExec::tables->get_table(Columns::TABLE_NAME);
	
	ColumnNames* column_names = new ColumnNames();
	column_names->push_back("table_name");
	column_names->push_back("column_name");
	column_names->push_back("data_type");
	
	ColumnAttributes* column_attributes = new ColumnAttributes();
	column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));
	column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));
	column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));
	
	ValueDict where;
	where["table_name"] = Value(statement->tableName);
	Handles* handles = columns_table.select(&where);
	u_long n = handles->size();
	
	ValueDicts* rows = new ValueDicts();
	
	for (const auto& handle: *handles)
	{
		ValueDict* row = columns_table.project(handle, column_names);
		rows->push_back(row);
	}
	
	delete handles;
	
	return new QueryResult(column_names, column_attributes, rows, "successfully returned " + to_string(n) + " rows");
}

