/* sql5300.cpp
 * This program runs a basic sql interpreter line
 * It requires one command line argument, the existing
 * r/w directory in which to create the DB environment.
 *
 * @author Jacob Mouser, Brian Doersh, Kevin Lundeen
 */

#include <stdio.h>
#include <stdlib.h>
#include <cstring>
#include <iostream>
#include <string>
#include <cassert>
#include "db_cxx.h"
#include "SQLParser.h"
#include "ParseTreeToString.h"
#include "SQLExec.h"

using namespace std;
using namespace hsql;

// Declare constants
DbEnv* _DB_ENV;

/*
 * we allocate and initialize the _DB_ENV global
 */
void initialize_environment(char *envHome);

 
// main methood with 1 arg (directory path), drives execute
int main(int argc, char *argv[]) {
   
	if (argc <=1) {
		cerr << "Incorrect usage" << endl;
		exit(1);
	}
	initialize_environment(argv[1]);

   //create a user input loop
	while(1) {
		// get user input
		cout << "SQL> ";
		string input;
		getline(cin,input);
		
		if (input.length() == 0)
			continue;  // blank line -- just skip
		//if response is "quit" then exit
		if (input == "quit") {
			break;
		}
		//test our code up to date
		if (input == "test") {
			cout << "test_heap_storage: " << (test_heap_storage() ? "ok" : "failed") << endl;
			continue;
		}


		// parse result
		SQLParserResult* parse = SQLParser::parseSQLString(input);       

		//Check that parse tree is valid
		if(parse->isValid()) {
			//call our execute function
			for  (uint i = 0; i < parse->size(); i++) {
				const SQLStatement *statement = parse->getStatement(i);
				try {
					cout << ParseTreeToString::statement(statement) << endl;
					QueryResult *result = SQLExec::execute(statement);
					cout << *result << endl;
					delete result;
				} catch (SQLExecError& e) {
					cout << "Error: " << e.what() << endl;
				}
			}
		}
		else {
			cout << "Invalid SQL: " << input << endl;
			cout << parse->errorMsg() << endl;
		}
		delete parse;
	}
	return 0;
}

void initialize_environment(char *envHome) {
	cout << "(sql5300: running with database environment at " << envHome
		 << ")" << endl;

	DbEnv *env = new DbEnv(0U);
	env->set_message_stream(&cout);
	env->set_error_stream(&cerr);
	try {
		env->open(envHome, DB_CREATE | DB_INIT_MPOOL, 0);
	} catch (DbException &exc) {
		cerr << "(sql5300: " << exc.what() << ")" << endl;
		exit(1);
	}
	_DB_ENV = env;
	initialize_schema_tables();
}
