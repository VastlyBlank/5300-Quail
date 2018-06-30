/* sqlshell.cpp
 * This program runs a basic sql interpreter line
 */

#include "SQLParser.h"
#include "db_cxx.h"
#include<string.h>
#include<iostream>
using namespace std;

// Declare constants
const char *DBFILE = "quail.db";
const unsigned int BLOCK_SIZE = 4096;

// execute function
string execute(const hsql::SQLStatement* stmt) {
   hsql::StatementType typeOfStatement = stmt->type();
   if(typeOfStatement == hsql::kStmtSelect) {
      return "SELECT";
   } else if (typeOfStatement == hsql::kStmtCreate) {
      return "CREATE";
   }
}
 
// main methood with 1 arg (directory path)
int main(int argc, char *argv[]) {
   if (argc <=1) {
      cout << "Incorrect usage" << endl;
      exit(1);
   }

   // get directory from argument
   const char *dirPath = argv[1];

   DbEnv env(0U);
   env.set_message_stream(&cout);
   env.set_error_stream(&cerr);
   env.open(dirPath, DB_CREATE | DB_INIT_MPOOL, 0); 

   //create DB
   Db db(&env, 0);
   db.set_message_stream(env.get_message_stream());
   db.set_error_stream(env.get_error_stream());
   db.set_re_len(BLOCK_SIZE); //sets block size to 4k
   //Erases old trees due to DB_TRUNCATE flag
   db.open(NULL, DBFILE, NULL, DB_RECNO, DB_CREATE | DB_TRUNCATE, 0644);
   
   //print system running info
   cout << "(sql5300: running with database evniroment at "  << dirPath << ")" << endl;
   //create a user input loop
   string input;
   hsql::SQLParserResult* result;
   while(1) {
      // get user input
      cout << "SQL> ";
      getline(cin,input);

      //if response is "quit" then exit
      if (input == "quit") {
         break;
      }

      // parse result
      result = hsql::SQLParser::parseSQLString(input);       

      //Check that parse tree is valid
      if(result->isValid()) {
         //call our execute function
         const hsql::SQLStatement* stmt = result->getStatement(0);
         string parsedStatement = execute(stmt);

         //print result
         cout << parsedStatement << endl;

      } else {
         cout << "Invalid SQL: " << input << endl;
      }
 
   }
   delete result;
   return 0;
}


