/* sql5300.cpp
 * This program runs a basic sql interpreter line
 * It requires one command line argument, the existing
 * r/w directory in which to create the DB environment.
 *
 * @author Jacob Mouser
 * @author Brian Doersh
 */

#include "SQLParser.h"
#include "db_cxx.h"
#include <string.h>
#include <iostream>
#include "heap_storage.h"

using namespace std;
using namespace hsql;

// Declare constants
const char *DBFILE = "quail.db"; //name of DB file
const unsigned int BLOCK_SIZE = 4096;

string getExprString(Expr *expr); //initialize function in adv.

/* 
 * converst hsql::expr into sql operator expression (ie >=<, NOT)
 * @param expr operator to convert
 * @return sql operator expression in string format 
 */ 
string getOpString(Expr* expr) {
  string toReturn = "";
  if (expr==NULL) return "null";
  if (expr->opType == Expr::NOT) toReturn += "NOT";
  toReturn += getExprString(expr->expr) + " "; // left side of expr
  switch(expr->opType) {
    case Expr::SIMPLE_OP:
      toReturn += expr->opChar;
      break;
    case Expr::AND:
      toReturn += "AND";
      break;
    case Expr::OR:
      toReturn += "OR";
      break;
    default:
      break;
  }
  //print right side of operator
  if(expr->expr2!=NULL) toReturn += " " + getExprString(expr->expr2);
  return toReturn;
}

/*
 * converts hsal::expr to sql string
 * @param expr expression to convert
 * @return string of sql statment
  */
string getExprString(Expr* expr) {
  string toReturn;
  switch (expr->type) {
    case kExprStar:
      toReturn += "*";
      break;
    case kExprColumnRef:
      if (expr->table != NULL)
        toReturn += string(expr->table) + ".";
    case kExprLiteralString:
      toReturn += expr->name;
      break;
    case kExprLiteralFloat:
      toReturn += to_string(expr->fval);
      break;
    case kExprLiteralInt:
      toReturn += to_string(expr->ival);
      break;
    case kExprFunctionRef:
      toReturn += string(expr->name) + "?" + expr->expr->name;
      break;
    case kExprOperator:
      toReturn += getOpString(expr);
      break;
    default:
      toReturn += "exprNotKnown";
  }
  if (expr->alias != NULL)
    toReturn += string(" AS ") + expr->alias;
  return toReturn;
}

/* converts hsql::TableRef object into a sql string
 * @param table to be broken down
 * @return sql string to print
 */
string getTableInfo(TableRef* table) {
   string toReturn = "";
   switch (table->type) {
      case kTableName:
         toReturn += table->name;
         if (table->alias !=NULL) toReturn += " AS " + string(table->alias);
         break;
      case kTableSelect:
         return "here";
      case kTableJoin:
         toReturn += getTableInfo(table->join->left);
         if (table->join->type == kJoinCross ||
            table->join->type == kJoinInner) {
            toReturn += " JOIN " + getTableInfo(table->join->right);
         }
         if (table->join->type == kJoinLeft ||
            table->join->type == kJoinLeftOuter ||
            table->join->type == kJoinOuter) {
            toReturn += " LEFT JOIN " + getTableInfo(table->join->right);
         }
         if (table->join->type == kJoinRight ||
            table->join->type == kJoinRightOuter) {
            toReturn += " RIGHT JOIN  " + getTableInfo(table->join->right);
         }
         if (table->join->type == kJoinNatural) {
            toReturn += " NATURAL JOIN " + getTableInfo(table->join->right);
         }
         if (table->join->condition != NULL) toReturn += " ON " + 
            getExprString(table->join->condition);
         break;
      case kTableCrossProduct: {
        int count = 0;
        for (TableRef* list : *table->list) {
            if (count > 0) toReturn += ", ";
            toReturn += getTableInfo(list);
            count +=1;
        }
      }
        break;
    }
    return toReturn;
}

/*
 * Execute function for converting hsql::statement into a c string
 * @param stmt hsqlSQLStatment for converting
 * @return string for printing statement
 */
string execute(const SQLStatement* stmt) {
   string toReturn = "";
   switch (stmt->type()) {
      //select portion of code
      case kStmtSelect: {
          toReturn += "SELECT ";
          const SelectStatement* selSTMT = (const SelectStatement*)stmt;
          int count = 0;
          for(Expr* expr : *selSTMT->selectList) {
             if (count > 0) toReturn += ", ";
             toReturn += getExprString(expr);
             count += 1;
          }
          if (selSTMT->fromTable != nullptr) {
             toReturn += " FROM " + getTableInfo(selSTMT->fromTable);
          }
          if (selSTMT->whereClause != nullptr) {
             toReturn += " WHERE " + getExprString(selSTMT->whereClause);
          }
      }
          break;
      //create portion of code
      case kStmtCreate: {
         toReturn += "CREATE TABLE ";
         const CreateStatement* creSTMT = (const CreateStatement*)stmt;
         if (creSTMT->ifNotExists) toReturn += "IF NOT EXISTS ";
         toReturn += string(creSTMT->tableName) + "(";
         int count = 0;
         for(ColumnDefinition* col : *creSTMT->columns) {
            if (count > 0) toReturn += ", ";
            string type;
            switch (col->type) {
               case 0: 
                  type = "UNKNOWN";
                  break;
               case 1:
                  type = "TEXT";
                  break;
               case 2:
                  type = "INT";
                  break;
               case 3:
                  type = "DOUBLE";
                  break;
               default:
                  type = "NOTATYPE";
            }
            toReturn += string(col->name) + " " + type;
            count += 1;
         }
         toReturn += ")";
      }
         break;
      default:
         toReturn += "Cannot Print statement";
         break;
   }
   return toReturn;
}   
 
// main methood with 1 arg (directory path), drives execute
int main(int argc, char *argv[]) {
   if (argc <=1) {
      cerr << "Incorrect usage" << endl;
      exit(1);
   }

   // get directory from argument
   const char *dirPath = argv[1];

   DbEnv env(0U);
   env.set_message_stream(&cout);
   env.set_error_stream(&cerr);
   try {
    env.open(dirPath, DB_CREATE | DB_INIT_MPOOL, 0); 
   } catch (DbException& exc) {
    cerr << "(sql5300: " << exc.what() << ")";
    exit(1);
   }
   _DB_ENV = &env;
   
   //print system running info
   cout << "(sql5300: running with database evniroment at "  << dirPath << ")" << endl;
   //create a user input loop
   string input;
   while(1) {
      // get user input
      cout << "SQL> ";
      getline(cin,input);

      //if response is "quit" then exit
      if (input == "quit") {
         break;
      }
      if (input == "test") {
         cout << "test_heap_storage: " << (test_heap_storage() ? "ok" : "failed") << endl;
         continue;
      }
    

      // parse result
      SQLParserResult* result = SQLParser::parseSQLString(input);       

      //Check that parse tree is valid
      if(result->isValid()) {
         //call our execute function
         for  (uint i = 0; i < result->size(); i++) {
            
            string parsedStatement = execute(result->getStatement(i));

            //print result
            cout << parsedStatement << endl;
         }
      } else {
         cout << "Invalid SQL: " << input << endl;
      }
 
   }
   return 0;
}
