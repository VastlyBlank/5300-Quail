/**
 * @file ParseTreeToString.cpp - SQL unparsing class implementation
 * @author Kevin Lundeen
 * @see "Seattle University, CPSC5300, Summer 2018"
 */
#include "ParseTreeToString.h"
using namespace std;
using namespace hsql;

const vector<string> ParseTreeToString::reserved_words = {
"COLUMNS", "SHOW", "TABLES",
"ADD","ALL","ALLOCATE","ALTER","AND","ANY","ARE","ARRAY","AS","ASENSITIVE","ASYMMETRIC","AT",
                  "ATOMIC","AUTHORIZATION","BEGIN","BETWEEN","BIGINT","BINARY","BLOB","BOOLEAN","BOTH","BY","CALL",
                  "CALLED","CASCADED","CASE","CAST","CHAR","CHARACTER","CHECK","CLOB","CLOSE","COLLATE","COLUMN",
                  "COMMIT","CONNECT","CONSTRAINT","CONTINUE","CORRESPONDING","CREATE","CROSS","CUBE","CURRENT",
                  "CURRENT_DATE","CURRENT_DEFAULT_TRANSFORM_GROUP","CURRENT_PATH","CURRENT_ROLE","CURRENT_TIME",
                  "CURRENT_TIMESTAMP","CURRENT_TRANSFORM_GROUP_FOR_TYPE","CURRENT_USER","CURSOR","CYCLE","DATE",
                  "DAY","DEALLOCATE","DEC","DECIMAL","DECLARE","DEFAULT","DELETE","DEREF","DESCRIBE","DETERMINISTIC",
                  "DISCONNECT","DISTINCT","DOUBLE","DROP","DYNAMIC","EACH","ELEMENT","ELSE","END","END-EXEC","ESCAPE",
                  "EXCEPT","EXEC","EXECUTE","EXISTS","EXTERNAL","FALSE","FETCH","FILTER","FLOAT","FOR","FOREIGN",
                  "FREE","FROM","FULL","FUNCTION","GET","GLOBAL","GRANT","GROUP","GROUPING","HAVING","HOLD","HOUR",
                  "IDENTITY","IMMEDIATE","IN","INDICATOR","INNER","INOUT","INPUT","INSENSITIVE","INSERT","INT",
                  "INTEGER","INTERSECT","INTERVAL","INTO","IS","ISOLATION","JOIN","LANGUAGE","LARGE","LATERAL",
                  "LEADING","LEFT","LIKE","LOCAL","LOCALTIME","LOCALTIMESTAMP","MATCH","MEMBER","MERGE","METHOD",
                  "MINUTE","MODIFIES","MODULE","MONTH","MULTISET","NATIONAL","NATURAL","NCHAR","NCLOB","NEW","NO",
                  "NONE","NOT","NULL","NUMERIC","OF","OLD","ON","ONLY","OPEN","OR","ORDER","OUT","OUTER","OUTPUT",
                  "OVER","OVERLAPS","PARAMETER","PARTITION","PRECISION","PREPARE","PRIMARY","PROCEDURE","RANGE",
                  "READS","REAL","RECURSIVE","REF","REFERENCES","REFERENCING","REGR_AVGX","REGR_AVGY","REGR_COUNT",
                  "REGR_INTERCEPT","REGR_R2","REGR_SLOPE","REGR_SXX","REGR_SXY","REGR_SYY","RELEASE","RESULT","RETURN",
                  "RETURNS","REVOKE","RIGHT","ROLLBACK","ROLLUP","ROW","ROWS","SAVEPOINT","SCROLL","SEARCH","SECOND",
                  "SELECT","SENSITIVE","SESSION_USER","SET","SIMILAR","SMALLINT","SOME","SPECIFIC","SPECIFICTYPE",
                  "SQL","SQLEXCEPTION","SQLSTATE","SQLWARNING","START","STATIC","SUBMULTISET","SYMMETRIC","SYSTEM",
                  "SYSTEM_USER","TABLE","THEN","TIME","TIMESTAMP","TIMEZONE_HOUR","TIMEZONE_MINUTE","TO","TRAILING",
                  "TRANSLATION","TREAT","TRIGGER","TRUE","UESCAPE","UNION","UNIQUE","UNKNOWN","UNNEST","UPDATE",
                  "UPPER","USER","USING","VALUE","VALUES","VAR_POP","VAR_SAMP","VARCHAR","VARYING","WHEN","WHENEVER",
                  "WHERE","WIDTH_BUCKET","WINDOW","WITH","WITHIN","WITHOUT","YEAR"};

bool ParseTreeToString::is_reserved_word(string candidate) {
    for(auto const& word: reserved_words)
        if (candidate == word)
            return true;
    return false;
}

/* 
 * converst hsql::expr into sql operator expression (ie >=<, NOT)
 * @param expr operator to convert
 * @return sql operator expression in string format 
 */ 
string ParseTreeToString::get_operator_string(const Expr* expr) {
	string toReturn = "";
	if (expr == NULL) return "null";
	
	if (expr->opType == Expr::NOT) toReturn += "NOT";
	toReturn += get_expression_string(expr->expr) + " "; // left side of expr
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
			toReturn += "???";
			break;
	}
	//print right side of operator
	if(expr->expr2!=NULL) toReturn += " " + get_expression_string(expr->expr2);
	return toReturn;
}

/*
 * converts hsal::expr to sql string
 * @param expr expression to convert
 * @return string of sql statment
  */
string ParseTreeToString::get_expression_string(const Expr* expr) {
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
			toReturn += get_operator_string(expr);
			break;
		default:
			toReturn += "???";
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
string ParseTreeToString::get_table_information(const TableRef* table) {
	string toReturn = "";
	switch (table->type) {
		case kTableName:
			toReturn += table->name;
			if (table->alias !=NULL) toReturn += " AS " + string(table->alias);
			break;
		case kTableSelect:
			return "here";
		case kTableJoin:
			toReturn += get_table_information(table->join->left);
			if (table->join->type == kJoinCross || table->join->type == kJoinInner) {
				toReturn += " JOIN " + get_table_information(table->join->right);
			}
			else if (table->join->type == kJoinLeft || table->join->type == kJoinLeftOuter || table->join->type == kJoinOuter) {
				toReturn += " LEFT JOIN " + get_table_information(table->join->right);
			}
			else if (table->join->type == kJoinRight || table->join->type == kJoinRightOuter) {
				toReturn += " RIGHT JOIN  " + get_table_information(table->join->right);
			}
			else if (table->join->type == kJoinNatural) {
				toReturn += " NATURAL JOIN " + get_table_information(table->join->right);
			}
			if (table->join->condition != NULL) {
				toReturn += " ON " + get_expression_string(table->join->condition);
			}
			break;
		case kTableCrossProduct:
			int count = 0;
			for (TableRef* list : *table->list) {
				if (count > 0) toReturn += ", ";
				toReturn += get_table_information(list);
				count +=1;
			}
			break;
	}
	return toReturn;
}

string ParseTreeToString::column_definition(const ColumnDefinition *col) {
	string toReturn(col->name);
	switch (col->type) {
		case 0: 
			toReturn += " UNKNOWN";
			break;
		case 1:
			toReturn += " TEXT";
			break;
		case 2:
			toReturn += " INT";
			break;
		case 3:
			toReturn += " DOUBLE";
			break;
		default:
			toReturn += " NOTATYPE";
	}
	return toReturn;
}

string ParseTreeToString::select(const SelectStatement *stmt) {
	string toReturn = "SELECT ";
	int count = 0;
	for(Expr* expr : *stmt->selectList) {
		if (count > 0) toReturn += ", ";
		toReturn += get_expression_string(expr);
		count += 1;
	}
	if (stmt->fromTable != nullptr) {
		toReturn += " FROM " + get_table_information(stmt->fromTable);
	}
	if (stmt->whereClause != nullptr) {
		toReturn += " WHERE " + get_expression_string(stmt->whereClause);
	}
	
	return toReturn;
}

string ParseTreeToString::insert(const InsertStatement *stmt) {
    return "INSERT ...";
}

string ParseTreeToString::create(const CreateStatement *stmt) {
	string ret("CREATE ");
	if (stmt->type == CreateStatement::kTable) {
		ret += "TABLE ";
		if (stmt->ifNotExists)
			ret += "IF NOT EXISTS ";
		ret += string(stmt->tableName) + " (";
		bool doComma = false;
		for (ColumnDefinition *col : *stmt->columns) {
			if (doComma)
				ret += ", ";
			ret += column_definition(col);
			doComma = true;
		}
		ret += ")";
	} else if (stmt->type == CreateStatement::kIndex) {
		ret += "INDEX ";
		ret += string(stmt->indexName) + " ON ";
		ret += string(stmt->tableName) + " USING " + stmt->indexType + " (";
		bool doComma = false;
		for (auto const& col : *stmt->indexColumns) {
			if (doComma)
				ret += ", ";
			ret += string(col);
			doComma = true;
		}
		ret += ")";
	} else {
		ret += "...";
	}

	return ret;
}

string ParseTreeToString::drop(const DropStatement *stmt) {
    string  ret("DROP ");
    switch(stmt->type) {
        case DropStatement::kTable:
            ret += "TABLE ";
            break;
		case DropStatement::kIndex:
			ret += string("INDEX ") + stmt->indexName + " FROM ";
			break;
        default:
            ret += "? ";
    }
    ret += stmt->name;
    return ret;
}

string ParseTreeToString::show(const ShowStatement *stmt) {
    string ret("SHOW ");
    switch (stmt->type) {
        case ShowStatement::kTables:
            ret += "TABLES";
            break;
        case ShowStatement::kColumns:
            ret += string("COLUMNS FROM ") + stmt->tableName;
            break;
        case ShowStatement::kIndex:
            ret += string("INDEX FROM ") + stmt->tableName;
            break;
        default:
            ret += "?what?";
            break;
    }
    return ret;
}

/*
 * statement function for converting hsql::statement into a c string
 * @param stmt hsqlSQLStatment for converting
 * @return string for printing statement
 */
string ParseTreeToString::statement(const SQLStatement* stmt) {
	switch (stmt->type()) {
		//select portion of code
		case kStmtSelect:
			return select((const SelectStatement *) stmt);
		case kStmtInsert:
			return insert((const InsertStatement *) stmt);
		//create portion of code
		case kStmtCreate:
			return create((const CreateStatement *) stmt);
		case kStmtDrop:
			return drop((const DropStatement *) stmt);
		case kStmtShow:
			return show((const ShowStatement *) stmt);

		case kStmtError:
		case kStmtImport:
		case kStmtUpdate:
		case kStmtDelete:
		case kStmtPrepare:
		case kStmtExecute:
		case kStmtExport:
		case kStmtRename:
		case kStmtAlter:
		default:
			return "Not implemented";
	}
}   