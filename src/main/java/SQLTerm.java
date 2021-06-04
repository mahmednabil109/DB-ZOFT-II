public class SQLTerm {
    public String _strTableName;
    public String _strColumnName;
    public String _strOperator;
    public Object _objValue;

    public SQLTerm() {
    }

    public SQLTerm(String tableName, String columnName, String operator, Object value){
        this._strTableName = tableName;
        this._strColumnName = columnName;
        this._strOperator = operator;
        this._objValue = value;
    }

}