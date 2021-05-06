import java.io.Serializable;
import java.util.*;

class Tuple implements Comparable<Tuple>, Serializable {

    // the name of the primaryKey to compare based on it
    String primaryKeyName;
    // list of obects to store data of the row
    Hashtable<String, Object> data;

    public Tuple(String primaryKeyName, Hashtable<String, Object> rowData) {
        this.primaryKeyName = primaryKeyName;
        this.data = (Hashtable<String, Object>) rowData.clone();
    }

    Object getPrimeKey() {
        return this.get(this.primaryKeyName);
    }

    Object get(String columnName) {
        return data.get(columnName);
    }

    void put(String columnName, Object columnValue){
        data.put(columnName, columnValue);
    }

    @Override
    public int compareTo(Tuple o) {
        Object pk = this.getPrimeKey();
        Class pkClass = pk.getClass();
        return ((Comparable) pkClass.cast(pk)).compareTo(pkClass.cast(o.getPrimeKey()));
    }

    @Override
    public String toString() {
        StringBuilder tupleString = new StringBuilder("");
        
        for(Map.Entry<String, Object> entries : this.data.entrySet()){
            if(entries.getKey().equals(this.primaryKeyName))
                tupleString.append(" [PK] " + entries.getValue().toString() + " ");
            else
                tupleString.append(" " + entries.getValue() + " ");        
        }
        return tupleString.toString();
    }
}