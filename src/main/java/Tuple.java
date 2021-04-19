import java.util.*;

class Tuple implements Comparable<Tuple> {

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

    @Override
    public int compareTo(Tuple o) {
        Object pk = this.getPrimeKey();
        Class pkClass = pk.getClass();
        return ((Comparable) pkClass.cast(pk)).compareTo(pkClass.cast(o.getPrimeKey()));
    }

    // TODO override this with something useful
    @Override
    public String toString() {
        Object pk = this.getPrimeKey();
        Class cl = pk.getClass();
        return cl.cast(pk).toString();
    }

    // public static void main(String[] args) {
    // Hashtable<String, Object> a = new Hashtable<String, Object>();
    // Hashtable<String, Object> b = new Hashtable<String, Object>();
    // a.put("1", "1");
    // b.put("1", "2");
    // Tuple t = new Tuple("1", a);
    // Tuple n = new Tuple("1", b);
    // System.out.println(t.compareTo(n));
    // System.out.println(t.compareTo(t));

    // }
}