import java.io.Serializable;
import java.util.*;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

class Tuple extends Observable implements Comparable<Tuple>, Serializable {

    // the name of the primaryKey to compare based on it
    String primaryKeyName;
    // list of obects to store data of the row
    Hashtable<String, Object> data;
    // save the indexes and the places
    Vector<Object> placeInIndex;

    public Tuple(String primaryKeyName, Hashtable<String, Object> rowData) {
        this.primaryKeyName = primaryKeyName;
        this.data = (Hashtable<String, Object>) rowData.clone();
        placeInIndex = new Vector<>();

    }

    Object getPrimeKey() {
        return this.get(this.primaryKeyName);
    }

    Object get(String columnName) {
        return data.get(columnName);
    }

    void put(String columnName, Object columnValue) {
        data.put(columnName, columnValue);
    }

    @Override
    public int compareTo(Tuple o) {
        Object pk = this.getPrimeKey();
        Class pkClass = pk.getClass();
        return ((Comparable) pkClass.cast(pk)).compareTo(pkClass.cast(o.getPrimeKey()));
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.getPrimeKey());
    }

    @Override
    public boolean equals(Object obj){
        if(this == obj) return true;
        else if(obj instanceof Tuple){
            Tuple other = (Tuple) obj;
            return Objects.equals(this.getPrimeKey(), other.getPrimeKey());
        }
        else return false;
    }

    @Override
    public String toString() {
        StringBuilder tupleString = new StringBuilder("");

        for (Map.Entry<String, Object> entries : this.data.entrySet()) {
            if (entries.getKey().equals(this.primaryKeyName))
                tupleString.append(" [PK] " + entries.getValue().toString() + " ");
            else
                tupleString.append(" " + entries.getValue() + " ");
        }
        return tupleString.toString();
    }

    public void change(int pagePos, int tuplePos) {
        setChanged();
        notifyObservers(IntStream.of(pagePos, tuplePos).boxed().collect(Collectors.toCollection(Vector::new)));
    }

    public void delete() {
        setChanged();
        notifyObservers(null);
    }

    public static void main(String args[]) {
        Hashtable<String, Object> htb = new Hashtable<>();
        htb.put("s1", "12");

        Tuple t1 = new Tuple("s1", htb);
        Tuple t2 = new Tuple("s1", htb);

        Set<Tuple> s1 = new HashSet<>();
        Set<Tuple> s2 = new HashSet<>();

        s1.add(t1);
        s2.add(t2);

        System.out.println(t1 == t2);
        System.out.println(t1.equals(t2));
        s1.retainAll(s2);
        System.out.println(s1);
    }
}