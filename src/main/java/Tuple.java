import java.util.*;

class Tuple implements Comparable<Tuple>{
    
    // list of obects to store data of the row
    List<Object> data;
    // the position of the primaryKey to compare based on it
    int primayKeyPos;


    public Tuple(int pkp, Object ...args){
        this.primayKeyPos = pkp;
        data = Arrays.asList(args);
        // System.out.println("this is a Tuple!");
    }


    Object getPrimeKey(){
        return data.get(primayKeyPos);
    }

    @Override
    public int compareTo(Tuple o) {
        Object pk = this.getPrimeKey();
        Class pkClass = pk.getClass();
        // TODO remove logs
        // System.out.println(pkClass);
        return ((Comparable) pkClass.cast(pk)).compareTo(pkClass.cast(o.getPrimeKey()));
    }

    // TODO override this with something useful
    @Override
    public String toString(){
        return "" + this.getPrimeKey();
    }
}