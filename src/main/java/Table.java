import java.io.*;
import java.util.*;

class Table implements Serializable{

    // name of the table "relation"
    String name;
    // hashtabel to store the columns names and types
    Hashtable htbColumnsNameType;
    // number of records to alse calculate the number of pages stored
    Long size;
    // dll that holds the references "names" of the pages on the desk
    Deque<PageInfo> buckets;
    //

    
    public Table(String name, Hashtable columsInfos){
        // initalize the table
        // check if there aready pages directory
        // if not create one
        System.out.println("this is a Table");
    }

    public void insert(Hashtable arg){}

    public void delete(Hashtable arg){}

    public void update(Hashtable arg){}
}