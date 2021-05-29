import java.io.Serializable;
import java.util.*;

public class Index implements Serializable{

    private Table context;
    private Vector<RangeWrapper> dimentions;
    // linear vector to simulate the 
    transient Vector<TuplePointer> data;

    public Index(
        Table context,
        String[] columnNames, 
        Hashtable<String, String> columnsNameType, 
        Hashtable<String, String> columnMin, 
        Hashtable<String, String> columnMax) throws ClassNotFoundException, DBAppException{

        this.context = context;
        this.dimentions = new Vector<>();
        this.data = new Vector<>((int) Math.pow(10, columnNames.length));

        // TODO write the loading and freeing methods of the  data

        for(String columnName : columnNames)
            this.dimentions.add(
                new RangeWrapper(
                    columnName,
                    columnsNameType.get(columnName), 
                    columnMin.get(columnName), 
                    columnMax.get(columnName)
                )
            );

        this._fill();
    }

    private void _fill(){
    
    }

    public void insert(Tuple tuple){

    }

    // TODO rethink of the return type
    public Vector<TuplePointer> search(){
        return null;
    }

    public void load(){

    }

    public void free(){

    }
}
