import java.io.Serializable;

public class TuplePointer implements Serializable{
    int pagePos, tuplePos;

    public TuplePointer(int pagePos, int tuplePos){
        this.pagePos = pagePos;
        this.tuplePos = tuplePos;
    }

    public String toString(){
        return  "( " + this.pagePos + ", " + this.tuplePos + " )"; 
    }
}