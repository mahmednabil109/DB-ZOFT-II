import java.io.Serializable;
import java.util.*;

public class TuplePointer implements Serializable, Observer{
    int pagePos, tuplePos;
    IndexPage parent;

    public TuplePointer(int pagePos, int tuplePos){
        this.pagePos = pagePos;
        this.tuplePos = tuplePos;
    }

    public void setParent(IndexPage page){
        this.parent = page;
    }

    public void remove() throws DBAppException{
        if(this.parent == null){
            System.out.printf("[ERROR] misconfiguartion of parent Page!\n");
            throw new DBAppException();
        }else{
            // TODO make sure to load the IndexPage before removing this
            this.parent.remove(this);
        }
        
    }

    public String toString(){
        return  "( " + this.pagePos + ", " + this.tuplePos + " )"; 
    }

    @Override
    public void update(Observable tuple, Object flags) {
        if(flags == null){
            this.parent.remove(this);
        }else{
            Vector<Integer> pos = (Vector<Integer>) flags;
            this.pagePos = pos.get(0);
            this.tuplePos = pos.get(1);
        }
    }
}