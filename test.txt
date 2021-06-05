import java.io.Serializable;

public class TuplePointer implements Serializable{
    String pageHash; 
    int tuplePos;
    IndexPage parent;

    public TuplePointer(String pagePos, int tuplePos){
        this.pageHash = pagePos;
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
        return  "( " + this.pageHash + ", " + this.tuplePos + " )"; 
    }
}