import java.io.Serializable;

public class TuplePointer implements Serializable{

    String PKName;
    Object PKValue;
    IndexPage parent;

    public TuplePointer(String primaryKeyName, Object primaryKeyValue){
        this.PKName = primaryKeyName;
        this.PKValue = primaryKeyValue;
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
        return  "<" + this.PKName + ", " + this.PKValue.toString() + " >"; 
    }
}