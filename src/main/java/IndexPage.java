import java.io.Serializable;
import java.util.Collections;
import java.util.Hashtable;
import java.util.Map;
import java.util.Vector;

public class IndexPage implements Serializable{

    private IndexPage overflowPage;
    private String pathToPages;
    private String pageName;
    private Table context;
    transient private Vector<TuplePointer> data;

    public IndexPage(Table context, String path){
        this.context = context;
        this.overflowPage = null;
        this.data = new Vector<>();
        this.pathToPages = path;
        this.pageName = this.toString();
    }

    public IndexPage insert(TuplePointer tp){
        if(this.data.size() == DBApp.maxPerIndexPage){
            this.overflowPage = new IndexPage(this.context, this.pathToPages);
            this.overflowPage.load();
            this.overflowPage.insert(tp);
            this.overflowPage.save();
        }else{
            this.data.add(tp);
        }
        return this;
    }

    public Vector<Tuple> get(Vector<SQLTerm> columns) throws DBAppException{
        Vector<Tuple> res = new Vector<>();
        Hashtable<Integer, Vector<Integer>> pointerPack = new Hashtable<>();

        Collections.sort(this.data, (a, b) -> a.pagePos - b.pagePos);
        
        for(TuplePointer tp : this.data){
            Vector<Integer> _tmp = (
                pointerPack.containsKey(tp.pagePos) ? pointerPack.get(tp.pagePos) : new Vector<>()
            );
            _tmp.add(tp.tuplePos);
            pointerPack.put(tp.pagePos, _tmp);
        }

        for(Map.Entry<Integer, Vector<Integer>> entries: pointerPack.entrySet()){
            int pagePos = entries.getKey();
            Vector<Integer> tuples = entries.getValue();
            Page page = this.context.buckets.get(pagePos);
            page.load();
            for(int t : tuples){
                Tuple tuple = page.data.get(t);
                if(Utils.doesTupleMatch(tuple, columns))
                    res.add(tuple);
            }
            page.free();
        }

        if(this.overflowPage != null) res.addAll(this.overflowPage.get(columns));
        return res;
    }

    public void load() {

    }

    public void free() {

    }

    public void save(){

    }

    public static void main(String args[]){
        // Vector<TuplePointer> v = new Vector<>();
        // v.add(new TuplePointer(10 , 2));
        // v.add(new TuplePointer(1 , 2));
        // v.add(new TuplePointer(2 , 2));
        // v.add(new TuplePointer(0 , 2));
        // v.add(new TuplePointer(2 , 2));
        // v.add(new TuplePointer(3 , 2));
        // Collections.sort(v, (a, b) -> a.pagePos - b.pagePos);
        // System.out.println(v);

        // Hashtable<Integer, Integer> tmp = new Hashtable<>();
        // tmp.put(11, 2);
        // System.out.println(tmp.contains(2));
        // System.out.println(tmp.containsKey(11));
    }
}