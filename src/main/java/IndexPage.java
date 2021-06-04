import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Map;
import java.util.Set;
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
        this.save();
    }

    public IndexPage insert(TuplePointer tp){
        if(this.data.size() == DBApp.maxPerIndexPage){
            this.overflowPage = new IndexPage(this.context, this.pathToPages);
            this.overflowPage.load();
            this.overflowPage.insert(tp);
            this.overflowPage.saveAndFree();
        }else{
            this.data.add(tp);
            tp.setParent(this);
        }
        return this;
    }

    public Vector<Tuple> get(Vector<SQLTerm> columns) throws DBAppException{
        Vector<Tuple> res = new Vector<>();
        Hashtable<Integer, Vector<Integer>> pointerPack = new Hashtable<>();

        // sorting the data with respect to page position
        Collections.sort(this.data, (a, b) -> a.pagePos - b.pagePos);
        
        // grouping the tuples by pages to minimize the IO operations
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

    public void remove(TuplePointer ptr){
        this.data.remove(ptr);
    }

    public void free() {
        // free the memory occupied be the page in the main memory
        this.data = null;
    }

    public void save() {
        // save the page back to the disk
        this._serialize();
    }

    public void saveAndFree() {
        // save to the disk then free from the main memory
        this.save();
        this.free();
    }

    public void load() {
        this._deserialize();
    }

    private void _serialize(){
        if(data == null)
            return ;
        try{
            FileOutputStream file = new FileOutputStream(Paths.get(this.pathToPages, pageName).toString());
            ObjectOutputStream out = new ObjectOutputStream(file);
            out.writeObject(data);
            out.close();
            file.close();
        } catch (IOException e){
            e.printStackTrace();
            System.out.printf("[ERROR] something habbens when saving IndexPage <%s : %s>\n", pathToPages, pageName);
        }
    }

    private void _deserialize(){
        if(data != null)
            return;
        try {
            FileInputStream file = new FileInputStream(Paths.get(pathToPages, pageName).toString());
            ObjectInputStream in = new ObjectInputStream(file);
            data = (Vector<TuplePointer>) in.readObject();
            in.close();
            file.close();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
            System.out.printf("[ERROR] something habbens when loading IndexPage <%s : %s>\n", pathToPages, pageName);
        }
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
        // Hashtable<Set<String>, String> htb = new Hashtable<>();
        // Set<String> s1 = new HashSet<>();
        // Set<String> s2 = new HashSet<>();
        // s1.add("s1"); s2.add("s1");
        // htb.put(s1, "first");
        // htb.put(s2, "second");
        // System.out.println(htb);
        // System.out.println(htb.get(s2));

    }
}