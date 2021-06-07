import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.file.Paths;
import java.util.Hashtable;
import java.util.Map;
import java.util.Vector;

public class IndexPage implements Serializable{

    private IndexPage overflowPage;
    private String pageHash;
    private String pathToPages;
    private String pageName;
    private Table context;
    private Index index;
    private int posInIndex;
    transient Vector<TuplePointer> data;

    
    public IndexPage(Table context, Index index, int pos, String path){
        this.posInIndex = pos;
        this.index = index;
        this.pageHash = this.toString();
        this.context = context;
        this.overflowPage = null;
        this.data = new Vector<>();
        this.pathToPages = path;
        this.pageName = this.toString();
        this.save();
    }

    public int indexOf(String hash,TuplePointer tp){
        if(!hash.equals(this.pageHash)) 
            return this.overflowPage.indexOf(hash,tp);
        return this.data.indexOf(tp);
    }

    public String insert(TuplePointer tp){
        if(this.data.size() == DBApp.maxPerIndexPage){
            this.overflowPage = new IndexPage(this.context, this.index, this.posInIndex, this.pathToPages);
            this.overflowPage.load();
            String hash = this.overflowPage.insert(tp);
            // System.out.println("A: " + this.posInIndex + " ," + hash);
            this.overflowPage.saveAndFree();
            return hash;
        }else{
            this.data.add(tp);
            tp.setParent(this);
        }
        // System.out.println("A: " + this.posInIndex + " ," + this.pageHash);

        return this.pageHash;
    }

    public TuplePointer get(String hash, int place){
        if(hash.equals(this.pageHash))
            return data.get(place);
        else{
            System.out.println("what hash: " + hash + ", " + this.pageHash);
            return overflowPage.get(hash, place);
        }
    }


    public Vector<Tuple> get(Vector<SQLTerm> columns) throws DBAppException{
        Vector<Tuple> res = new Vector<>();
        System.out.println("[LOG] HEY IAM HERE");
        Hashtable<Integer, Vector<Tuple>> pointerPack = new Hashtable<>();
        
        // grouping the tuples by pages to minimize the IO operations
        for(TuplePointer tp : this.data){
            Tuple _tuple = Utils.wrapTuplePointer(tp);
            int tPagePos = this.context.getPagePos(_tuple);
            Vector<Tuple> _tmp = (
                pointerPack.containsKey(tPagePos) ? pointerPack.get(tPagePos) : new Vector<>()
            );
            _tmp.add(_tuple);
            pointerPack.put(tPagePos, _tmp);
        }

        for(Map.Entry<Integer, Vector<Tuple>> entries: pointerPack.entrySet()){
            Integer tPagePos = entries.getKey();
            Vector<Tuple> tuples = entries.getValue();
            
            Page page = this.context.buckets.get(tPagePos);

            if(page == null){
                System.out.printf("[ERROR] this page dose not exists %s\n", pageHash);
                throw new DBAppException();
            }
            page.load();
            for(Tuple tr : tuples){
                Tuple tuple = page.getTupleByWrapper(tr);
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
    
    public String getPathToPage() {
        return this.pathToPages;
    }

    public String getPageName(){
        return this.pageName;
    }

    public void remove(int index) throws DBAppException{
        this.data.remove(index);
        // handle over flowpages
        if(this.data.size() == 0){
            if(this.overflowPage != null){
                // switch between this page and the linked over flow
                this.index._delete(this.overflowPage);
                this.data = this.overflowPage.data;
                this.pageName = this.overflowPage.pageName;
                this.overflowPage = this.overflowPage.overflowPage;
            }else{
                // Tdelete the current file
                this.index.data.set(this.posInIndex, null);
                this.index._delete(this);
                ;
            }
        }
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

    public int size(String hash) {
        if(hash.equals(pageHash)) return data.size();
        return overflowPage.size(hash);
    }
}