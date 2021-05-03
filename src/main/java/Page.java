import java.io.*;
import java.nio.file.*;
import java.util.*;

//TODOS cache the size , min ,  max of data to avoid loading the data every time

// class to hold needed info for each page
class Page implements Serializable {

    // to hold the reference "name" of the page on the desk
    private String pageName;
    private String pagePath;

    private int size;
    // to hold the min, max, size
    public Object min, max;

    // this holds the real data in the DB
    transient Vector<Tuple> data;

    public Page(String pagePath) {
        // init the min, max, size
        min = max = null;
        size = 0;
        // each page would have a unique name which is the hash of the object that
        // points to
        this.pageName = this.toString();
        this.pagePath = pagePath;
        this.data = new Vector<Tuple>();
    }

    private int _searchTuple(Tuple tuple, boolean update) throws DBAppException {
        int min = 0;
        int max = data.size() - 1;

        if (update) {
            // implementation of the lower_bound
            while (max > min) {
                int i = min + (max - min) / 2;
                Tuple t = data.get(i);
                if (t.compareTo(tuple) >= 0) {
                    max = i ;
                } else {
                    min = i + 1;
                }
            }
        } else {
            // implemenation of the upper_bound
            while (max > min) {
                int i = min + (max - min) / 2;
                Tuple t = data.get(i);
                // System.out.printf("(%d, %d) , %s <=> [%s] %d\n", min, max, t.toString(), tuple.toString(), t.compareTo(tuple));
                if (t.compareTo(tuple) <= 0) {
                    min = i + 1;
                } else {
                    max = i;
                }
            }
            
            // System.out.println(min);
            if(data.lastElement().compareTo(tuple) < 0){
                min ++;
                // System.out.println("[in] " + min);
            }

            if(!DBApp.ALLOW_DUBLICATES){
                if(!(min >= data.size()) && data.get(min).equals(tuple))
                    throw new DBAppException();
                if(!(min == 0) && data.get(min - 1).equals(tuple))
                    throw new DBAppException();
                if(!(min >= data.size()-1) && data.get(min + 1).equals(tuple))
                    throw new DBAppException();
            }
        }
        return min;
    }

    public Tuple insert(Tuple tuple, boolean next) throws DBAppException {
        Tuple res = null;

        int index = this._searchTuple(tuple, false);
        
        data.add(index, tuple);

        if (data.size() == DBApp.maxPerPage + 1) {
            if (next)
                res = data.remove(data.size() - 1);
            else
                res = data.remove(0);
            this.size--;
        }

        this._updateCachedValues();

        // ![DEBUG]
        // System.out.printf("inserted %s at index %d\n", tuple.toString(), index);
        // System.out.println("=====================");
        // for(Tuple t : data)
        //     System.out.printf("inserted %s\n", t.toString());
        // System.out.println("=====================");

        return res;
    }

    public Vector<Tuple> insertAndSplit(Tuple tuple)throws DBAppException {
        
        Tuple last = this.insert(tuple, true);

        Vector<Tuple> result = new Vector<Tuple>();
        int maxPerPage = DBApp.maxPerPage;
        
        while (this.data.size() != maxPerPage / 2) {
            result.add(this.data.remove(maxPerPage / 2));
        }
        result.add(last);
        this._updateCachedValues();
        
        return result;
    }

    public void setData(Vector<Tuple> newData) {
        this.data = (Vector<Tuple>) newData.clone();
        this._updateCachedValues();
    }

    public String getPageName() {
        return this.pageName;
    }

    public void update(Tuple pk, Hashtable<String, Object> colNameVlaue) throws DBAppException {
        int index = this._searchTuple(pk, true);
        // System.out.printf("[LOG] the position is %d\n", index);
        if (index != -1) {
            while(pk.compareTo(this.data.get(index)) == 0 && index < this.data.size()){
                for (Map.Entry<String, Object> entries : colNameVlaue.entrySet())
                    this.data.get(index).put(entries.getKey(), entries.getValue());
                index++;
            }
        }
    }

    public void add(Tuple tuple) {
        this.data.add(tuple);
        this._updateCachedValues();
    }

    public Tuple remove(int index) {
        Tuple tuple = this.data.remove(index);
        this._updateCachedValues();
        return tuple;
    }

    // TODO uncomment free load save
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

    public int size() {
        return size;
    }

    public void load() {
        this._deserialize();
    }

    private void _updateCachedValues() {
        // update the size, min, max to avoid loading the pages every time
        this.size = data.size();
        this.min = data.firstElement().get(data.firstElement().primaryKeyName);
        this.max = data.lastElement().get(data.lastElement().primaryKeyName);
    }

    private void _serialize() {
        if (data == null)
            return;
        try {
            FileOutputStream file = new FileOutputStream(Paths.get(pagePath, pageName).toString());
            ObjectOutputStream out = new ObjectOutputStream(file);
            out.writeObject(data);
            out.close();
            file.close();
        } catch (IOException e) {
            e.printStackTrace();
            System.out.printf("[ERROR] something habbens when saving page <%s : %s>\n", pagePath, pageName);
        }

    }

    private void _deserialize() {
        if (data != null)
            return;
        try {
            FileInputStream file = new FileInputStream(Paths.get(pagePath, pageName).toString());
            ObjectInputStream in = new ObjectInputStream(file);
            data = (Vector<Tuple>) in.readObject();
            in.close();
            file.close();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
            System.out.printf("[ERROR] something habbens when loading page <%s : %s>\n", pagePath, pageName);
        }
    }

}