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

    public Page(Path PagePath) {
        // init the min, max, size
        min = max = null;
        size = 0;
        // each page would have a unique name which is the hash of the object points to
        this.pageName = this.toString();
        this.pagePath = pagePath.toString();
        this.data = new Vector<Tuple>();
    }

    public Tuple insert(Tuple tuple) throws DBAppException {

        int max = 0;
        int min = data.size();
        int index = 0;

        while (max >= min) {
            int i = max + min / 2;
            if (data.get(i).compareTo(tuple) < 0) {
                min = i + 1;
                index = i;
            } else if (data.get(i).compareTo(tuple) > 0) {
                max = i - 1;
            } else {
                throw new DBAppException();
            }
        }

        data.add(index, tuple);

        this.size = data.size();
        this.min = data.firstElement().get(data.firstElement().primaryKeyName);
        this.max = data.lastElement().get(data.lastElement().primaryKeyName);

        if (data.size() == DBApp.maxPerPage + 1) {
            this.size --;
            return data.remove(data.size() - 1);
        }
        return null;
    }

    public String getPageName() {
        return this.pageName;
    }

    public void add(Tuple tuple){
        this.size ++;
        this.data.add(tuple);
    }

    public Tuple remove(int index){
        this.size --;
        return this.data.remove(index);
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

    public int size() {
        return size;
    }


    public void load() {
        this._deserialize();
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