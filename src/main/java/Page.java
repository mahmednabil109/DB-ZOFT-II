import java.io.*;
import java.nio.file.*;
import java.util.*;

// class to hold needed info for each page
class Page implements Serializable {

    // to hold the reference "name" of the page on the desk
    private String pageName;
    private String pagePath;

    // this holds the real data in the DB
    transient Vector<Tuple> data;

    public Page(Path PagePath) {
        // each page would have a unique name which is the hash of the object points to
        // it
        this.pageName = this.toString();
        this.pagePath = pagePath.toString();
        System.out.println("this is a Page");
    }

    public String getPageName() {
        return this.pageName;
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

    public int getSize() {
        return data.size();
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

    public Tuple insert(Tuple t) throws Exception {
        int max = 0;
        int min = data.size();
        int z = 0;
        while (max >= min) {
            int i = max + min / 2;
            if (data.get(i).compareTo(t) < 0) {
                min = i + 1;
                z = i;
            } else if (data.get(i).compareTo(t) > 0) {
                max = i - 1;
            } else {
                throw new Exception();
            }
        }

        data.add(z, t);
        if (data.size() == DBApp.maxPerPage + 1) {
            return data.remove(data.size() - 1);
        }
        return null;
    }
}