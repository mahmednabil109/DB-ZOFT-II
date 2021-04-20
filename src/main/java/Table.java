import java.io.*;
import java.net.*;
import java.nio.file.*;
import java.util.*;

class Table implements Serializable {

    // name of the table "relation"
    String name, primaryKeyName;
    // number of records to alse calculate the number of pages stored
    Long size;
    // hashtabel to store the columns names and types
    Hashtable<String, String> htbColumnsNameType, htbColumsMin, htbColumsMax;
    // dll that holds the references "names" of the pages on the desk
    Vector<Page> buckets;
    // path to the pages folder
    private Path pathToPages;

    // hold the space utlization
    private double spaceUtlize;

    // threshold utlization
    private final double thresholdUtliz = 0.75;


    public Table(String name, String primaryKeyName, Hashtable<String, String> columsInfos,
            Hashtable<String, String> columnMin, Hashtable<String, String> columnMax) throws DBAppException {

        // initalize the table
        this.size = 0L;
        this.spaceUtlize = 0.0;
        this.name = name;
        this.primaryKeyName = primaryKeyName;
        this.buckets = new Vector<Page>();
        this.htbColumnsNameType = (Hashtable<String, String>) columsInfos.clone();
        this.htbColumsMin = (Hashtable<String, String>) columnMin.clone();
        this.htbColumsMax = (Hashtable<String, String>) columnMax.clone();

        // initalize the Folder for the pages
        try {
            this.pathToPages = Paths.get(Resources.getResourcePath(), this.name);
        } catch (IOException | URISyntaxException e) {
            e.printStackTrace();
            System.out.println("[ERROR] something wrong habben when trying to read the resources location");
        }

        if (Files.exists(pathToPages)) {
            // it throws an Error as the relation with that name aleardy Exists
            System.out.println("[ERROR] table already exists");
            throw new DBAppException();
        } else {
            try {
                Files.createDirectories(pathToPages);
            } catch (IOException e) {
                e.printStackTrace();
                System.out.println("[ERROR] something wrong habben when trying to make the directory for the pages");
            }
        }

        // save the table object to the disk
        this._saveChanges();
    }

    public void insert(Hashtable<String, Object> colNameValue) {

        Tuple tuple = new Tuple(this.primaryKeyName, colNameValue);

        if (buckets.size() != 0) {

            int index = _searchPages(tuple);

            while ((!(tuple == null)) && index != buckets.size()) {
                Page page = buckets.get(index);
                page.load();
                try {
                    tuple = page.insert(tuple);
                } catch (DBAppException e) {
                    e.printStackTrace();
                    System.out.println("[ERROR] a tuple with the PK already exists !");
                }
                page.saveAndFree();
                index++;
            }
        }

        if (!(tuple == null)) {
            Page page = new Page(pathToPages);
            try {
                page.insert(tuple);
            } catch (Exception e) {
                e.printStackTrace();
            }
            page.saveAndFree();
            buckets.add(buckets.size(), page);
        }
        this.size ++;
        this.spaceUtlize = this.size / (this.buckets.size() * DBApp.maxPerPage * 1.0);
    }

    public void update(Hashtable<Object, Object> arg) {
    }

    public void delete(Hashtable<String, Object> columnNameVlaue) throws DBAppException {
        // if there are no records do nothing
        if (size == 0 || buckets.size() == 0)
            return;

        // if there are no restrictions
        if (columnNameVlaue.size() == 0) {
            for (Page page : buckets) {
                this._delete(page);
            }
            this.buckets.clear();
            return;
        }

        // first we call _searchRows to get the needed rows to be deleted
        Hashtable<Page, Vector<Integer>> rows = _searchRows(columnNameVlaue);
        // if there are no rows that satsifiy the conditions return
        if (rows == null)
            return;

        // delete the records from the pages
        for (Map.Entry<Page, Vector<Integer>> entries : rows.entrySet()) {
            Page page = entries.getKey();
            for (Integer i : entries.getValue()) {
                page.data.remove(i.intValue());
                this.size--;
            }

            // if the page holds no value then delete it
            if (page.data.size() == 0) {
                // remove the pointer of it from the bucket
                buckets.remove(page);
                // and remove it from the disk
                this._delete(page);
            } else {
                // unload the pages from the main memory and save it to the disk
                page.saveAndFree();
            }
        }

        this.spaceUtlize = this.size / (this.buckets.size() * DBApp.maxPerPage * 1.0);
        if(this.spaceUtlize < this.thresholdUtliz)
            this._defragment();
    }

    // gets the rows by matching all the columnNameValues
    private Hashtable<Page, Vector<Integer>> _searchRows(Hashtable<String, Object> columnNameVlaue) {

        Hashtable<Page, Vector<Integer>> result = new Hashtable<Page, Vector<Integer>>();
        String[] entries = (String[]) columnNameVlaue.keySet().toArray();

        // getting the needed pages to work with
        for (Page page : buckets) {
            page.load();
            boolean free = true;
            for (int i = 0; i < page.data.size(); i++) {
                if (page.data.get(i).get(entries[0]).equals(columnNameVlaue.get(entries[0]))) {
                    if (!result.containsKey(page)) {
                        free = false;
                        result.put(page, new Vector<Integer>());
                    }
                    result.get(page).add(i);
                }
            }
            if (free)
                page.free();

        }

        // perform the `and` on the resutl of the columnValue
        for (int j=1; j < entries.length; j++) {
            String entry = entries[j];
            Hashtable<Page, Vector<Integer>> tmp = new Hashtable<Page, Vector<Integer>>();
            for (Map.Entry<Page, Vector<Integer>> items : result.entrySet()) {
                Page page = items.getKey();
                Vector<Integer> indexes = items.getValue();
                boolean free = true;
                for (Integer i : indexes) {
                    if (page.data.get(i).get(entry).equals(columnNameVlaue.get(entry))) {
                        if (!tmp.containsKey(page)) {
                            free = false;
                            tmp.put(page, new Vector<Integer>());
                        }
                        tmp.get(page).add(i);
                    }
                }

                if(free)
                    page.free();
            }
            result = tmp;
        }
        if (result.size() == 0)
            return null;
        return result;
    }

    // BS to get the needed page to insert or update into
    private int _searchPages(Tuple tuple) {
        int index = 0;
        int max = buckets.size();
        int min = 0;

        while (max >= min) {
            int i = max + min / 2;
            Page page = buckets.get(i);
            if (((Comparable) page.min).compareTo(tuple.getPrimeKey()) <= 0) {
                min = i + 1;
                index = i;
            } else {
                max = i - 1;
            }
        }

        return index;
    }

    private void _delete(Page page) throws DBAppException {
        File file = new File(Paths.get(pathToPages.toString(), page.getPageName()).toString());
        if (!file.delete()) {
            System.out.printf("[ERROR] not able to delete page <%s>\n", page.getPageName());
            throw new DBAppException();
        }
    }

    private void _defragment() throws DBAppException{
        
        // god only know why 
        if(this.buckets.size() < 3) return;

        // handle the defragmentation
        for(int i=0; i<buckets.size()-1; i++){
            Page page = buckets.get(i);
            Page nxtPage = buckets.get(i+1);
            if(page.size() == DBApp.maxPerPage)
                continue;
            page.load();
            while(page.size() < DBApp.maxPerPage && nxtPage.size() == 0){
                page.add(nxtPage.remove(0));
            }

            if(nxtPage.size() == 0){
                this._delete(nxtPage);
                this.buckets.remove(i+1);
            }

            if(page.size() != DBApp.maxPerPage)
                i--;
            else
                page.saveAndFree();
            
        }

        // save the last page
        buckets.lastElement().saveAndFree();
        
    }


    // this method is to locate the location of a given

    // this method saves serialize and saves the table object
    // whenever any changes habbens inside the object
    private void _saveChanges() {
        try {
            // serialize the object
            Path p = Paths.get(Resources.getResourcePath(), "tables", this.toString());
            System.out.println(p);
            FileOutputStream file = new FileOutputStream(p.toString());
            ObjectOutputStream out = new ObjectOutputStream(file);
            out.writeObject(this);
        } catch (IOException | URISyntaxException e) {
            e.printStackTrace();
        }
    }
}
