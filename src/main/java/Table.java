import java.io.*;
import java.net.*;
import java.nio.file.*;
import java.util.*;

import javax.management.ObjectInstance;

class Table implements Serializable {

    // name of the table "relation"
    String name, primaryKeyName;
    // number of records to alse calculate the number of pages stored
    Long size;
    // hashtabel to store the columns names and types
    Hashtable<String, String> htbColumnsNameType, htbColumsMin, htbColumsMax;
    // dll that holds the references "names" of the pages on the desk
    Deque<Page> buckets;
    // path to the pages folder
    private Path pathToPages;

    public Table(String name, String primaryKeyName, Hashtable<String, String> columsInfos,
            Hashtable<String, String> columnMin, Hashtable<String, String> columnMax) throws DBAppException {

        // initalize the table
        this.size = 0L;
        this.name = name;
        this.primaryKeyName = primaryKeyName;
        this.buckets = new ArrayDeque<Page>();
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

    public void insert(Hashtable<Object, Object> arg) {
    }

    public void update(Hashtable<Object, Object> arg) {
    }

    public void delete(Hashtable<String, Object> columnNameVlaue) {
        // if there are no records do nothing
        if(size == 0 || buckets.size() == 0)
            return;

        // if there are no restrictions
        if(columnNameVlaue.size() == 0){
            for(Page page : buckets){
                File file = new File(Paths.get(pathToPages.toString(), page.getPageName()).toString());
                // TODO maybe throw error
                if(!file.delete())
                    System.out.printf("[ERROR] not able to delete page <%s>\n", page.getPageName());
            }
            this.buckets.clear();
            return;
        }

        // first we call _searchRows to get the needed rows to be deleted
        Hashtable<Page, Vector<Integer>> rows = _searchRows(columnNameVlaue);
        // if there are no rows that satsifiy the conditions return
        if(rows == null) return;
        // delete the records from the pages
        for(Map.Entry<Page, Vector<Integer>> entries : rows.entrySet()){
            for(Integer i : entries.getValue())
                entries.getKey().data.remove(i.intValue());
            // TODO rethink about freeing the pages
            entries.getKey().saveAndFree();
        }
        // TODO then we need to handle the after delete actions
        // like delete the empty page 
        // or maybe shift all the rows to minmize the disk usages
    }

    // gets the rows by matching all the columnNameValues
    private Hashtable<Page, Vector<Integer>> _searchRows(Hashtable<String, Object> columnNameVlaue) {
        
        Hashtable<Page, Vector<Integer>> result = new Hashtable<Page, Vector<Integer>>();
        String [] entries = (String []) columnNameVlaue.keySet().toArray();
       
        // getting the needed pages to work with
        for (Page page : buckets) {
            page.load();
            boolean free = true;
            for(int i=0; i<page.data.size(); i++){
                if(page.data.get(i).get(entries[0]).equals(columnNameVlaue.get(entries[0]))){
                    if(!result.containsKey(page)){
                        free = false;
                        result.put(page, new Vector<Integer>());
                    }
                    result.get(page).add(i);
                }
            }
            if(free) page.free();

        }

        // perform the `and` on the resutl of the columnValue
        for(String entry : entries){
            Hashtable<Page, Vector<Integer>> tmp = new Hashtable<Page, Vector<Integer>>();
            for(Map.Entry<Page, Vector<Integer>> items : result.entrySet()){
                Page page = items.getKey();
                Vector<Integer> indexes = items.getValue();
                for(Integer i : indexes){
                    if(page.data.get(i).get(entry).equals(columnNameVlaue.get(entry))){
                        if(!tmp.containsKey(page)){
                            tmp.put(page, new Vector<Integer>());
                        }
                        tmp.get(page).add(i);
                    }
                }
            }
            result = tmp;
        }
        if(result.size() == 0) return null;
        return result;
    }

    private void _defragment() {

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