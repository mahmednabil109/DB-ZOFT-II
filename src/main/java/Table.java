import java.io.*;
import java.net.*;
import java.nio.file.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

class Table implements Serializable {

    // finalls
    static final String DateFormate = "yyyy-MM-dd";
    // name of the table "relation"
    String name, primaryKeyName;
    // number of records to alse calculate the number of pages stored
    Long size;
    // number of columns in the table
    int numberOfColumns;
    // hashtabel to store the columns names and types
    Hashtable<String, String> htbColumnsNameType, htbColumnsMin, htbColumnsMax;
    // dll that holds the references "names" of the pages on the desk
    Vector<Page> buckets;
    // path to the pages folder
    private String pathToPages;

    // hashcode to store the hashcode at the creatation
    // becuase java says why not to change it
    private String HASHCODE;
    // hold the space utlization
    private double spaceUtlize;

    // threshold utlization
    private final double thresholdUtliz = 0.75;

    public Table(String name, String primaryKeyName, Hashtable<String, String> columnsInfos,
            Hashtable<String, String> columnMin, Hashtable<String, String> columnMax) throws DBAppException {

        // initalize the table
        this.size = 0L;
        this.spaceUtlize = 0.0;
        this.name = name;
        this.HASHCODE = this.toString();
        this.primaryKeyName = primaryKeyName;
        this.buckets = new Vector<Page>();
        this.numberOfColumns = columnsInfos.size();
        this.htbColumnsNameType = (Hashtable<String, String>) columnsInfos.clone();
        this.htbColumnsMin = (Hashtable<String, String>) columnMin.clone();
        this.htbColumnsMax = (Hashtable<String, String>) columnMax.clone();

        // initalize the Folder for the pages
        try {
            this.pathToPages = Paths.get(Resources.getResourcePath(), this.name).toString();
        } catch (IOException | URISyntaxException e) {
            e.printStackTrace();
            System.out.println("[ERROR] something wrong habben when trying to read the resources location");
        }

        if (Files.exists(Paths.get(pathToPages))) {
            // it throws an Error as the relation with that name aleardy Exists
            System.out.println("[ERROR] table already exists");
            throw new DBAppException();
        } else {
            try {
                Files.createDirectories(Paths.get(pathToPages));
            } catch (IOException e) {
                e.printStackTrace();
                System.out.println("[ERROR] something wrong habben when trying to make the directory for the pages");
            }
        }

        // TODO write this info to the csv also

        // save the table object to the disk
        this._saveChanges();
        this._updateMetaData();

    }

    public void insert(Hashtable<String, Object> colNameValue) throws DBAppException {

        this._validate(colNameValue, false);

        Tuple tuple = new Tuple(this.primaryKeyName, colNameValue);
        int pageIndex = this._searchPages(tuple);
        // System.out.printf("[LOG] inserting %s in page %d\n", tuple.toString(), pageIndex);

        while (pageIndex < this.buckets.size() && ((Comparable) buckets.get(pageIndex).min).compareTo(tuple.get(this.primaryKeyName)) <= 0){
            pageIndex++;
        }

        pageIndex = Math.max(0, --pageIndex);

        if (pageIndex == 0 && this.buckets.size() == 0) {
            Page page = new Page(pathToPages);
            page.add(tuple);
            page.saveAndFree();
            this.buckets.add(page);
        } else {
            Page page = this.buckets.get(pageIndex);
            if (page.size() < DBApp.maxPerPage) {
                page.load();
                page.insert(tuple, false);
                page.saveAndFree();
            } else if (pageIndex != this.buckets.size() - 1 && buckets.get(pageIndex + 1).size() < DBApp.maxPerPage) {
                Page nxtPage = buckets.get(pageIndex + 1);
                nxtPage.load();
                page.load();
                // get the last tuple
                tuple = page.insert(tuple, true);
                nxtPage.insert(tuple, false);
                page.saveAndFree();
                nxtPage.saveAndFree();
            }else if(pageIndex != 0 && buckets.get(pageIndex - 1).size() < DBApp.maxPerPage){
                Page prevPage = buckets.get(pageIndex - 1);
                prevPage.load();
                page.load();
                // get the last tuple
                tuple = page.insert(tuple, false);
                prevPage.insert(tuple, false);
                page.saveAndFree();
                prevPage.saveAndFree();
            }else{
                Page nxtPage = new Page(pathToPages);
                page.load();
                Vector<Tuple> newData = page.insertAndSplit(tuple);
                nxtPage.setData(newData);
                page.saveAndFree();
                nxtPage.saveAndFree();
                this.buckets.add(pageIndex + 1, nxtPage);
            }
        }

        this.size++;
        this.spaceUtlize = this.size / (this.buckets.size() * DBApp.maxPerPage * 1.0);
        this._saveChanges();
    }

    // BS with min&max to get the needed page to insert or update into
    private int _searchPages(Tuple tuple) {
        int index = 0;
        int max = buckets.size() - 1;
        int min = 0;

        while (max >= min) {
            int i = (max + min) / 2;
            Page page = buckets.get(i);
            if (((Comparable) page.min).compareTo(tuple.getPrimeKey()) < 0) {
                min = i + 1;
                index = i;
            } else {
                max = i - 1;
            }
        }

        return index;
    }

    public void update(String clusteringKeyValue, Hashtable<String, Object> colNameValue) throws DBAppException {

        this._validate(colNameValue, true);

        try {
            Object pk;
            Class c;

            c = Class.forName(this.htbColumnsNameType.get(this.primaryKeyName));

            // handle each type
            if (c.equals(String.class)) {
                pk = (String) clusteringKeyValue;

            } else if (c.equals(Integer.class)) {
                pk = Integer.parseInt(clusteringKeyValue);

            } else if (c.equals(Double.class)) {
                pk = Double.parseDouble(clusteringKeyValue);

            } else {
                pk = this._parseDate(clusteringKeyValue);
            }

            Hashtable<String, Object> htb = new Hashtable<>();
            htb.put(this.primaryKeyName, pk);
            Tuple tuple = new Tuple(this.primaryKeyName, htb);
            if (((Comparable) buckets.lastElement().max).compareTo(pk) < 0
                    || ((Comparable) buckets.firstElement().min).compareTo(pk) > 0)
                return;
            int i = this._searchPages(tuple);
           
            while (i < this.buckets.size() && ((Comparable) buckets.get(i).min).compareTo(pk) <= 0) {
                System.out.printf("[LOG] the Page is Found for %s an it's is the %s\n", pk, i);
                System.out.println("[LOG] updating the page\n");
                Page page = this.buckets.get(i);
                page.load();
                page.update(tuple, colNameValue);
                page.saveAndFree();
                i++;
            }
            System.out.printf("[DONE] updating the page\n");

        } catch (ClassNotFoundException e) {
            throw new DBAppException();
        }
        this._saveChanges();
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
            Vector<Integer> indexes = entries.getValue();
            
            Collections.sort(indexes);
            Collections.reverse(indexes);
            
            page.load();

            for (Integer i : indexes){
                page.remove(i.intValue());
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

        // this is optional to minimize the search speed and the space reserved
        // this.spaceUtlize = this.size / (this.buckets.size() * DBApp.maxPerPage *
        // 1.0);
        // if(this.spaceUtlize < this.thresholdUtliz)
        // this._defragment();
        this._saveChanges();
    }

    // [DEBUG] function to print all the content of the pages
    public void printAll(){
        for(Page page : buckets){
            System.out.println(page.getPageName());
            System.out.println("===================");
            page.load();
            for(Tuple tuple : page.data)
                System.out.printf("[PK] %s\n", tuple.toString());
            page.free();
        }
    }


    // gets the rows by matching all the columnNameValues
    private Hashtable<Page, Vector<Integer>> _searchRows(Hashtable<String, Object> columnNameVlaue) {

        Hashtable<Page, Vector<Integer>> result = new Hashtable<Page, Vector<Integer>>();
        Object[] entries = columnNameVlaue.keySet().toArray();

        // getting the needed pages to work with
        for (Page page : buckets) {
            page.load();
            // boolean free = true;
            for (int i = 0; i < page.data.size(); i++) {
                if (page.data.get(i).get((String) entries[0]).equals(columnNameVlaue.get(entries[0]))) {
                    if (!result.containsKey(page)) {
                        // free = false;
                        result.put(page, new Vector<Integer>());
                    }
                    result.get(page).add(i);
                }
            }
            // if (free)
            page.free();

        }

        // perform the `and` on the resutl of the columnValue
        for (int j = 1; j < entries.length; j++) {
            String entry = (String) entries[j];
            Hashtable<Page, Vector<Integer>> tmp = new Hashtable<Page, Vector<Integer>>();
            for (Map.Entry<Page, Vector<Integer>> items : result.entrySet()) {
                Page page = items.getKey();
                page.load();
                Vector<Integer> indexes = items.getValue();
                // boolean free = true;
                for (Integer i : indexes) {
                    if (page.data.get(i).get(entry).equals(columnNameVlaue.get(entry))) {
                        if (!tmp.containsKey(page)) {
                            // free = false;
                            tmp.put(page, new Vector<Integer>());
                        }
                        tmp.get(page).add(i);
                    }
                }

                // if(free)
                page.free();
            }
            result = tmp;
        }
        if (result.size() == 0)
            return null;
        return result;
    }

    private void _delete(Page page) throws DBAppException {
        File file = new File(Paths.get(pathToPages, page.getPageName()).toString());
        if (!file.delete()) {
            System.out.printf("[ERROR] not able to delete page <%s>\n", page.getPageName());
            throw new DBAppException();
        }else{
            this.size -= page.size();
        }
    }

    private void _validate(Hashtable<String, Object> colNameValue, boolean update) throws DBAppException {

        // check that the PK is not null incase of the insert
        if (!update && colNameValue.get(this.primaryKeyName) == null) {
            System.out.println("[ERROR] the PK cannot be null ");
            throw new DBAppException();
        }

        // incase of the update check that you not updating the PK
        if (update && colNameValue.get(this.primaryKeyName) != null) {
            System.out.println("[ERROR] the PK cannot be updated");
            throw new DBAppException();
        }

        if (colNameValue.size() > numberOfColumns) {
            System.out.println("[ERROR] the number of values dose not match the number of columns");
            throw new DBAppException();
        }

        // check that the column names are valid
        Set<String> validColNames = this.htbColumnsNameType.keySet();
        for (Object s : colNameValue.keySet().toArray()) {
            if (!validColNames.contains(s)) {
                System.out.println("[ERROR] the number of values dose not match the number of columns");
                throw new DBAppException();
            }
        }

        for (Map.Entry<String, Object> entries : colNameValue.entrySet()) {
            try {
                String key = entries.getKey();
                Object value = entries.getValue();
                Class c = Class.forName(this.htbColumnsNameType.get(key));

                // check the type first
                if (!c.equals(value.getClass())) {
                    System.out.println("[ERROR] while matching the types of the input values");
                    throw new DBAppException();
                }

                // check the constrains of the min and the max
                String min = htbColumnsMin.get(key), max = htbColumnsMax.get(key);

                // handle each type
                if (c.equals(String.class)) {
                    String string = (String) value;
                    if (!(string.compareTo(min) >= 0 && string.compareTo(max) <= 0)) {
                        System.out.printf("%s %s \n", this.htbColumnsNameType.get(key), key);
                        System.out.printf("[ERROR] table %s, the values dose not respect the min/max constrain [%s | %s] -> %s",
                                this.name, min, max, string);
                        throw new DBAppException();
                    }
                } else if (c.equals(Integer.class)) {
                    Integer integer = (Integer) value, minInt = Integer.parseInt(min), maxInt = Integer.parseInt(max);
                    if (!(integer.compareTo(minInt) >= 0 && integer.compareTo(maxInt) <= 0)) {
                        System.out.println("[ERROR] the values dose not respect the min/max constrain");
                        throw new DBAppException();
                    }
                } else if (c.equals(Double.class)) {
                    Double dob = (Double) value, minDob = Double.parseDouble(min), maxDob = Double.parseDouble(max);
                    if (!(dob.compareTo(minDob) >= 0 && dob.compareTo(maxDob) <= 0)) {
                        System.out.println("[ERROR] the values dose not respect the min/max constrain");
                        throw new DBAppException();
                    }
                } else {
                    Date date = (Date) value, minDate = this._parseDate(min), maxDate = this._parseDate(max);

                    if (!(date.compareTo(minDate) >= 0 && date.compareTo(maxDate) <= 0)) {
                        System.out.println("[ERROR] the values dose not respect the min/max constrain");
                        throw new DBAppException();
                    }
                }
            } catch (ClassNotFoundException e) {
                System.out.println("[ERROR] while matching the types of the input values");
                throw new DBAppException();
            }
        }

    }

    private Date _parseDate(String value) throws DBAppException {
        Date result = null;
        try {
            result = new SimpleDateFormat(DateFormate).parse(value);
        } catch (ParseException e) {
            System.out.println("[ERROR] the input date is malformed");
            throw new DBAppException();
        }
        return result;
    }

    private void _defragment() throws DBAppException {

        // god only knows why
        if (this.buckets.size() < 3)
            return;

        // handle the defragmentation
        for (int i = 0; i < buckets.size() - 1; i++) {
            Page page = buckets.get(i);
            Page nxtPage = buckets.get(i + 1);
            if (page.size() == DBApp.maxPerPage)
                continue;
            page.load();
            while (page.size() < DBApp.maxPerPage && nxtPage.size() == 0) {
                page.add(nxtPage.remove(0));
            }

            if (nxtPage.size() == 0) {
                this._delete(nxtPage);
                this.buckets.remove(i + 1);
            }

            if (page.size() != DBApp.maxPerPage)
                i--;
            else
                page.saveAndFree();

        }

        // save the last page
        buckets.lastElement().saveAndFree();

    }

    // this method saves serialize and saves the table object
    // whenever any changes habbens inside the object
    private void _saveChanges() {
        try {
            // serialize the object
            Path p = Paths.get(Resources.getResourcePath(), "data", this.HASHCODE);
            FileOutputStream file = new FileOutputStream(p.toString());
            ObjectOutputStream out = new ObjectOutputStream(file);
            out.writeObject(this);
            // System.out.printf("[IN LOG] %s %s\n", this.name, this.toString());
        } catch (IOException | URISyntaxException e) {
            e.printStackTrace();
        }
    }

    private void _updateMetaData(){
        try {
            Path p = Paths.get(Resources.getResourcePath(), "metadata.csv");
            if(Files.exists(p)){
               StringBuilder oldData = new StringBuilder(new String(Files.readAllBytes(p)));
               StringBuilder added = new StringBuilder("");
               for(Map.Entry<String, String> columns : this.htbColumnsNameType.entrySet()){
                    added.append(
                        this.name + "," + columns.getKey() + ","  + columns.getValue() + ","
                        + columns.getKey().equals(this.primaryKeyName) + "," + "False" + ","
                    );

                    if(this.htbColumnsNameType.get(columns.getKey()).equals("java.lang.String"))
                        added.append(
                            "\"" + this.htbColumnsMin.get(columns.getKey()) + "\",\"" + this.htbColumnsMax.get(columns.getKey()) + "\"\n"
                        );
                    else
                        added.append(
                            this.htbColumnsMin.get(columns.getKey()) + "," + this.htbColumnsMax.get(columns.getKey()) + "\n"
                        );
               }
               oldData.append(added);
               Files.write(p, oldData.toString().getBytes());
            //    System.out.println("done updateing the metadata");
            }else{
                System.out.println("[ERROR] the metadata file does not exists");
            }
        } catch (IOException | URISyntaxException e) {
            e.printStackTrace();
        }
    }

    public static void main(String args[]) throws Exception {
        
    }


}
