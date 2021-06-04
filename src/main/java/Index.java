import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Index implements Serializable {

    private Table context;
    private String[] columns;
    private Hashtable<String, RangeWrapper> dimentions;
    // linear Vector to simulate the
    Vector<IndexPage> data;

    public Index(Table context, String[] columnNames, Hashtable<String, String> columnsNameType,
            Hashtable<String, String> columnMin, Hashtable<String, String> columnMax)
            throws ClassNotFoundException, DBAppException {

        this.context = context;
        this.columns = Arrays.copyOf(columnNames, columnNames.length);
        this.dimentions = new Hashtable<>();
        this.data = (Vector<IndexPage>) IntStream.range(0, (int) Math.pow(10, columnNames.length)).
                    boxed().
                    map(i -> (IndexPage) null).
                    collect(Collectors.toCollection(Vector::new));
        int rank = 0;

        for (String columnName : columnNames)
            this.dimentions.put(
                columnName,
                new RangeWrapper(
                    rank++, 
                    columnName, 
                    columnsNameType.get(columnName),
                    columnMin.get(columnName), 
                    columnMax.get(columnName)
                )   
            );
        this._fill();
    }

    public void add(Tuple t, TuplePointer tp) throws DBAppException{
        // guaranted to be array of one element as _formateArga will form an exeact value query
        for(int pos : this._getPositions(this._formateArgs(t))){
           IndexPage iPage = this.data.get(pos);
           iPage.load();
           iPage.insert(tp);
           iPage.saveAndFree();
        }
    }

    private void _fill() throws DBAppException {
        
        Hashtable<Integer,Vector<TuplePointer>> pagePack = new Hashtable<>();

        // generate the positions and group it by page to optimize the io operation
        for (int i = 0; i < this.context.buckets.size(); i++) {
            Page tPage = this.context.buckets.get(i);
            tPage.load();
            for (int j = 0; j < tPage.data.size(); j++) {
                // TODO add other methods in both tupe & tuplePointer to handle update and delete
                // TODO we could use SUB/PUB pattern
                Tuple tuple = tPage.data.get(j);
                TuplePointer tp = new TuplePointer(i, j);
                for(int pos : this._getPositions(this._formateArgs(tuple))){
                    Vector<TuplePointer> _tmp = (pagePack.containsKey(pos) ? pagePack.get(pos) : new Vector<>());
                    _tmp.add(tp);
                    pagePack.put(pos, _tmp);
                }
            }
            tPage.free();
        }
        
        // write the data to the pages and save them
        for(Map.Entry<Integer, Vector<TuplePointer>> entries : pagePack.entrySet()){
            IndexPage iPage = new IndexPage(this.context, this.context.getPathToIndexes());
            iPage.load();
            for(TuplePointer tp : entries.getValue())
                iPage.insert(tp);
            this.data.set(entries.getKey(), iPage);
            iPage.saveAndFree();
        }
    }

    private Vector<SQLTerm> _formateArgs(Tuple tuple){
        Vector<SQLTerm> res = new Vector<>();
        for(String columnName : this.columns){
            SQLTerm sqlTerm = new SQLTerm();
            sqlTerm._strTableName = this.context.name;
            sqlTerm._strColumnName = columnName;
            sqlTerm._strOperator = "=";
            sqlTerm._objValue = tuple.get(columnName);
            res.add(sqlTerm);
        }
        return res;
    }

    // TODO test this method
    private int[] _getPositions(Vector<SQLTerm> columns) throws DBAppException {
        // vector that made to be send to _permute
        Vector<Vector<Integer>> sets = new Vector<>();
        // hashtable that collects the conditions with columns - incase there are multiple -.
        Hashtable<String, Vector<SQLTerm>> colPack = new Hashtable<>();

        for(SQLTerm st : columns){
            Vector<SQLTerm> _tmp;
            
            if(colPack.containsKey(st._strColumnName))
               _tmp = colPack.get(st._strColumnName);
            else
                _tmp = new Vector<>();

            _tmp.add(st);
            colPack.put(st._strColumnName, _tmp);
        }

        // merging the resutls to get the positions
        for(String column : this.columns){
            if(colPack.containsKey(column)){
                Vector<SQLTerm> conditions = colPack.get(column);
                RangeWrapper rw = this.dimentions.get(column);
                Set<Integer> _pos = Utils.range(0, (int) rw.size);
                for(SQLTerm st : conditions){
                    Set<Integer> _tmp = new HashSet<>();
                    int i = rw.getPos(st._objValue);
                    if(i == -1 || _pos.size() == 0)
                        return (new int[0]);
                    switch(st._strOperator){
                        case "=": _tmp.add(i); break;
                        case "!=": _tmp = Utils.range(0, (int) rw.size); break;
                        case ">":
                        case ">=": _tmp = Utils.range(i, (int) rw.size); break;
                        case "<":
                        case "<=": _tmp = Utils.range(0, i + 1); break;
                        default:
                            System.out.printf("[ERROR] this oprator %s is not defined", st._strOperator);
                            throw new DBAppException();
                    }
                    _pos.retainAll(_tmp);
                }
                // if there was a conditions that resulted in an empty positions then return {};
                if(_pos.size() == 0) return (new int[0]);
                // add the result to the sets
                sets.add(
                    _pos.stream().collect(Collectors.toCollection(Vector::new))
                );
            }else{
                // get the dimention size to evaluate partial query
                int dSize = (int) this.dimentions.get(column).size;
                sets.add(
                    IntStream.
                    range(0, dSize).
                    boxed().
                    collect(Collectors.toCollection(Vector::new))
                );
            }
        }

        // !D
        // System.out.println(sets);
        // compute the product of the resulted sets to get the positions
        Vector<Vector<Integer>> pos = this._permute(sets);
        // !D
        // System.out.println(pos);
        // compute the mapped positions in the array
        int[] res = IntStream.
                    range(0, pos.size()).
                    map(p -> this._calcPos(pos.get(p))).
                    toArray();
        // !D
        // System.out.println(Arrays.toString(res));

        return res;
    }

    public HashSet<Tuple> search(Vector<SQLTerm> columns) throws DBAppException {
        
        // validate that those are the columns that the index is on
        Set<String> columnNames = Arrays.asList(this.columns).stream().collect(Collectors.toSet());
        for(SQLTerm st : columns)
            if(!columnNames.contains(st._strColumnName)){
                System.out.printf("[ERROR] the column < %s >is not in this index\n", st._strColumnName);
                throw new DBAppException();
            }
        //!D
        System.out.println("[LOG] those columns are realy exists");
        
        Vector<Tuple> res = new Vector<>();
        //  here goes the dE7k
        // get the pages to filter the needed tuples
        //!D
        // System.out.println("size: " + this.data.size());
        Vector<IndexPage> pages = Arrays.stream(this._getPositions(columns)).
                                    boxed().
                                    map(i -> this.data.get(i)).
                                    collect(
                                        Collectors.toCollection(Vector::new)
                                    );
        
        // get the needed tuples after evaluating the tuple pointers
        for(IndexPage iPage : pages){
            if(iPage == null) continue;
            iPage.load();
            res.addAll(iPage.get(columns));
            iPage.free();
        }

        HashSet<Tuple> _tmp = new HashSet<>();
        for(Tuple t : res) _tmp.add(t);
        return _tmp;
    }



    private Vector<Vector<Integer>> _permute(Vector<Vector<Integer>> sets) {
        int solutions = 1;
        Vector<Vector<Integer>> res = new Vector<>();
        for(int i = 0; i < sets.size(); solutions *= sets.get(i).size(), i++);
        for(int i = 0; i < solutions; i++) {
            Vector<Integer> tmp = new Vector<>();
            int j = 1;
            for(Vector<Integer> set : sets) {
                tmp.add(set.get((i/j)%set.size()));
                j *= set.size();
            }
            res.add(tmp);
        }
        return res;
    }
    
    private int _calcPos(Vector<Integer> pos) {
        int res = 0, prev = 1;
        // hope not to overflow yarab
        for(int i=0; i<pos.size();i++){
            res += pos.get(i) * prev;
            prev *= this.dimentions.get(this.columns[i]).size;
        }
        return res;
    }

    public String toString(){
        return this.dimentions.toString();
    }
    

    //! DEBUG[] remove tha System.out.prints later
    public static void main(String args[]) throws DBAppException, ClassNotFoundException {

        DBApp app = new DBApp();
        app.init();
        Hashtable<String, String> colNameType = new Hashtable<>();
        Hashtable<String, String> colNameMin = new Hashtable<>();
        Hashtable<String, String> colNameMax = new Hashtable<>();

        // init the columns 
        colNameType.put("id", "java.lang.Integer");
        colNameMin.put("id", "0");
        colNameMax.put("id", "10");

        colNameType.put("age", "java.lang.String");
        colNameMin.put("age", "A");
        colNameMax.put("age", "zz");

        // create the table
        
        // app.createTable("test1", "id", colNameType, colNameMin, colNameMax);
        Table table = app._getTable("test1");

        // create the index
        // app.createIndex("test1", new String[]{"id", "age"});

        Set<String> columns = new HashSet<>();
        columns.add("id");
        columns.add("age");
        Index index = table.indexes.get(columns);
        System.out.println(index.dimentions);


        // create sqltrem and test it
        Vector<SQLTerm> vst = new Vector<>();
        SQLTerm st = new SQLTerm();
        st._strTableName = "test1";
        st._strColumnName = "id";
        st._strOperator = ">";
        st._objValue = new Integer(2);

        SQLTerm st2 = new SQLTerm();
        st2._strTableName = "test1";
        st2._strColumnName = "id";
        st2._strOperator = "<";
        st2._objValue = new Integer(5);

        SQLTerm st3 = new SQLTerm();
        st3._strTableName = "test1";
        st3._strColumnName = "age";
        st3._strOperator = "=";
        st3._objValue = "A";

        vst.add(st);
        vst.add(st2);
        vst.add(st3);

        System.out.println(index.search(vst));

    }
}
